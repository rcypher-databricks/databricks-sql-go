package arrowbased

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/iterators"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"

	"net/http"

	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/fetcher"
)

type BatchLoader iterators.Iterator[ArrowRecordBatch]

func NewCloudBatchLoader(
	ctx context.Context,
	files []*cli_service.TSparkArrowResultLink,
	startRowOffset int64,
	cfg *config.Config,
	errMkr rowscanner.ErrMaker) *batchLoader[*cloudURL] {

	if cfg == nil {
		cfg = config.WithDefaults()
	}

	inputChan := make(chan fetcher.FetchableItems[ArrowRecordBatch], len(files))

	var rowCount int64
	for i := range files {
		f := files[i]
		li := &cloudURL{
			Delimiter:         rowscanner.NewDelimiter(f.StartRowOffset, f.RowCount),
			fileLink:          f.FileLink,
			expiryTime:        f.ExpiryTime,
			minTimeToExpiry:   cfg.MinTimeToExpiry,
			compressibleBatch: compressibleBatch{useLz4Compression: cfg.UseLz4Compression},
		}
		inputChan <- li

		rowCount += f.RowCount
	}

	// make sure to close input channel or fetcher will block waiting for more inputs
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*cloudURL](ctx, cfg.MaxDownloadThreads, cfg.MaxFilesInMemory, inputChan)
	cbl := &batchLoader[*cloudURL]{
		Delimiter:      rowscanner.NewDelimiter(startRowOffset, rowCount),
		fetcher:        f,
		errMkr:         errMkr,
		nBatches:       len(files),
		nextBatchStart: startRowOffset,
	}

	return cbl
}

func NewLocalBatchLoader(
	ctx context.Context,
	batches []*cli_service.TSparkArrowBatch,
	startRowOffset int64,
	arrowSchemaBytes []byte,
	cfg *config.Config,
	errMkr rowscanner.ErrMaker) *batchLoader[*localBatch] {

	if cfg == nil {
		cfg = config.WithDefaults()
	}

	var startRow int64 = startRowOffset
	var rowCount int64
	inputChan := make(chan fetcher.FetchableItems[ArrowRecordBatch], len(batches))
	for i := range batches {
		b := batches[i]
		if b != nil {
			li := &localBatch{
				Delimiter:         rowscanner.NewDelimiter(startRow, b.RowCount),
				batchBytes:        b.Batch,
				arrowSchemaBytes:  arrowSchemaBytes,
				compressibleBatch: compressibleBatch{useLz4Compression: cfg.UseLz4Compression},
			}
			inputChan <- li
			startRow = startRow + b.RowCount
			rowCount += b.RowCount
		}
	}
	close(inputChan)

	f, _ := fetcher.NewConcurrentFetcher[*localBatch](ctx, cfg.MaxDownloadThreads, cfg.MaxFilesInMemory, inputChan)
	cbl := &batchLoader[*localBatch]{
		Delimiter:      rowscanner.NewDelimiter(startRowOffset, rowCount),
		fetcher:        f,
		errMkr:         errMkr,
		nBatches:       len(batches),
		nextBatchStart: startRowOffset,
	}

	return cbl
}

type batchLoader[T interface {
	Fetch(ctx context.Context) (ArrowRecordBatch, error)
}] struct {
	rowscanner.Delimiter
	fetcher        fetcher.Fetcher[ArrowRecordBatch]
	arrowBatches   []ArrowRecordBatch
	nBatches       int
	nextBatchStart int64
	cancelFetch    context.CancelFunc
	batchChan      <-chan ArrowRecordBatch
	errMkr         rowscanner.ErrMaker
}

var _ BatchLoader = (*batchLoader[*localBatch])(nil)
var _ iterators.Iterator[ArrowRecordBatch] = (*batchLoader[*localBatch])(nil)
var _ iterators.Iterator[ArrowRecordBatch] = (*batchLoader[*cloudURL])(nil)

func (cbl *batchLoader[T]) getBatchFor(rowNumber int64) (ArrowRecordBatch, dbsqlerr.DBError) {

	for i := range cbl.arrowBatches {
		if cbl.arrowBatches[i].Contains(rowNumber) {
			ab := cbl.arrowBatches[i]
			cbl.arrowBatches[i] = cbl.arrowBatches[len(cbl.arrowBatches)-1]
			cbl.arrowBatches = cbl.arrowBatches[:len(cbl.arrowBatches)-1]
			return ab, nil
		}
	}

	batchChan, cancelFunc, err := cbl.fetcher.Start()
	cbl.cancelFetch = cancelFunc
	cbl.batchChan = batchChan

	var emptyBatch ArrowRecordBatch
	if err != nil {
		return emptyBatch, cbl.errMkr.Driver(errArrowRowsStartFetcher, err)
	}

	for {
		batch, ok := <-batchChan
		if !ok {
			err := cbl.fetcher.Err()
			if err != nil {
				return emptyBatch, cbl.errMkr.Driver(errArrowRowsFetcher, err)
			}
			break
		}

		if batch.Contains(rowNumber) {
			return batch, nil
		} else {
			cbl.arrowBatches = append(cbl.arrowBatches, batch)
		}
	}

	return emptyBatch, cbl.errMkr.Driver(errArrowRowsInvalidRowNumber(rowNumber), err)
}

func (bl *batchLoader[T]) Next() (ArrowRecordBatch, error) {
	if !bl.HasNext() {
		return nil, io.EOF
	}

	batch, err := bl.getBatchFor(bl.nextBatchStart)
	if err != nil {
		bl.Close()
		return nil, err
	}

	bl.nextBatchStart = batch.Start() + batch.Count()

	return batch, err
}

func (bl *batchLoader[T]) HasNext() bool {
	return bl.Contains(bl.nextBatchStart)
}

func (cbl *batchLoader[T]) Close() {
	if cbl.cancelFetch != nil {
		cbl.cancelFetch()
	}

	for i := range cbl.arrowBatches {
		cbl.arrowBatches[i].Close()
	}

	// drain any batches in the fetcher output channel
	if cbl.batchChan != nil {
		for b := range cbl.batchChan {
			b.Close()
		}
	}
}

type compressibleBatch struct {
	useLz4Compression bool
}

func (cb compressibleBatch) getReader(r io.Reader) io.Reader {
	if cb.useLz4Compression {
		return lz4.NewReader(r)
	}
	return r
}

type cloudURL struct {
	compressibleBatch
	rowscanner.Delimiter
	fileLink        string
	expiryTime      int64
	minTimeToExpiry time.Duration
}

func (cu *cloudURL) Fetch(ctx context.Context) (ArrowRecordBatch, error) {
	var sab ArrowRecordBatch

	if isLinkExpired(cu.expiryTime, cu.minTimeToExpiry) {
		return sab, errors.New(dbsqlerr.ErrLinkExpired)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", cu.fileLink, nil)
	if err != nil {
		return sab, err
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return sab, err
	}
	if res.StatusCode != http.StatusOK {
		return sab, dbsqlerrint.NewDriverError(ctx, errArrowRowsCloudFetchDownloadFailure, err)
	}

	defer res.Body.Close()

	r := cu.compressibleBatch.getReader(res.Body)

	records, err := getArrowRecords(r, cu.Start())
	if err != nil {
		return nil, err
	}

	arrowBatch := arrowRecordBatch{
		Delimiter:    rowscanner.NewDelimiter(cu.Start(), cu.Count()),
		arrowRecords: records,
	}

	return &arrowBatch, nil
}

func isLinkExpired(expiryTime int64, linkExpiryBuffer time.Duration) bool {
	bufferSecs := int64(linkExpiryBuffer.Seconds())
	return expiryTime-bufferSecs < time.Now().Unix()
}

var _ fetcher.FetchableItems[ArrowRecordBatch] = (*cloudURL)(nil)

type localBatch struct {
	compressibleBatch
	rowscanner.Delimiter
	batchBytes       []byte
	arrowSchemaBytes []byte
}

var _ fetcher.FetchableItems[ArrowRecordBatch] = (*localBatch)(nil)

func (lb *localBatch) Fetch(ctx context.Context) (ArrowRecordBatch, error) {
	r := lb.compressibleBatch.getReader(bytes.NewReader(lb.batchBytes))
	r = io.MultiReader(bytes.NewReader(lb.arrowSchemaBytes), r)

	records, err := getArrowRecords(r, lb.Start())
	if err != nil {
		return &arrowRecordBatch{}, err
	}

	lb.batchBytes = nil
	batch := arrowRecordBatch{
		Delimiter:    rowscanner.NewDelimiter(lb.Start(), lb.Count()),
		arrowRecords: records,
	}

	return &batch, nil
}

func getArrowRecords(r io.Reader, startRowOffset int64) ([]ArrowRecord, error) {
	ipcReader, err := ipc.NewReader(r)
	if err != nil {
		return nil, err
	}

	defer ipcReader.Release()

	startRow := startRowOffset
	var records []ArrowRecord
	for ipcReader.Next() {
		r := ipcReader.Record()
		r.Retain()

		sar := arrowRecord{
			Delimiter: rowscanner.NewDelimiter(startRow, r.NumRows()),
			Record:    r,
		}

		records = append(records, &sar)

		startRow += r.NumRows()
	}

	if ipcReader.Err() != nil {
		for i := range records {
			records[i].Release()
		}
		return nil, ipcReader.Err()
	}

	return records, nil
}
