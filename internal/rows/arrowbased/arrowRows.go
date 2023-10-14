package arrowbased

import (
	"bytes"
	"context"
	"database/sql/driver"
	"io"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/iterators"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/databricks/databricks-sql-go/rows"
)

// Abstraction for a set of arrow records
type ArrowRecordBatch interface {
	rowscanner.Delimiter
	iterators.Iterator[ArrowRecord]
}

// Abstraction for an arrow record
type ArrowRecord interface {
	rowscanner.Delimiter
	arrow.Record
}

type timeStampFn func(arrow.Timestamp) time.Time

type colInfo struct {
	name      string
	arrowType arrow.DataType
	dbType    cli_service.TTypeId
}

// arrowRowScanner handles extracting values from arrow records
type arrowRowScanner struct {
	rowValues                RowsValues
	rowValuesIterator        iterators.Iterator[RowsValues]
	sparkArrowRecordIterator iterators.Iterator[ArrowRecord]
	nextRowNumber            int64
	colInfo                  []colInfo
	errMkr                   rowscanner.ErrMaker
}

func NewArrowRowScanner(
	resultPageIterator rowscanner.ResultPageIterator,
	closedOnServer bool,
	logger *dbsqllog.DBSQLLogger,
	cfg config.Config,
	resultSetMetadata *cli_service.TGetResultSetMetadataResp,
	fetchResults *cli_service.TFetchResultsResp,
	errMkr rowscanner.ErrMaker,
) (rowscanner.RowScanner2, error) {

	var location *time.Location = time.UTC
	if cfg.Location != nil {
		location = cfg.Location
	}

	arrowSchemaBytes, arrowSchema, metadataErr := tGetResultSetMetadataRespToArrowSchema(resultSetMetadata, cfg.ArrowConfig, logger, errMkr)
	if metadataErr != nil {
		return nil, metadataErr
	}

	// Create column info
	colInfos := getColumnInfo(arrowSchema, resultSetMetadata.Schema)

	// check for unsupported types
	for _, col := range colInfos {
		dbType := col.dbType
		if (dbType == cli_service.TTypeId_DECIMAL_TYPE && cfg.UseArrowNativeDecimal) ||
			(isIntervalType(dbType) && cfg.UseArrowNativeIntervalTypes) {
			//	not yet fully supported
			logger.Error().Msgf(errArrowRowsUnsupportedNativeType(dbType.String()))
			return nil, errMkr.Driver(errArrowRowsUnsupportedNativeType(dbType.String()), nil)
		}
	}

	ctx := errMkr.Context()

	newBatchLoaderFn := func(rp *cli_service.TFetchResultsResp) BatchLoader {
		if rp == nil {
			return nil
		}
		rowSet := rp.Results
		var bl BatchLoader
		if len(rowSet.ResultLinks) > 0 {
			bl = NewCloudBatchLoader(ctx, rowSet.ResultLinks, rowSet.StartRowOffset, &cfg, errMkr)
		} else {
			bl = NewLocalBatchLoader(ctx, rowSet.ArrowBatches, rowSet.StartRowOffset, arrowSchemaBytes, &cfg, errMkr)
		}

		return bl
	}

	bl := newBatchLoaderFn(fetchResults)

	// get the function for converting arrow timestamps to a time.Time
	// time values from the server are returned as UTC with microsecond precision
	ttsf, err := arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType).GetToTimeFunc()
	if err != nil {
		logger.Err(err).Msg(errArrowRowsToTimestampFn)
		return nil, errMkr.Driver(errArrowRowsToTimestampFn, err)
	}

	makeRowsValuesFn := func(sar ArrowRecord) (RowsValues, error) {
		defer sar.Release()
		return makeRowsValues(sar, colInfos, location, ttsf, logger)
	}

	batchLoaderIterator := NewBatchLoaderIterator(resultPageIterator, newBatchLoaderFn)
	batchIterator := NewBatchIterator(batchLoaderIterator, bl)
	sparkArrowRecordIterator := NewSparkArrowRecordIterator(batchIterator)
	rowsValuesIterator := NewRowsValuesIterator(sparkArrowRecordIterator, makeRowsValuesFn)

	ars2 := &arrowRowScanner{
		rowValuesIterator:        rowsValuesIterator,
		sparkArrowRecordIterator: sparkArrowRecordIterator,
		colInfo:                  colInfos,
		errMkr:                   errMkr,
	}

	return ars2, nil

}

var _ rowscanner.RowScanner2 = (*arrowRowScanner)(nil)

func (ars *arrowRowScanner) GetArrowBatches(ctx context.Context) (rows.ArrowBatchIterator, error) {
	ari := iterators.NewTransformingIterator[ArrowRecord, arrow.Record](
		ars.sparkArrowRecordIterator,
		func(sar ArrowRecord) (arrow.Record, error) { return sar, nil },
	)

	return ari, nil
}

func (ars *arrowRowScanner) ScanRow(destination []driver.Value) error {
	err := ars.getRowValues()
	if err != nil {
		return err
	}

	// loop over the destination slice filling in values
	for i := range destination {
		// clear the destination
		destination[i] = nil

		// if there is a corresponding column and the value for the specified row
		// is not null we put the value in the destination
		if !ars.rowValues.IsNull(i, ars.nextRowNumber) {

			// get the value from the column values holder
			var err1 error
			destination[i], err1 = ars.rowValues.Value(i, ars.nextRowNumber)
			if err1 != nil {
				err = ars.errMkr.Driver(errArrowRowsColumnValue(ars.colInfo[i].name), err1)
			}
		}
	}

	ars.nextRowNumber += 1

	return err
}

// Close any open resources
func (ars *arrowRowScanner) Close() {
	if ars.rowValues != nil {
		ars.rowValues.Close()
	}

	if ars.rowValuesIterator != nil {
		ars.rowValuesIterator.Close()
	}

	if ars.sparkArrowRecordIterator != nil {
		ars.sparkArrowRecordIterator.Close()
	}
}

func (ars *arrowRowScanner) getRowValues() (err error) {
	if ars.rowValues == nil || !ars.rowValues.Contains(ars.nextRowNumber) {
		if ars.rowValues != nil {
			ars.rowValues.Close()
			ars.rowValues = nil
		}

		var rv RowsValues
		rv, err = ars.rowValuesIterator.Next()
		ars.rowValues = rv
	}

	return
}

var intervalTypes map[cli_service.TTypeId]struct{} = map[cli_service.TTypeId]struct{}{
	cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE:   {},
	cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE: {}}

func isIntervalType(typeId cli_service.TTypeId) bool {
	_, ok := intervalTypes[typeId]
	return ok
}

// Build a slice of columnInfo using the arrow schema and the thrift schema
func getColumnInfo(arrowSchema *arrow.Schema, schema *cli_service.TTableSchema) []colInfo {
	if arrowSchema == nil || schema == nil {
		return []colInfo{}
	}

	nFields := len(arrowSchema.Fields())
	if len(schema.Columns) < nFields {
		nFields = len(schema.Columns)
	}

	colInfos := make([]colInfo, nFields)
	for i := 0; i < nFields; i++ {
		col := schema.Columns[i]
		field := arrowSchema.Field(i)
		colInfos[i] = colInfo{name: field.Name, arrowType: field.Type, dbType: rowscanner.GetDBType(col)}
	}

	return colInfos
}

// Derive an arrow.Schema object and the corresponding serialized bytes from TGetResultSetMetadataResp
func tGetResultSetMetadataRespToArrowSchema(resultSetMetadata *cli_service.TGetResultSetMetadataResp, arrowConfig config.ArrowConfig, logger *dbsqllog.DBSQLLogger, errMkr rowscanner.ErrMaker) ([]byte, *arrow.Schema, dbsqlerr.DBError) {

	var arrowSchema *arrow.Schema
	schemaBytes := resultSetMetadata.ArrowSchema

	br := bytes.NewReader(schemaBytes)
	rdr, err := ipc.NewReader(br)
	if err != nil {
		return nil, nil, errMkr.Driver(errArrowRowsUnableToReadSchema, err)
	}
	defer rdr.Release()

	arrowSchema = rdr.Schema()

	return schemaBytes, arrowSchema, nil
}

// Container for a set of arrow records
type arrowRecordBatch struct {
	// Delimiter indicating the range of rows covered by the arrow records
	rowscanner.Delimiter
	arrowRecords []ArrowRecord
}

var _ ArrowRecordBatch = (*arrowRecordBatch)(nil)

func (b *arrowRecordBatch) Next() (ArrowRecord, error) {
	if len(b.arrowRecords) > 0 {
		r := b.arrowRecords[0]
		// remove the record from the slice as iteration is only forwards
		b.arrowRecords = b.arrowRecords[1:]
		return r, nil
	}

	// no more records
	return nil, io.EOF
}

func (b *arrowRecordBatch) HasNext() bool { return b != nil && len(b.arrowRecords) > 0 }

func (b *arrowRecordBatch) Close() {
	// Release any arrow records
	for i := range b.arrowRecords {
		b.arrowRecords[i].Release()
	}
	b.arrowRecords = nil
}

// Composite of an arrow record and a delimiter indicating
// the rows corresponding to the record.
type arrowRecord struct {
	rowscanner.Delimiter
	arrow.Record
}

var _ ArrowRecord = (*arrowRecord)(nil)

func (sar *arrowRecord) Release() {
	if sar.Record != nil {
		sar.Record.Release()
		sar.Record = nil
	}
}

func (sar *arrowRecord) Retain() {
	if sar.Record != nil {
		sar.Record.Retain()
	}
}

func NewSparkArrowRecordIterator(
	batchIterator iterators.Iterator[ArrowRecordBatch],
) iterators.Iterator[ArrowRecord] {

	si := iterators.NewTransformingIterator[ArrowRecordBatch, iterators.Iterator[ArrowRecord]](
		batchIterator,
		func(b ArrowRecordBatch) (iterators.Iterator[ArrowRecord], error) { return b, nil },
	)

	sari := iterators.NewStacked1ToNIterator[ArrowRecord](si, nil)

	return sari
}

func NewRowsValuesIterator(
	sparkArrowRecordIterator iterators.Iterator[ArrowRecord],
	makeRowsValuesFn func(ArrowRecord) (RowsValues, error),
) iterators.Iterator[RowsValues] {

	rvi := iterators.NewTransformingIterator[ArrowRecord, RowsValues](
		sparkArrowRecordIterator,
		makeRowsValuesFn,
	)

	return rvi
}

func NewBatchIterator(
	batchLoaderIterator iterators.Iterator[BatchLoader],
	batchLoader BatchLoader,
) iterators.Iterator[ArrowRecordBatch] {

	ti := iterators.NewTransformingIterator[BatchLoader, iterators.Iterator[ArrowRecordBatch]](
		batchLoaderIterator,
		func(bl BatchLoader) (iterators.Iterator[ArrowRecordBatch], error) { return bl, nil },
	)

	bi := iterators.NewStacked1ToNIterator[ArrowRecordBatch](ti, batchLoader)

	return bi
}

func NewBatchLoaderIterator(
	resultPageIterator rowscanner.ResultPageIterator,
	newBatchLoaderFn func(*cli_service.TFetchResultsResp) BatchLoader,
) iterators.Iterator[BatchLoader] {

	bli := iterators.NewTransformingIterator[*cli_service.TFetchResultsResp, BatchLoader](
		resultPageIterator,
		func(rp *cli_service.TFetchResultsResp) (BatchLoader, error) { return newBatchLoaderFn(rp), nil },
	)

	if !resultPageIterator.HasNext() {
		bli.Close()
	}

	return bli
}
