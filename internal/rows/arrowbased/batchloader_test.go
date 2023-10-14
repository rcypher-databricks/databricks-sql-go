package arrowbased

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCloudURLFetch(t *testing.T) {

	var handler func(w http.ResponseWriter, r *http.Request)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	}))
	defer server.Close()
	testTable := []struct {
		name             string
		response         func(w http.ResponseWriter, r *http.Request)
		linkExpired      bool
		expectedResponse ArrowRecordBatch
		expectedErr      error
	}{
		{
			name: "cloud-fetch-happy-case",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
				if err != nil {
					panic(err)
				}
			},
			linkExpired: false,
			expectedResponse: &arrowRecordBatch{
				Delimiter: rowscanner.NewDelimiter(0, 3),
				arrowRecords: []ArrowRecord{
					&arrowRecord{Delimiter: rowscanner.NewDelimiter(0, 3), Record: generateArrowRecord()},
					&arrowRecord{Delimiter: rowscanner.NewDelimiter(3, 3), Record: generateArrowRecord()},
				},
			},
			expectedErr: nil,
		},
		{
			name: "cloud-fetch-expired_link",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
				if err != nil {
					panic(err)
				}
			},
			linkExpired:      true,
			expectedResponse: nil,
			expectedErr:      errors.New(dbsqlerr.ErrLinkExpired),
		},
		{
			name: "cloud-fetch-http-error",
			response: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			linkExpired:      false,
			expectedResponse: nil,
			expectedErr:      dbsqlerrint.NewDriverError(context.TODO(), errArrowRowsCloudFetchDownloadFailure, nil),
		},
	}

	for _, tc := range testTable {
		t.Run(tc.name, func(t *testing.T) {
			handler = tc.response

			expiryTime := time.Now()
			// If link expired, subtract 1 sec from current time to get expiration time
			if tc.linkExpired {
				expiryTime = expiryTime.Add(-1 * time.Second)
			} else {
				expiryTime = expiryTime.Add(10 * time.Second)
			}

			cu := &cloudURL{
				Delimiter:  rowscanner.NewDelimiter(0, 3),
				fileLink:   server.URL,
				expiryTime: expiryTime.Unix(),
			}

			ctx := context.Background()

			resp, err := cu.Fetch(ctx)

			if tc.expectedResponse != nil {
				assert.NotNil(t, resp)
				esab, ok := tc.expectedResponse.(*arrowRecordBatch)
				assert.True(t, ok)
				asab, ok2 := resp.(*arrowRecordBatch)
				assert.True(t, ok2)
				if !reflect.DeepEqual(esab.Delimiter, asab.Delimiter) {
					t.Errorf("expected (%v), got (%v)", esab.Delimiter, asab.Delimiter)
				}
				assert.Equal(t, len(esab.arrowRecords), len(asab.arrowRecords))
				for i := range esab.arrowRecords {
					er := esab.arrowRecords[i]
					ar := asab.arrowRecords[i]

					eb := generateMockArrowBytes(er)
					ab := generateMockArrowBytes(ar)
					assert.Equal(t, eb, ab)
				}
			}

			if !errors.Is(err, tc.expectedErr) {
				assert.EqualErrorf(t, err, fmt.Sprintf("%v", tc.expectedErr), "expected (%v), got (%v)", tc.expectedErr, err)
			}
		})
	}
}

func TestBatchLoader(t *testing.T) {

	t.Run("test loading with cloud batches", func(t *testing.T) {
		var nLoads int
		var handler func(w http.ResponseWriter, r *http.Request) = func(w http.ResponseWriter, r *http.Request) {
			nLoads += 1
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
			if err != nil {
				panic(err)
			}

		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler(w, r)
		}))
		defer server.Close()

		expiryTime := time.Now().Add(10 * time.Second)

		urls := []*cli_service.TSparkArrowResultLink{
			{
				FileLink:       server.URL,
				ExpiryTime:     expiryTime.Unix(),
				StartRowOffset: 0,
				RowCount:       3,
			},
			{
				FileLink:       server.URL,
				ExpiryTime:     expiryTime.Unix(),
				StartRowOffset: 3,
				RowCount:       3,
			},
			{
				FileLink:       server.URL,
				ExpiryTime:     expiryTime.Unix(),
				StartRowOffset: 6,
				RowCount:       3,
			},
		}

		bl := NewCloudBatchLoader(
			context.Background(),
			urls,
			0,
			config.WithDefaults(),
			rowscanner.NewErrMaker("a", "b", "c"),
		)

		assert.Equal(t, int64(0), bl.Start())
		assert.Equal(t, int64(9), bl.Count())
		assert.True(t, bl.HasNext())

		for i := range urls {
			assert.True(t, bl.HasNext())
			batch, err := bl.Next()
			assert.Nil(t, err)
			assert.NotNil(t, batch)
			assert.Equal(t, urls[i].RowCount, batch.Count())
			assert.Equal(t, urls[i].StartRowOffset, batch.Start())
		}

		assert.False(t, bl.HasNext())
		assert.Equal(t, len(urls), nLoads)
	})

	t.Run("test loading with local batches", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "diamonds.json", &executeStatementResp)
		nRows := rowscanner.CountRows(executeStatementResp.DirectResults.ResultSet.Results)
		arrowBatches := executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches

		bl := NewLocalBatchLoader(
			context.Background(),
			arrowBatches,
			0,
			executeStatementResp.DirectResults.ResultSetMetadata.ArrowSchema,
			config.WithDefaults(),
			rowscanner.NewErrMaker("a", "b", "c"),
		)

		assert.Equal(t, int64(0), bl.Start())
		assert.Equal(t, nRows, bl.Count())
		assert.True(t, bl.HasNext())

		var batchStart int64
		for i := range arrowBatches {
			assert.True(t, bl.HasNext())
			batch, err := bl.Next()
			assert.Nil(t, err)
			assert.NotNil(t, batch)
			assert.Equal(t, arrowBatches[i].RowCount, batch.Count())
			assert.Equal(t, batchStart, batch.Start())

			batchStart += arrowBatches[i].RowCount
		}

	})

	// t.Run("test link load failure", func(t *testing.T) {
	// 	var nLoads int
	// 	var handler func(w http.ResponseWriter, r *http.Request) = func(w http.ResponseWriter, r *http.Request) {
	// 		nLoads += 1
	// 		if nLoads == 3 {
	// 			w.WriteHeader(http.StatusInternalServerError)
	// 		} else {
	// 			w.WriteHeader(http.StatusOK)
	// 			_, err := w.Write(generateMockArrowBytes(generateArrowRecord()))
	// 			if err != nil {
	// 				panic(err)
	// 			}
	// 		}
	// 	}

	// 	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	// 		handler(w, r)
	// 	}))
	// 	defer server.Close()

	// 	expiryTime := time.Now().Add(10 * time.Second)

	// 	urls := []*cli_service.TSparkArrowResultLink{
	// 		{
	// 			FileLink:       server.URL,
	// 			ExpiryTime:     expiryTime.Unix(),
	// 			StartRowOffset: 0,
	// 			RowCount:       3,
	// 		},
	// 		{
	// 			FileLink:       server.URL,
	// 			ExpiryTime:     expiryTime.Unix(),
	// 			StartRowOffset: 3,
	// 			RowCount:       3,
	// 		},
	// 		{
	// 			FileLink:       server.URL,
	// 			ExpiryTime:     expiryTime.Unix(),
	// 			StartRowOffset: 6,
	// 			RowCount:       3,
	// 		},
	// 		{
	// 			FileLink:       server.URL,
	// 			ExpiryTime:     expiryTime.Unix(),
	// 			StartRowOffset: 9,
	// 			RowCount:       3,
	// 		},
	// 	}

	// 	cfg := config.WithDefaults()
	// 	cfg.MaxDownloadThreads = 1

	// 	bl, err := NewCloudBatchLoader(
	// 		context.Background(),
	// 		urls,
	// 		0,
	// 		cfg,
	// 		&testStatusGetter{},
	// 		logger.Logger,
	// 	)
	// 	assert.Nil(t, err)

	// 	batch, err := bl.GetBatchFor(0)
	// 	assert.Nil(t, err)
	// 	assert.NotNil(t, batch)

	// 	time.Sleep(1 * time.Second)

	// 	for _, i := range []int{0, 1} {
	// 		batch, err := bl.GetBatchFor(urls[i].StartRowOffset + 1)
	// 		assert.Nil(t, err)
	// 		assert.NotNil(t, batch)
	// 		assert.Equal(t, urls[i].RowCount, batch.Count())
	// 		assert.Equal(t, urls[i].StartRowOffset, batch.Start())
	// 	}

	// 	for _, i := range []int{2, 3} {
	// 		_, err := bl.GetBatchFor(urls[i].StartRowOffset + 1)
	// 		assert.NotNil(t, err)

	// 	}

	// })
}

func generateArrowRecord() arrow.Record {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)

	record := builder.NewRecord()

	return record
}

func generateMockArrowBytes(record arrow.Record) []byte {

	defer record.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Write(record); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	return buf.Bytes()
}
