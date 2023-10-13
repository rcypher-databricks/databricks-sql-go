package rowscanner

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
)

func TestHandlingDateTime(t *testing.T) {
	t.Run("should do nothing if data is not a date/time", func(t *testing.T) {
		val, err := HandleDateTime("this is not a date", "STRING", "string_col", time.UTC)
		assert.Nil(t, err, "handleDateTime should do nothing if a column is not a date/time")
		assert.Equal(t, "this is not a date", val)
	})

	t.Run("should error on invalid date/time value", func(t *testing.T) {
		_, err := HandleDateTime("this is not a date", "DATE", "date_col", time.UTC)
		assert.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), fmt.Sprintf(ErrRowsParseValue, "DATE", "this is not a date", "date_col")))
	})

	t.Run("should parse valid date", func(t *testing.T) {
		dt, err := HandleDateTime("2006-12-22", "DATE", "date_col", time.UTC)
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2006, 12, 22, 0, 0, 0, 0, time.UTC), dt)
	})

	t.Run("should parse valid timestamp", func(t *testing.T) {
		dt, err := HandleDateTime("2006-12-22 17:13:11.000001000", "TIMESTAMP", "timestamp_col", time.UTC)
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2006, 12, 22, 17, 13, 11, 1000, time.UTC), dt)
	})

	t.Run("should parse date with negative year", func(t *testing.T) {
		expectedTime := time.Date(-2006, 12, 22, 0, 0, 0, 0, time.UTC)
		dateStrings := []string{
			"-2006-12-22",
			"\u22122006-12-22",
			"\x2D2006-12-22",
		}

		for _, s := range dateStrings {
			dt, err := HandleDateTime(s, "DATE", "date_col", time.UTC)
			assert.Nil(t, err)
			assert.Equal(t, expectedTime, dt)
		}
	})

	t.Run("should parse timestamp with negative year", func(t *testing.T) {
		expectedTime := time.Date(-2006, 12, 22, 17, 13, 11, 1000, time.UTC)

		timestampStrings := []string{
			"-2006-12-22 17:13:11.000001000",
			"\u22122006-12-22 17:13:11.000001000",
			"\x2D2006-12-22 17:13:11.000001000",
		}

		for _, s := range timestampStrings {
			dt, err := HandleDateTime(s, "TIMESTAMP", "timestamp_col", time.UTC)
			assert.Nil(t, err)
			assert.Equal(t, expectedTime, dt)
		}
	})
}

func TestRowsFetchResultPageErrors(t *testing.T) {
	t.Parallel()

	var fetcher *resultPageIterator

	_, err := fetcher.Next()
	assert.EqualError(t, err, "databricks: driver error: "+errRowsNilResultPageFetcher)

	fetcher = &resultPageIterator{
		prevPageBounds: NewDelimiter(0, -1),
		logger:         dbsqllog.WithContext("", "", ""),
		errMkr:         NewErrMaker("connectionId", "correlationId", "queryId"),
	}

	_, err = fetcher.Next()
	assert.EqualError(t, err, "databricks: driver error: "+ErrRowsFetchPriorToStart, "negative row number should return error")

	fetcher = &resultPageIterator{
		prevPageBounds: NewDelimiter(0, 0),
		isFinished:     true,
		logger:         dbsqllog.WithContext("", "", ""),
		errMkr:         NewErrMaker("connectionId", "correlationId", "queryId"),
	}

	_, err = fetcher.Next()
	assert.EqualError(t, err, io.EOF.Error(), "row number past end of result set should return EOF")
}

func TestDelimiter(t *testing.T) {
	t.Parallel()

	var d Delimiter = delimiter{}

	assert.False(t, d.Contains(0))
	assert.False(t, d.Contains(1))
	assert.False(t, d.Contains(-1))
	assert.Equal(t, DirForward, d.Direction(0))
	assert.Equal(t, DirForward, d.Direction(1))
	assert.Equal(t, DirBack, d.Direction(-1))

	d = NewDelimiter(0, 5)
	assert.True(t, d.Contains(0))
	assert.True(t, d.Contains(4))
	assert.False(t, d.Contains(-1))
	assert.False(t, d.Contains(5))
	assert.Equal(t, DirNone, d.Direction(0))
	assert.Equal(t, DirNone, d.Direction(4))
	assert.Equal(t, DirForward, d.Direction(5))
	assert.Equal(t, DirBack, d.Direction(-1))
}

func TestCountRows(t *testing.T) {
	t.Run("columnBased", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{}
		assert.Equal(t, int64(0), CountRows(rowSet))

		rowSet.Columns = make([]*cli_service.TColumn, 1)

		bc := make([]bool, 3)
		rowSet.Columns[0] = &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: bc}}
		assert.Equal(t, int64(len(bc)), CountRows(rowSet))

		by := make([]int8, 5)
		rowSet.Columns[0] = &cli_service.TColumn{ByteVal: &cli_service.TByteColumn{Values: by}}
		assert.Equal(t, int64(len(by)), CountRows(rowSet))

		i16 := make([]int16, 7)
		rowSet.Columns[0] = &cli_service.TColumn{I16Val: &cli_service.TI16Column{Values: i16}}
		assert.Equal(t, int64(len(i16)), CountRows(rowSet))

		i32 := make([]int32, 11)
		rowSet.Columns[0] = &cli_service.TColumn{I32Val: &cli_service.TI32Column{Values: i32}}
		assert.Equal(t, int64(len(i32)), CountRows(rowSet))

		i64 := make([]int64, 13)
		rowSet.Columns[0] = &cli_service.TColumn{I64Val: &cli_service.TI64Column{Values: i64}}
		assert.Equal(t, int64(len(i64)), CountRows(rowSet))

		str := make([]string, 17)
		rowSet.Columns[0] = &cli_service.TColumn{StringVal: &cli_service.TStringColumn{Values: str}}
		assert.Equal(t, int64(len(str)), CountRows(rowSet))

		dbl := make([]float64, 19)
		rowSet.Columns[0] = &cli_service.TColumn{DoubleVal: &cli_service.TDoubleColumn{Values: dbl}}
		assert.Equal(t, int64(len(dbl)), CountRows(rowSet))

		bin := make([][]byte, 23)
		rowSet.Columns[0] = &cli_service.TColumn{BinaryVal: &cli_service.TBinaryColumn{Values: bin}}
		assert.Equal(t, int64(len(bin)), CountRows(rowSet))
	})

	t.Run("with Arrow batches", func(t *testing.T) {

		rowSet := &cli_service.TRowSet{}
		assert.Equal(t, int64(0), CountRows(rowSet))

		rowSet.ArrowBatches = []*cli_service.TSparkArrowBatch{}
		assert.Equal(t, int64(0), CountRows(rowSet))

		rowSet.ArrowBatches = []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}
		assert.Equal(t, int64(5), CountRows(rowSet))
	})

	t.Run("with result links", func(t *testing.T) {

		rowSet := &cli_service.TRowSet{}
		assert.Equal(t, int64(0), CountRows(rowSet))

		rowSet.ResultLinks = []*cli_service.TSparkArrowResultLink{}
		assert.Equal(t, int64(0), CountRows(rowSet))

		rowSet.ResultLinks = []*cli_service.TSparkArrowResultLink{{StartRowOffset: 3, RowCount: 2}, {StartRowOffset: 0, RowCount: 3}}
		assert.Equal(t, int64(5), CountRows(rowSet))
	})
}
