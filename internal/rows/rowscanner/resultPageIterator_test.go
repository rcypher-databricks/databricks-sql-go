package rowscanner

import (
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
)

func TestFetchResultPagination(t *testing.T) {
	t.Parallel()

	fetches := []fetch{}
	pageSequence := []int{0, 3, 2, 0, 1, 2}
	client := getSimpleClient(&fetches, pageSequence)

	rf := &resultPageIterator{
		prevPageBounds: NewDelimiter(0, 0),
		client:         client,
		logger:         dbsqllog.WithContext("connId", "correlationId", ""),
		errMkr:         NewErrMaker("connectionId", "correlationId", "queryId"),
	}

	// next row number is zero so should fetch first result page
	_, err := rf.Next()
	assert.Nil(t, err)
	assert.Len(t, fetches, 1)
	assert.Equal(t, fetches[0].direction, DirForward)

	// The test client returns rows
	_, err = rf.Next()
	assert.Nil(t, err)
	assert.Len(t, fetches, 5)
	expected := []fetch{
		{direction: DirForward, resultStartRec: 0},
		{direction: DirForward, resultStartRec: 15},
		{direction: DirBack, resultStartRec: 10},
		{direction: DirBack, resultStartRec: 0},
		{direction: DirForward, resultStartRec: 5},
	}
	assert.Equal(t, expected, fetches)
}

type fetch struct {
	direction      Direction
	resultStartRec int
}

// Build a simple test client
func getSimpleClient(fetches *[]fetch, pageSequence []int) RowsClient {
	// We are simulating the scenario where network errors and retry behaviour cause the fetch
	// result request to be sent multiple times, resulting in jumping past the next/previous result
	// page. Behaviour should be robust enough to handle this by changing the fetch orientation.

	// Metadata for the different types is based on the results returned when querying a table with
	// all the different types which was created in a test shard.
	metadata := &cli_service.TGetResultSetMetadataResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
		Schema: &cli_service.TTableSchema{
			Columns: []*cli_service.TColumnDesc{
				{
					ColumnName: "bool_col",
					TypeDesc: &cli_service.TTypeDesc{
						Types: []*cli_service.TTypeEntry{
							{
								PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
									Type: cli_service.TTypeId_BOOLEAN_TYPE,
								},
							},
						},
					},
				},
			},
		},
	}

	getMetadata := func() (_r *cli_service.TGetResultSetMetadataResp, _err error) {
		return metadata, nil
	}

	moreRows := true
	noMoreRows := false
	colVals := []*cli_service.TColumn{{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}}

	pages := []*cli_service.TFetchResultsResp{
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &moreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 0,
				ArrowBatches:   []*cli_service.TSparkArrowBatch{{RowCount: 5}},
				Columns:        colVals,
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &moreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 5,
				ArrowBatches:   []*cli_service.TSparkArrowBatch{{RowCount: 5}},
				Columns:        colVals,
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &noMoreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 10,
				ArrowBatches:   []*cli_service.TSparkArrowBatch{{RowCount: 5}},
				Columns:        colVals,
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &noMoreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 15,
				ArrowBatches:   []*cli_service.TSparkArrowBatch{{RowCount: 5}},
				Columns:        []*cli_service.TColumn{},
			},
		},
	}

	pageIndex := -1

	fetchResults := func(dir Direction) (_r *cli_service.TFetchResultsResp, _hasMoreRows bool, _err error) {
		pageIndex++

		p := pages[pageSequence[pageIndex]]
		*fetches = append(*fetches, fetch{direction: dir, resultStartRec: int(p.Results.StartRowOffset)})
		return p, p.GetHasMoreRows(), _err
	}

	client := &testRowsClient{

		fnGetResultSetMetadata: getMetadata,
		fnFetchResults:         fetchResults,
	}

	return client
}

type testRowsClient struct {
	fnGetResultSetMetadata func() (*cli_service.TGetResultSetMetadataResp, error)
	fnFetchResults         func(Direction) (results *cli_service.TFetchResultsResp, hasMoreRows bool, err error)
	fnCloseOperation       func() error
}

func (tc *testRowsClient) GetResultSetMetadata() (*cli_service.TGetResultSetMetadataResp, error) {
	if tc.fnGetResultSetMetadata != nil {
		return tc.fnGetResultSetMetadata()
	}
	return nil, errors.New("databricks: not implemented")
}
func (tc *testRowsClient) FetchResults(dir Direction) (results *cli_service.TFetchResultsResp, hasMoreRows bool, err error) {
	if tc.fnFetchResults != nil {
		return tc.fnFetchResults(dir)
	}
	return nil, false, errors.New("databricks: not implemented")
}
func (tc *testRowsClient) CloseOperation() error {
	if tc.fnCloseOperation != nil {
		return tc.fnCloseOperation()
	}
	return errors.New("databricks: not implemented")
}
