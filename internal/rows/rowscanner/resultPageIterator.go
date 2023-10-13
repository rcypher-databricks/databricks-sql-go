package rowscanner

import (
	"context"
	"fmt"
	"io"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/iterators"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
)

var errRowsResultFetchFailed = "databricks: Rows instance failed to retrieve results"
var errRowsResultFetchZeroRows = "databricks: Rows instance retrieved results with zero records"
var ErrRowsFetchPriorToStart = "databricks: unable to fetch row page prior to start of results"
var errRowsNilResultPageFetcher = "databricks: nil ResultPageFetcher instance"

func errRowsUnandledFetchDirection(dir string) string {
	return fmt.Sprintf("databricks: unhandled fetch direction %s", dir)
}

// Interface for iterating over the pages in the result set of a query
type ResultPageIterator iterators.Iterator[*cli_service.TFetchResultsResp]

// Define directions for seeking in the pages of a query result
type Direction int

const (
	DirUnknown Direction = iota
	DirNone
	DirForward
	DirBack
)

var directionNames []string = []string{"Unknown", "None", "Forward", "Back"}

func (d Direction) String() string {
	return directionNames[d]
}

// Create a new result page iterator.
func NewResultPageIterator(
	previousPageBounds Delimiter,
	closedOnServer bool,
	hasMoreRows bool,
	client RowsClient,
	errMkr ErrMaker,
	logger *dbsqllog.DBSQLLogger,
) ResultPageIterator {

	// delimiter and hasMoreRows are used to set up the point in the paginated
	// result set that this iterator starts from.
	return &resultPageIterator{
		prevPageBounds: previousPageBounds,
		isFinished:     !hasMoreRows,
		closedOnServer: closedOnServer,
		client:         client,
		errMkr:         errMkr,
		logger:         logger,
	}
}

type resultPageIterator struct {
	// bounds of the previously loaded result page
	prevPageBounds Delimiter

	//	indicates whether there are any more pages in the result set
	isFinished     bool
	closedOnServer bool

	// client for communicating with the server
	client RowsClient

	errMkr ErrMaker
	logger *dbsqllog.DBSQLLogger
}

var _ ResultPageIterator = (*resultPageIterator)(nil)

// Returns true if there are more pages in the result set.
func (rpf *resultPageIterator) HasNext() bool { return !rpf.isFinished }

// Returns the next page of the result set. io.EOF will be returned if there are
// no more pages.
func (rpf *resultPageIterator) Next() (*cli_service.TFetchResultsResp, error) {

	if rpf == nil {
		return nil, dbsqlerrint.NewDriverError(context.Background(), errRowsNilResultPageFetcher, nil)
	}

	if rpf.isFinished {
		rpf.Close()
		return nil, io.EOF
	}

	// Starting row number of next result page. This is used to check that the returned page is
	// the expected one.
	nextPageStartRow := rpf.prevPageBounds.Start() + rpf.prevPageBounds.Count()

	rpf.logger.Debug().Msgf("databricks: fetching result page for row %d", nextPageStartRow)

	var fetchResult *cli_service.TFetchResultsResp
	var hasMoreRows bool = !rpf.isFinished

	// Keep fetching in the appropriate direction until we have the expected page.
	for !rpf.prevPageBounds.Contains(nextPageStartRow) && !rpf.isFinished {
		direction := rpf.prevPageBounds.Direction(nextPageStartRow)
		err := checkDirectionValid(rpf.errMkr, rpf.prevPageBounds, hasMoreRows, direction)
		if err != nil {
			rpf.logger.Err(err)
			rpf.Close()
			return nil, err
		}

		rpf.logger.Debug().Msgf("fetching next batch of up to %d rows, %s", 0, direction.String())

		fetchResult, hasMoreRows, err = rpf.client.FetchResults(direction)
		if err != nil {
			rpf.logger.Err(err).Msg(errRowsResultFetchFailed)
			rpf.Close()
			return nil, rpf.errMkr.Request(errRowsResultFetchFailed, err)
		}

		nRows := CountRows(fetchResult.Results)
		if nRows == 0 {
			rpf.logger.Err(err).Msg(errRowsResultFetchZeroRows)
			rpf.Close()
			return nil, rpf.errMkr.Request(errRowsResultFetchZeroRows, err)
		}

		rpf.prevPageBounds = NewDelimiter(fetchResult.Results.StartRowOffset, nRows)
		rpf.logger.Debug().Msgf("databricks: new result page startRow: %d, nRows: %v, hasMoreRows: %v", rpf.prevPageBounds.Start(), rpf.prevPageBounds.Count(), fetchResult.HasMoreRows)
	}

	if !hasMoreRows {
		rpf.Close()
	}

	return fetchResult, nil
}

func (rpf *resultPageIterator) Close() {
	rpf.isFinished = true
	if !rpf.closedOnServer {
		rpf.closedOnServer = true
		if rpf.client != nil {
			_ = rpf.client.CloseOperation()
		}
	}
}

// countRows returns the number of rows in the TRowSet
func CountRows(rowSet *cli_service.TRowSet) int64 {
	if rowSet == nil {
		return 0
	}

	var n int64
	if rowSet.ArrowBatches != nil {
		batches := rowSet.ArrowBatches
		for i := range batches {
			n += batches[i].RowCount
		}
	} else if rowSet.ResultLinks != nil {
		links := rowSet.ResultLinks
		for i := range links {
			n += links[i].RowCount
		}
	} else if rowSet.Columns != nil {
		// Find a column/values and return the number of values.
		for _, col := range rowSet.Columns {
			if col.BoolVal != nil {
				n = int64(len(col.BoolVal.Values))
			}
			if col.ByteVal != nil {
				n = int64(len(col.ByteVal.Values))
			}
			if col.I16Val != nil {
				n = int64(len(col.I16Val.Values))
			}
			if col.I32Val != nil {
				n = int64(len(col.I32Val.Values))
			}
			if col.I64Val != nil {
				n = int64(len(col.I64Val.Values))
			}
			if col.StringVal != nil {
				n = int64(len(col.StringVal.Values))
			}
			if col.DoubleVal != nil {
				n = int64(len(col.DoubleVal.Values))
			}
			if col.BinaryVal != nil {
				n = int64(len(col.BinaryVal.Values))
			}
		}
	}
	return n
}

// Check if trying to fetch in the specified direction creates an error condition.
func checkDirectionValid(errMkr ErrMaker, currentPageBounds Delimiter, hasMoreRows bool, direction Direction) (err error) {
	switch direction {
	case DirBack:
		if currentPageBounds.Start() == 0 {
			// can't fetch rows previous to the start
			err = errMkr.Driver(ErrRowsFetchPriorToStart, nil)
		}
	case DirForward:
		if !hasMoreRows {
			// can't fetch past the end of the query results
			err = io.EOF
		}
	default:
		err = errMkr.Driver(errRowsUnandledFetchDirection(direction.String()), nil)
	}

	return
}
