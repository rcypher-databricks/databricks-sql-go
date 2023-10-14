package rows

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"reflect"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlclient "github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerr_int "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/arrowbased"
	"github.com/databricks/databricks-sql-go/internal/rows/columnbased"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

func NewRows2(
	connId string,
	correlationId string,
	opHandle *cli_service.TOperationHandle,
	client cli_service.TCLIService,
	config *config.Config,
	directResults *cli_service.TSparkDirectResults,
	arrow bool,
) (driver.Rows, error) {

	var logger *dbsqllog.DBSQLLogger
	var ctx context.Context
	var queryId string
	if opHandle != nil {
		queryId = dbsqlclient.SprintGuid(opHandle.OperationId.GUID)
		logger = dbsqllog.WithContext(connId, correlationId, queryId)
		ctx = driverctx.NewContextWithQueryId(driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), connId), correlationId), queryId)
	} else {
		logger = dbsqllog.WithContext(connId, correlationId, "")
		ctx = driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), connId), correlationId)
	}

	if client == nil {
		logger.Error().Msg(errRowsNoClient)
		return nil, dbsqlerr_int.NewDriverError(ctx, errRowsNoClient, nil)
	}

	var pageSize int64 = 10000
	var location *time.Location = time.UTC
	if config != nil {
		pageSize = int64(config.MaxRows)

		if config.Location != nil {
			location = config.Location
		}
	}

	logger.Debug().Msgf("databricks: creating Rows, pageSize: %d, location: %v", pageSize, location)

	rc := rclient{
		client:        client,
		maxPageSize:   pageSize,
		connectionId:  connId,
		correlationId: correlationId,
		queryId:       queryId,
		opHandle:      opHandle,
		logger:        logger,
	}

	rows := &rows2{
		connectionId:  connId,
		correlationId: correlationId,
		logger:        logger,
	}

	errMkr := rowscanner.NewErrMaker(connId, correlationId, queryId)

	var closedOnServer, hasMoreRows bool
	var fetchResults *cli_service.TFetchResultsResp
	var resultSetMetadata *cli_service.TGetResultSetMetadataResp

	hasMoreRows = true
	// if we already have results for the query do some additional initialization
	if directResults != nil {
		logger.Debug().Msgf("databricks: creating Rows with direct results")
		// set the result set metadata
		if directResults.ResultSetMetadata != nil {
			resultSetMetadata = directResults.ResultSetMetadata
		}

		// If the entire query result set fits in direct results the server closes
		// the operations.
		closedOnServer = directResults != nil && directResults.CloseOperation != nil
		fetchResults = directResults.GetResultSet()
		if fetchResults != nil {
			hasMoreRows = fetchResults.GetHasMoreRows()
		}
	}

	var err error
	if resultSetMetadata == nil {
		resultSetMetadata, err = getResultSetSchema(connId, correlationId, opHandle, &rc, logger)
		if err != nil {
			return rows, err
		}
	}

	rows.schema = resultSetMetadata.Schema

	// initialize the row scanner
	rowScanner, err := makeRowScanner(&rc, closedOnServer, hasMoreRows, *config, fetchResults, resultSetMetadata, errMkr, logger, arrow)
	if err != nil {
		return rows, err
	}

	rows.rowScanner = rowScanner

	return rows, nil
}

type rows2 struct {
	rowScanner    rowscanner.RowScanner2
	schema        *cli_service.TTableSchema
	connectionId  string
	correlationId string
	logger        *dbsqllog.DBSQLLogger
}

var _ driver.Rows = (*rows2)(nil)
var _ driver.RowsColumnTypeScanType = (*rows2)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows2)(nil)
var _ driver.RowsColumnTypeNullable = (*rows2)(nil)
var _ driver.RowsColumnTypeLength = (*rows2)(nil)
var _ dbsqlrows.Rows = (*rows2)(nil)

func (r *rows2) Close() error {
	r.rowScanner.Close()
	return nil
}

func (r *rows2) Columns() []string {
	tColumns := r.schema.GetColumns()
	colNames := make([]string, len(tColumns))

	for i := range tColumns {
		colNames[i] = tColumns[i].ColumnName
	}

	return colNames
}

func (r *rows2) Next(dest []driver.Value) (err error) {
	err = r.rowScanner.ScanRow(dest)
	return
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.
func (r *rows2) ColumnTypeDatabaseTypeName(index int) string {
	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return ""
	}

	dbtype := rowscanner.GetDBTypeName(column)

	return dbtype
}

// ColumnTypeNullable implements driver.RowsColumnTypeNullable.
func (*rows2) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	return false, false
}

// ColumnTypeLength implements driver.RowsColumnTypeLength.
func (r *rows2) ColumnTypeLength(index int) (length int64, ok bool) {
	columnInfo, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return 0, false
	}

	typeName := rowscanner.GetDBTypeID(columnInfo)

	switch typeName {
	case cli_service.TTypeId_STRING_TYPE,
		cli_service.TTypeId_VARCHAR_TYPE,
		cli_service.TTypeId_BINARY_TYPE,
		cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_MAP_TYPE,
		cli_service.TTypeId_STRUCT_TYPE:
		return math.MaxInt64, true
	default:
		return 0, false
	}
}

// GetArrowBatches implements rows.DBSQLRows.
func (r *rows2) GetArrowBatches(ctx context.Context) (dbsqlrows.ArrowBatchIterator, error) {
	return r.rowScanner.GetArrowBatches(ctx)
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *rows2) ColumnTypeScanType(index int) reflect.Type {
	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return nil
	}

	scanType := getScanType(column)
	return scanType
}

func getResultSetSchema(connectionId, correlationId string, opHandle *cli_service.TOperationHandle, client rowscanner.RowsClient, logger *dbsqllog.DBSQLLogger) (*cli_service.TGetResultSetMetadataResp, dbsqlerr.DBError) {
	resp, err2 := client.GetResultSetMetadata()
	if err2 != nil {
		logger.Err(err2).Msg(err2.Error())
		return nil, rowscanner.NewRequestError(connectionId, correlationId, "databricks: Rows instance failed to retrieve result set metadata", err2)
	}

	return resp, nil
}

func (r *rows2) getColumnMetadataByIndex(index int) (*cli_service.TColumnDesc, dbsqlerr.DBError) {
	columns := r.schema.GetColumns()
	if index < 0 || index >= len(columns) {
		//TODO
		err := dbsqlerr_int.NewDriverError(context.Background(), errRowsInvalidColumnIndex(index), nil)
		r.logger.Err(err).Msg(err.Error())
		return nil, err
	}

	return columns[index], nil
}

func makeRowScanner(
	client rowscanner.RowsClient,
	closedOnServer bool,
	hasMoreRows bool,
	cfg config.Config,
	fetchResults *cli_service.TFetchResultsResp,
	resultSetMetadata *cli_service.TGetResultSetMetadataResp,
	errMkr rowscanner.ErrMaker,
	logger *dbsqllog.DBSQLLogger,
	arrow bool) (rowscanner.RowScanner2, error) {
	d := rowscanner.NewDelimiter(0, 0)

	var rowSet *cli_service.TRowSet
	if fetchResults != nil {
		rowSet = fetchResults.Results
		d = rowscanner.NewDelimiter(rowSet.StartRowOffset, rowscanner.CountRows(rowSet))
	}

	rpi := rowscanner.NewResultPageIterator(d, closedOnServer, hasMoreRows, client, errMkr, logger)

	if arrow {
		rs, err := arrowbased.NewArrowRowScanner(
			rpi,
			closedOnServer,
			logger,
			cfg,
			resultSetMetadata,
			fetchResults,
			errMkr,
		)
		return rs, err
	}

	return columnbased.NewColumnRowScanner(rpi, resultSetMetadata.Schema, rowSet, &cfg, logger, errMkr)
}

type rclient struct {
	client         cli_service.TCLIService
	opHandle       *cli_service.TOperationHandle
	logger         *dbsqllog.DBSQLLogger
	connectionId   string
	correlationId  string
	queryId        string
	maxPageSize    int64
	closedOnServer bool
}

func (rc *rclient) GetResultSetMetadata() (*cli_service.TGetResultSetMetadataResp, error) {
	req := cli_service.TGetResultSetMetadataReq{
		OperationHandle: rc.opHandle,
	}
	ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), rc.connectionId), rc.correlationId)

	resp, err := rc.client.GetResultSetMetadata(ctx, &req)
	if err != nil {
		rc.logger.Err(err).Msg(err.Error())
		return nil, dbsqlerr_int.NewRequestError(ctx, "databricks: Rows instance failed to retrieve result set metadata", err)
	}

	return resp, nil
}

var errRowsResultFetchFailed = "databricks: Rows instance failed to retrieve results"

func (rc *rclient) FetchResults(direction rowscanner.Direction) (*cli_service.TFetchResultsResp, bool, error) {

	var includeResultSetMetadata = true
	req := cli_service.TFetchResultsReq{
		OperationHandle:          rc.opHandle,
		MaxRows:                  rc.maxPageSize,
		Orientation:              directionToSparkDirection(direction),
		IncludeResultSetMetadata: &includeResultSetMetadata,
	}

	if req.MaxRows <= 0 {
		req.MaxRows = 1
	}

	ctx := driverctx.NewContextWithQueryId(driverctx.NewContextWithCorrelationId(
		driverctx.NewContextWithConnId(
			context.Background(),
			rc.connectionId,
		),
		rc.correlationId),
		rc.queryId)

	fetchResult, err := rc.client.FetchResults(ctx, &req)
	if err != nil {
		rc.logger.Err(err).Msg("databricks: Rows instance failed to retrieve results")
		return nil, false, rowscanner.NewRequestError(rc.connectionId, rc.correlationId, errRowsResultFetchFailed, err)
	}
	var hasMoreRows bool
	if fetchResult.HasMoreRows != nil {
		hasMoreRows = *fetchResult.HasMoreRows
	}

	return fetchResult, hasMoreRows, err
}

func (rc *rclient) CloseOperation() (err error) {
	if !rc.closedOnServer {

		req := cli_service.TCloseOperationReq{
			OperationHandle: rc.opHandle,
		}

		_, err = rc.client.CloseOperation(context.Background(), &req)
	}

	return err
}

func (rc *rclient) Logger() *dbsqllog.DBSQLLogger {
	return rc.logger
}

func directionToSparkDirection(d rowscanner.Direction) cli_service.TFetchOrientation {
	switch d {
	case rowscanner.DirBack:
		return cli_service.TFetchOrientation_FETCH_PRIOR
	default:
		return cli_service.TFetchOrientation_FETCH_NEXT
	}
}

var (
	scanTypeNull     = reflect.TypeOf(nil)
	scanTypeBoolean  = reflect.TypeOf(true)
	scanTypeFloat32  = reflect.TypeOf(float32(0))
	scanTypeFloat64  = reflect.TypeOf(float64(0))
	scanTypeInt8     = reflect.TypeOf(int8(0))
	scanTypeInt16    = reflect.TypeOf(int16(0))
	scanTypeInt32    = reflect.TypeOf(int32(0))
	scanTypeInt64    = reflect.TypeOf(int64(0))
	scanTypeString   = reflect.TypeOf("")
	scanTypeDateTime = reflect.TypeOf(time.Time{})
	scanTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown  = reflect.TypeOf(new(any))
)

func getScanType(column *cli_service.TColumnDesc) reflect.Type {

	// Currently all types are returned from the thrift server using
	// the primitive entry
	entry := column.TypeDesc.Types[0].PrimitiveEntry

	switch entry.Type {
	case cli_service.TTypeId_BOOLEAN_TYPE:
		return scanTypeBoolean
	case cli_service.TTypeId_TINYINT_TYPE:
		return scanTypeInt8
	case cli_service.TTypeId_SMALLINT_TYPE:
		return scanTypeInt16
	case cli_service.TTypeId_INT_TYPE:
		return scanTypeInt32
	case cli_service.TTypeId_BIGINT_TYPE:
		return scanTypeInt64
	case cli_service.TTypeId_FLOAT_TYPE:
		return scanTypeFloat32
	case cli_service.TTypeId_DOUBLE_TYPE:
		return scanTypeFloat64
	case cli_service.TTypeId_NULL_TYPE:
		return scanTypeNull
	case cli_service.TTypeId_STRING_TYPE:
		return scanTypeString
	case cli_service.TTypeId_CHAR_TYPE:
		return scanTypeString
	case cli_service.TTypeId_VARCHAR_TYPE:
		return scanTypeString
	case cli_service.TTypeId_DATE_TYPE, cli_service.TTypeId_TIMESTAMP_TYPE:
		return scanTypeDateTime
	case cli_service.TTypeId_DECIMAL_TYPE, cli_service.TTypeId_BINARY_TYPE, cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_STRUCT_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_UNION_TYPE:
		return scanTypeRawBytes
	case cli_service.TTypeId_USER_DEFINED_TYPE:
		return scanTypeUnknown
	case cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE:
		return scanTypeString
	default:
		return scanTypeUnknown
	}
}
