package arrowbased

import "fmt"

var errArrowRowsUnableToReadSchema = "databricks: unable to read arrow schema"
var errArrowRowsDateTimeParse = "databrics: arrow row scanner failed to parse date/time"
var errArrowRowsToTimestampFn = "databricks: arrow row scanner failed getting toTimestamp function"
var errArrowRowsCloudFetchDownloadFailure = "cloud fetch batch loader failed to download results"
var errArrowRowsStartFetcher = "databricks: error starting ArrowRecordBatch fetcher"
var errArrowRowsFetcher = "databricks: error loading ArrowRecordBatch"

func errArrowRowsUnsupportedNativeType(t string) string {
	return fmt.Sprintf("databricks: arrow native values not yet supported for %s", t)
}
func errArrowRowsInvalidRowNumber(index int64) string {
	return fmt.Sprintf("databricks: row number %d is not contained in any arrow batch", index)
}
func errArrowRowsUnhandledArrowType(t any) string {
	return fmt.Sprintf("databricks: arrow row scanner unhandled type %s", t)
}
func errArrowRowsColumnValue(name string) string {
	return fmt.Sprintf("databricks: arrow row scanner failed getting column value for %s", name)
}
func errArrowRowsMakeRowValues(start, end int64, colInfo colInfo) string {
	return fmt.Sprintf("databricks: failure create RowsValues start: %v, end: %v, name: %v, type: %v", start, end, colInfo.name, colInfo.dbType.String())
}
