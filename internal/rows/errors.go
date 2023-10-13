package rows

import "fmt"

var errRowsNoClient = "databricks: instance of Rows missing client"

func errRowsInvalidColumnIndex(index int) string {
	return fmt.Sprintf("databricks: invalid column index: %d", index)
}
