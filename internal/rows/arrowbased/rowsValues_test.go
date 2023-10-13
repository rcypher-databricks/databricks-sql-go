package arrowbased

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/zeebo/assert"
)

func TestRowsValues(t *testing.T) {
	t.Run("Create default column value holders", func(t *testing.T) {
		// Check that correct typing is happening when creating column values
		// holders
		arrowSchema := getAllTypesArrowSchema()
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

		builder := array.NewRecordBuilder(mem, arrowSchema)
		defer builder.Release()
		record := builder.NewRecord()
		mr := &arrowRecord{Record: record, Delimiter: rowscanner.NewDelimiter(0, 1)}

		schema := getAllArrowTypesSchema()
		ci := getColumnInfo(arrowSchema, schema)

		rvi, err2 := makeRowsValues(mr, ci, nil, nil, nil)
		assert.Nil(t, err2)

		rv, ok := rvi.(*rowsValues)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[0].(*columnValuesTyped[*array.Boolean, bool])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[1].(*columnValuesTyped[*array.Int8, int8])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[2].(*columnValuesTyped[*array.Int16, int16])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[3].(*columnValuesTyped[*array.Int32, int32])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[4].(*columnValuesTyped[*array.Int64, int64])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[5].(*columnValuesTyped[*array.Float32, float32])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[6].(*columnValuesTyped[*array.Float64, float64])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[7].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[8].(*decimal128Container)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[9].(*dateValueContainer)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[10].(*timestampValueContainer)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[11].(*columnValuesTyped[*array.Binary, []byte])
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[12].(*listValueContainer)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[13].(*mapValueContainer)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[14].(*structValueContainer)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[15].(*nullContainer_)
		assert.True(t, ok)

	})

	t.Run("timestamp as string", func(t *testing.T) {
		// Check that correct typing is happening when creating column values
		// holders
		arrowSchema := getAllTypesArrowSchema()
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())

		builder := array.NewRecordBuilder(mem, arrowSchema)
		defer builder.Release()
		record := builder.NewRecord()
		mr := &arrowRecord{Record: record, Delimiter: rowscanner.NewDelimiter(0, 1)}

		schema := getAllArrowTypesSchema()
		ci := getColumnInfo(arrowSchema, schema)

		ci[7].dbType = cli_service.TTypeId_TIMESTAMP_TYPE

		rvi, err2 := makeRowsValues(mr, ci, nil, nil, nil)
		assert.Nil(t, err2)

		rv, ok := rvi.(*rowsValues)
		assert.True(t, ok)

		_, ok = rv.columnValueHolders[7].(*timestampStringValueContainer)
		assert.True(t, ok)
	})

}

func getAllTypesArrowSchema() *arrow.Schema {

	dt, _ := arrow.NewDecimalType(arrow.DECIMAL128, 2, 32)

	fields := []arrow.Field{
		{
			Name: "bool_col",
			Type: arrow.FixedWidthTypes.Boolean,
		},
		{
			Name: "tinyInt_col",
			Type: arrow.PrimitiveTypes.Int8,
		},
		{
			Name: "smallInt_col",
			Type: arrow.PrimitiveTypes.Int16,
		},
		{
			Name: "int_col",
			Type: arrow.PrimitiveTypes.Int32,
		},
		{
			Name: "bigInt_col",
			Type: arrow.PrimitiveTypes.Int64,
		},
		{
			Name: "float_col",
			Type: arrow.PrimitiveTypes.Float32,
		},
		{
			Name: "double_col",
			Type: arrow.PrimitiveTypes.Float64,
		},
		{
			Name: "string_col",
			Type: arrow.BinaryTypes.String,
		},
		{
			Name: "decimal_col",
			Type: dt,
		},
		{
			Name: "date_col",
			Type: arrow.FixedWidthTypes.Date32,
		},
		{
			Name: "timestamp_col",
			Type: arrow.FixedWidthTypes.Timestamp_us,
		},
		{
			Name: "binary_col",
			Type: arrow.BinaryTypes.Binary,
		},
		{
			Name: "array_col",
			Type: arrow.ListOf(arrow.PrimitiveTypes.Int16),
		},
		{
			Name: "map_col",
			Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int16),
		},
		{
			Name: "struct_col",
			Type: arrow.StructOf(arrow.Field{Name: "s", Type: arrow.BinaryTypes.String}),
		},
		{
			Name: "null_col",
			Type: arrow.Null,
		},
	}
	schema := arrow.NewSchema(fields, nil)

	return schema
}

func getAllArrowTypesSchema() *cli_service.TTableSchema {
	var scale int32 = 10
	var precision int32 = 2

	return &cli_service.TTableSchema{
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
			{
				ColumnName: "tinyInt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_TINYINT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "smallInt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_SMALLINT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "int_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_INT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "bigInt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_BIGINT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "float_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_FLOAT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "double_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DOUBLE_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "string_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_STRING_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "decimal_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DECIMAL_TYPE,
								TypeQualifiers: &cli_service.TTypeQualifiers{
									Qualifiers: map[string]*cli_service.TTypeQualifierValue{
										"scale":     {I32Value: &scale},
										"precision": {I32Value: &precision},
									},
								},
							},
						},
					},
				},
			},
			{
				ColumnName: "date_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DATE_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "timestamp_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_TIMESTAMP_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "binary_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_BINARY_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "array_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_ARRAY_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "map_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_MAP_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "struct_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_STRUCT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "null_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_NULL_TYPE,
							},
						},
					},
				},
			},
		},
	}
}
