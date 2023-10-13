package arrowbased

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

// Abstraction for holding the values for a set of rows
type RowsValues interface {
	rowscanner.Delimiter
	Close()
	NColumns() int
	SetColumnValues(columnIndex int, values arrow.ArrayData) error
	IsNull(columnIndex int, rowNumber int64) bool
	Value(columnIndex int, rowNumber int64) (any, error)
	SetDelimiter(d rowscanner.Delimiter)
}

type rowsValues struct {
	rowscanner.Delimiter
	columnValueHolders []columnValues
}

var _ RowsValues = (*rowsValues)(nil)

func (rv *rowsValues) Close() {
	// release any retained arrow arrays
	for i := range rv.columnValueHolders {
		if rv.columnValueHolders[i] != nil {
			rv.columnValueHolders[i].Release()
		}
	}
}

func (rv *rowsValues) SetColumnValues(columnIndex int, values arrow.ArrayData) error {
	var err error
	if columnIndex < len(rv.columnValueHolders) && rv.columnValueHolders[columnIndex] != nil {
		rv.columnValueHolders[columnIndex].Release()
		err = rv.columnValueHolders[columnIndex].SetValueArray(values)
	}
	return err
}

func (rv *rowsValues) IsNull(columnIndex int, rowNumber int64) bool {
	var b bool = true
	if columnIndex < len(rv.columnValueHolders) {
		b = rv.columnValueHolders[columnIndex].IsNull(int(rowNumber - rv.Start()))
	}
	return b
}

func (rv *rowsValues) Value(columnIndex int, rowNumber int64) (any, error) {
	var err error
	var value any
	if columnIndex < len(rv.columnValueHolders) {
		value, err = rv.columnValueHolders[columnIndex].Value(int(rowNumber - rv.Start()))
	}
	return value, err
}

func (rv *rowsValues) NColumns() int { return len(rv.columnValueHolders) }

func (rv *rowsValues) SetDelimiter(d rowscanner.Delimiter) { rv.Delimiter = d }

// columnValues is the interface for accessing the values for a column
type columnValues interface {
	Value(int) (any, error)
	IsNull(int) bool
	Release()
	SetValueArray(colData arrow.ArrayData) error
}

// a type constraint for the simple value types which can be handled by the generic
// implementation of columnValues
type valueTypes interface {
	bool |
		int8 |
		int16 |
		int32 |
		int64 |
		float32 |
		float64 |
		string |
		[]byte
}

// a type constraint for the arrow array types which can be handled by the generic
// implementation of columnValues
type arrowArrayTypes interface {
	*array.Boolean |
		*array.Int8 |
		*array.Int16 |
		*array.Int32 |
		*array.Int64 |
		*array.Float32 |
		*array.Float64 |
		*array.String |
		*array.Binary
}

// type constraint for wrapping arrow arrays
type columnValuesHolder[T valueTypes] interface {
	arrowArrayTypes
	Value(int) T
	IsNull(int) bool
	Release()
}

// a generic container for the arrow arrays/value types we handle
type columnValuesTyped[ValueHolderType columnValuesHolder[ValueType], ValueType valueTypes] struct {
	holder ValueHolderType
	foo    ValueType
}

// return the value for the specified row
func (cv *columnValuesTyped[X, T]) Value(rowNum int) (any, error) {
	return cv.holder.Value(rowNum), nil
}

// return true if the value at rowNum is null
func (cv *columnValuesTyped[X, T]) IsNull(rowNum int) bool {
	return cv.holder.IsNull(rowNum)
}

// release the the contained arrow array
func (cv *columnValuesTyped[X, T]) Release() {
	if cv.holder != nil {
		cv.holder.Release()
	}
}

func (cv *columnValuesTyped[X, T]) SetValueArray(colData arrow.ArrayData) error {
	var colValsHolder columnValues = cv
	switch t := any(cv.foo).(type) {
	case bool:
		colValsHolder.(*columnValuesTyped[*array.Boolean, bool]).holder = array.NewBooleanData(colData)

	case int8:
		colValsHolder.(*columnValuesTyped[*array.Int8, int8]).holder = array.NewInt8Data(colData)

	case int16:
		colValsHolder.(*columnValuesTyped[*array.Int16, int16]).holder = array.NewInt16Data(colData)

	case int32:
		colValsHolder.(*columnValuesTyped[*array.Int32, int32]).holder = array.NewInt32Data(colData)

	case int64:
		colValsHolder.(*columnValuesTyped[*array.Int64, int64]).holder = array.NewInt64Data(colData)

	case float32:
		colValsHolder.(*columnValuesTyped[*array.Float32, float32]).holder = array.NewFloat32Data(colData)

	case float64:
		colValsHolder.(*columnValuesTyped[*array.Float64, float64]).holder = array.NewFloat64Data(colData)

	case string:
		colValsHolder.(*columnValuesTyped[*array.String, string]).holder = array.NewStringData(colData)

	case []byte:
		colValsHolder.(*columnValuesTyped[*array.Binary, []byte]).holder = array.NewBinaryData(colData)

	default:
		return errors.New(errArrowRowsUnhandledArrowType(t))
	}

	return nil
}

// ensure the columnValuesTyped implements columnValues
var _ columnValues = (*columnValuesTyped[*array.Int16, int16])(nil)

type listValueContainer struct {
	listArray     array.ListLike
	values        columnValues
	complexValue  bool
	listArrayType *arrow.ListType
}

var _ columnValues = (*listValueContainer)(nil)

func (lvc *listValueContainer) Value(i int) (any, error) {
	if i < lvc.listArray.Len() {
		r := "["
		s, e := lvc.listArray.ValueOffsets(i)
		len := int(e - s)

		for i := 0; i < len; i++ {
			if lvc.values.IsNull(i + int(s)) {
				r = r + "null"
			} else {

				val, err := lvc.values.Value(i + int(s))
				if err != nil {
					return nil, err
				}

				if !lvc.complexValue {
					vb, err := marshal(val)
					if err != nil {
						return nil, err
					}
					r = r + string(vb)
				} else {
					r = r + val.(string)
				}
			}

			if i < len-1 {
				r = r + ","
			}
		}

		r = r + "]"
		return r, nil
	}
	return nil, nil
}

func (lvc *listValueContainer) IsNull(i int) bool {
	return lvc.listArray.IsNull(i)
}

func (lvc *listValueContainer) Release() {
	if lvc.listArray != nil {
		lvc.listArray.Release()
	}

	if lvc.values != nil {
		lvc.values.Release()
	}
}

func (lvc *listValueContainer) SetValueArray(colData arrow.ArrayData) error {
	lvc.listArray = array.NewListData(colData)
	lvs := lvc.listArray.ListValues()
	err := lvc.values.SetValueArray(lvs.Data())

	return err
}

type mapValueContainer struct {
	mapArray     *array.Map
	keys         columnValues
	values       columnValues
	complexValue bool
	mapArrayType *arrow.MapType
}

var _ columnValues = (*mapValueContainer)(nil)

func (mvc *mapValueContainer) Value(i int) (any, error) {
	if i < mvc.mapArray.Len() {
		s, e := mvc.mapArray.ValueOffsets(i)
		len := e - s
		r := "{"
		for i := int64(0); i < len; i++ {
			k, err := mvc.keys.Value(int(i + s))
			if err != nil {
				return nil, err
			}

			key, err := marshal(k)
			if err != nil {
				return nil, err
			}

			v, err := mvc.values.Value(int(i + s))
			if err != nil {
				return nil, err
			}

			var b string
			if mvc.values.IsNull(int(i + s)) {
				b = "null"
			} else if mvc.complexValue {
				b = v.(string)
			} else {
				vb, err := marshal(v)
				if err != nil {
					return nil, err
				}
				b = string(vb)
			}

			if !strings.HasPrefix(string(key), "\"") {
				r = r + "\"" + string(key) + "\":"
			} else {
				r = r + string(key) + ":"
			}

			r = r + b
			if i < len-1 {
				r = r + ","
			}
		}
		r = r + "}"

		return r, nil
	}
	return nil, nil
}

func (mvc *mapValueContainer) IsNull(i int) bool {
	return mvc.mapArray.IsNull(i)
}

func (mvc *mapValueContainer) Release() {
	if mvc.mapArray != nil {
		mvc.mapArray.Release()
	}

	if mvc.values != nil {
		mvc.values.Release()
	}

	if mvc.keys != nil {
		mvc.keys.Release()
	}
}

func (mvc *mapValueContainer) SetValueArray(colData arrow.ArrayData) error {
	mvc.mapArray = array.NewMapData(colData)
	err := mvc.values.SetValueArray(mvc.mapArray.Items().Data())
	if err != nil {
		return err
	}
	err = mvc.keys.SetValueArray(mvc.mapArray.Keys().Data())

	return err
}

type structValueContainer struct {
	structArray     *array.Struct
	fieldNames      []string
	complexValue    []bool
	fieldValues     []columnValues
	structArrayType *arrow.StructType
}

var _ columnValues = (*structValueContainer)(nil)

func (svc *structValueContainer) Value(i int) (any, error) {
	if i < svc.structArray.Len() {
		r := "{"
		for j := range svc.fieldValues {
			r = r + "\"" + svc.fieldNames[j] + "\":"

			if svc.fieldValues[j].IsNull(int(i)) {
				r = r + "null"
			} else {
				v, err := svc.fieldValues[j].Value(int(i))
				if err != nil {
					return nil, err
				}

				var b string
				if svc.complexValue[j] {
					b = v.(string)
				} else {
					vb, err := marshal(v)
					if err != nil {
						return nil, err
					}
					b = string(vb)
				}

				r = r + b
			}
			if j < len(svc.fieldValues)-1 {
				r = r + ","
			}
		}
		r = r + "}"

		return r, nil
	}
	return nil, nil
}

func (svc *structValueContainer) IsNull(i int) bool {
	return svc.structArray.IsNull(i)
}

func (svc *structValueContainer) Release() {
	if svc.structArray != nil {
		svc.structArray.Release()
	}

	for i := range svc.fieldValues {
		if svc.fieldValues[i] != nil {
			svc.fieldValues[i].Release()
		}
	}
}

func (svc *structValueContainer) SetValueArray(colData arrow.ArrayData) error {
	svc.structArray = array.NewStructData(colData)
	for i := range svc.fieldValues {
		err := svc.fieldValues[i].SetValueArray(svc.structArray.Field(i).Data())
		if err != nil {
			return err
		}
	}

	return nil
}

type dateValueContainer struct {
	dateArray *array.Date32
	location  *time.Location
}

var _ columnValues = (*dateValueContainer)(nil)

func (dvc *dateValueContainer) Value(i int) (any, error) {
	d32 := dvc.dateArray.Value(i)

	val := d32.ToTime().In(dvc.location)
	return val, nil
}

func (dvc *dateValueContainer) IsNull(i int) bool {
	return dvc.dateArray.IsNull(i)
}

func (dvc *dateValueContainer) Release() {
	if dvc.dateArray != nil {
		dvc.dateArray.Release()
	}
}

func (dvc *dateValueContainer) SetValueArray(colData arrow.ArrayData) error {
	dvc.dateArray = array.NewDate32Data(colData)
	return nil
}

type timestampValueContainer struct {
	timeArray     *array.Timestamp
	location      *time.Location
	toTimestampFn func(arrow.Timestamp) time.Time
}

var _ columnValues = (*timestampValueContainer)(nil)

func (tvc *timestampValueContainer) Value(i int) (any, error) {
	ats := tvc.timeArray.Value(i)
	val := tvc.toTimestampFn(ats).In(tvc.location)

	return val, nil
}

func (tvc *timestampValueContainer) IsNull(i int) bool {
	return tvc.timeArray.IsNull(i)
}

func (tvc *timestampValueContainer) Release() {
	if tvc.timeArray != nil {
		tvc.timeArray.Release()
	}
}

func (tvc *timestampValueContainer) SetValueArray(colData arrow.ArrayData) error {
	tvc.timeArray = array.NewTimestampData(colData)
	return nil
}

type timestampStringValueContainer struct {
	timeStringArray *array.String
	location        *time.Location
	fieldName       string
	*dbsqllog.DBSQLLogger
}

var _ columnValues = (*timestampStringValueContainer)(nil)

func (tvc *timestampStringValueContainer) Value(i int) (any, error) {
	sv := tvc.timeStringArray.Value(i)
	val, err := rowscanner.HandleDateTime(sv, "TIMESTAMP", tvc.fieldName, tvc.location)
	if err != nil {
		tvc.Err(err).Msg(errArrowRowsDateTimeParse)
	}

	return val, nil
}

func (tvc *timestampStringValueContainer) IsNull(i int) bool {
	return tvc.timeStringArray.IsNull(i)
}

func (tvc *timestampStringValueContainer) Release() {
	if tvc.timeStringArray != nil {
		tvc.timeStringArray.Release()
	}
}

func (tvc *timestampStringValueContainer) SetValueArray(colData arrow.ArrayData) error {
	tvc.timeStringArray = array.NewStringData(colData)
	return nil
}

type decimal128Container struct {
	decimalArray *array.Decimal128
	scale        int32
}

var _ columnValues = (*decimal128Container)(nil)

func (tvc *decimal128Container) Value(i int) (any, error) {
	dv := tvc.decimalArray.Value(i)
	fv := dv.ToFloat64(tvc.scale)
	return fv, nil
}

func (tvc *decimal128Container) IsNull(i int) bool {
	return tvc.decimalArray.IsNull(i)
}

func (tvc *decimal128Container) Release() {
	if tvc.decimalArray != nil {
		tvc.decimalArray.Release()
	}
}

func (tvc *decimal128Container) SetValueArray(colData arrow.ArrayData) error {
	tvc.decimalArray = array.NewDecimal128Data(colData)
	return nil
}

func marshal(val any) ([]byte, error) {
	if t, ok := val.(time.Time); ok {
		s := "\"" + t.String() + "\""
		return []byte(s), nil
	}
	vb, err := json.Marshal(val)
	return vb, err
}

var nullContainer *nullContainer_ = &nullContainer_{}

type nullContainer_ struct {
}

var _ columnValues = (*nullContainer_)(nil)

func (tvc *nullContainer_) Value(i int) (any, error) {
	return nil, nil
}

func (tvc *nullContainer_) IsNull(i int) bool {
	return true
}

func (tvc *nullContainer_) Release() {
}

func (tvc *nullContainer_) SetValueArray(colData arrow.ArrayData) error {
	return nil
}

func makeRowsValues(sar ArrowRecord, colInfo []colInfo, location *time.Location, toTimestampFn timeStampFn, logger *dbsqllog.DBSQLLogger) (RowsValues, error) {

	arrowSchema := sar.Schema()
	d := rowscanner.NewDelimiter(sar.Start(), sar.Count())

	columnValueHolders := make([]columnValues, len(colInfo))
	for i, field := range arrowSchema.Fields() {
		holder, err := makeColumnValueContainer(field.Type, colInfo[i].dbType, location, toTimestampFn, colInfo[i].name)
		if err != nil {
			msg := errArrowRowsMakeRowValues(sar.Start(), sar.End(), colInfo[i])
			logger.Err(err).Msg(msg)
			return nil, errors.Wrap(err, msg)
		}

		columnValueHolders[i] = holder
	}

	rowValues := &rowsValues{Delimiter: d, columnValueHolders: columnValueHolders}

	// for each column we want to create an arrow array specific to the data type
	for i, col := range sar.Columns() {
		func() {
			col.Retain()
			defer col.Release()

			colData := col.Data()
			colData.Retain()
			defer colData.Release()

			err := rowValues.SetColumnValues(i, colData)
			if err != nil {
				panic(err.Error())
				// ars.Error().Msg(err.Error())
			}
		}()
	}

	// Update the delimiter in rowValues to reflect the currently loaded set of rows
	rowValues.SetDelimiter(rowscanner.NewDelimiter(sar.Start(), sar.Count()))
	return rowValues, nil
}

func makeColumnValueContainer(t arrow.DataType, dbType cli_service.TTypeId, location *time.Location, toTimestampFn timeStampFn, colName string) (columnValues, error) {
	if location == nil {
		location = time.UTC
	}

	switch t := t.(type) {

	case *arrow.BooleanType:
		return &columnValuesTyped[*array.Boolean, bool]{}, nil

	case *arrow.Int8Type:
		return &columnValuesTyped[*array.Int8, int8]{}, nil

	case *arrow.Int16Type:
		return &columnValuesTyped[*array.Int16, int16]{}, nil

	case *arrow.Int32Type:
		return &columnValuesTyped[*array.Int32, int32]{}, nil

	case *arrow.Int64Type:
		return &columnValuesTyped[*array.Int64, int64]{}, nil

	case *arrow.Float32Type:
		return &columnValuesTyped[*array.Float32, float32]{}, nil

	case *arrow.Float64Type:
		return &columnValuesTyped[*array.Float64, float64]{}, nil

	case *arrow.StringType:
		if dbType == cli_service.TTypeId_TIMESTAMP_TYPE {
			return &timestampStringValueContainer{location: location, fieldName: colName}, nil
		} else {
			return &columnValuesTyped[*array.String, string]{}, nil
		}

	case *arrow.Decimal128Type:
		return &decimal128Container{scale: t.Scale}, nil

	case *arrow.Date32Type:
		return &dateValueContainer{location: location}, nil

	case *arrow.TimestampType:
		return &timestampValueContainer{location: location, toTimestampFn: toTimestampFn}, nil

	case *arrow.BinaryType:
		return &columnValuesTyped[*array.Binary, []byte]{}, nil

	case *arrow.ListType:
		lvc := &listValueContainer{listArrayType: t}
		var err error
		lvc.values, err = makeColumnValueContainer(t.Elem(), cli_service.TTypeId_NULL_TYPE, location, toTimestampFn, "")
		if err != nil {
			return nil, err
		}
		switch t.Elem().(type) {
		case *arrow.MapType, *arrow.ListType, *arrow.StructType:
			lvc.complexValue = true
		}
		return lvc, nil

	case *arrow.MapType:
		mvc := &mapValueContainer{mapArrayType: t}
		var err error
		mvc.values, err = makeColumnValueContainer(t.ItemType(), cli_service.TTypeId_NULL_TYPE, location, toTimestampFn, "")
		if err != nil {
			return nil, err
		}
		mvc.keys, err = makeColumnValueContainer(t.KeyType(), cli_service.TTypeId_NULL_TYPE, location, toTimestampFn, "")
		if err != nil {
			return nil, err
		}
		switch t.ItemType().(type) {
		case *arrow.MapType, *arrow.ListType, *arrow.StructType:
			mvc.complexValue = true
		}

		return mvc, nil

	case *arrow.StructType:
		svc := &structValueContainer{structArrayType: t}
		svc.fieldNames = make([]string, len(t.Fields()))
		svc.fieldValues = make([]columnValues, len(t.Fields()))
		svc.complexValue = make([]bool, len(t.Fields()))
		for i, f := range t.Fields() {
			svc.fieldNames[i] = f.Name
			c, err := makeColumnValueContainer(f.Type, cli_service.TTypeId_NULL_TYPE, location, toTimestampFn, f.Name)
			if err != nil {
				return nil, err
			}
			svc.fieldValues[i] = c
			switch f.Type.(type) {
			case *arrow.MapType, *arrow.ListType, *arrow.StructType:
				svc.complexValue[i] = true
			}

		}

		return svc, nil

	case *arrow.NullType:
		return nullContainer, nil

	default:
		return nil, errors.Errorf(errArrowRowsUnhandledArrowType(t.String()))
	}
}
