package types

type DataType string

const (
	DataTypeNull      DataType = "null"
	DataTypeBool      DataType = "bool"
	DataTypeInteger   DataType = "integer"
	DataTypeLong      DataType = "long"
	DataTypeFloat     DataType = "float"
	DataTypeDouble    DataType = "double"
	DataTypeString    DataType = "string"
	DataTypeBinary    DataType = "binary"
	DataTypeDate      DataType = "date"
	DataTypeTimestamp DataType = "timestamp"
)

func IsPrimitiveType(dt DataType) bool {
	switch dt {
	case DataTypeNull, DataTypeBool, DataTypeInteger, DataTypeLong, DataTypeFloat, DataTypeDouble, DataTypeString, DataTypeBinary, DataTypeDate, DataTypeTimestamp:
		return true
	}
	return false
}
