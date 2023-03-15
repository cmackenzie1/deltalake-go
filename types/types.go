package types

type DataType string

// Primitive types defined in the Delta Lake specification.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types
const (
	DataTypeBinary    DataType = "binary"    // go: []byte
	DataTypeByte      DataType = "byte"      // go: int8
	DataTypeBool      DataType = "bool"      // go: bool
	DataTypeDate      DataType = "date"      // go: time.Time
	DataTypeDouble    DataType = "double"    // go: float64
	DataTypeFloat     DataType = "float"     // go: float32
	DataTypeInteger   DataType = "integer"   // go: int32
	DataTypeLong      DataType = "long"      // go: int64
	DataTypeNull      DataType = "null"      // go: nil
	DataTypeShort     DataType = "short"     // go: int16
	DataTypeString    DataType = "string"    // go: string
	DataTypeTimestamp DataType = "timestamp" // go: time.Time
)

func IsPrimitiveType(dt DataType) bool {
	switch dt {
	case
		DataTypeBinary,
		DataTypeByte,
		DataTypeBool,
		DataTypeDate,
		DataTypeDouble,
		DataTypeFloat,
		DataTypeInteger,
		DataTypeLong,
		DataTypeNull,
		DataTypeShort,
		DataTypeString,
		DataTypeTimestamp:
		return true
	}
	return false
}
