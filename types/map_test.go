package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMapType(t *testing.T) {
	tests := map[string]struct {
		keyType           DataType
		valueType         DataType
		valueContainsNull bool
	}{
		"null":      {keyType: DataTypeNull, valueType: DataTypeNull, valueContainsNull: true},
		"bool":      {keyType: DataTypeBool, valueType: DataTypeBool, valueContainsNull: true},
		"int":       {keyType: DataTypeInteger, valueType: DataTypeInteger, valueContainsNull: true},
		"long":      {keyType: DataTypeLong, valueType: DataTypeLong, valueContainsNull: true},
		"float":     {keyType: DataTypeFloat, valueType: DataTypeFloat, valueContainsNull: true},
		"double":    {keyType: DataTypeDouble, valueType: DataTypeDouble, valueContainsNull: true},
		"string":    {keyType: DataTypeString, valueType: DataTypeString, valueContainsNull: true},
		"binary":    {keyType: DataTypeBinary, valueType: DataTypeBinary, valueContainsNull: true},
		"date":      {keyType: DataTypeDate, valueType: DataTypeDate, valueContainsNull: true},
		"timestamp": {keyType: DataTypeTimestamp, valueType: DataTypeTimestamp, valueContainsNull: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mapType := NewMapType(test.keyType, test.valueType, test.valueContainsNull)
			require.Equal(t, test.keyType, mapType.KeyType)
			require.Equal(t, test.valueType, mapType.ValueType)
			require.Equal(t, test.valueContainsNull, mapType.ValueContainsNull)
			t.Logf("str: %s", mapType)
		})
	}
}

func TestMapType_MarshalJSON(t *testing.T) {
	tests := map[string]struct {
		mapType  *MapType
		expected string
	}{

		"null":      {mapType: NewMapType(DataTypeNull, DataTypeNull, true), expected: `{"type":"map","keyType":"null","valueType":"null","valueContainsNull":true}`},
		"bool":      {mapType: NewMapType(DataTypeBool, DataTypeBool, true), expected: `{"type":"map","keyType":"bool","valueType":"bool","valueContainsNull":true}`},
		"integer":   {mapType: NewMapType(DataTypeInteger, DataTypeInteger, true), expected: `{"type":"map","keyType":"integer","valueType":"integer","valueContainsNull":true}`},
		"long":      {mapType: NewMapType(DataTypeLong, DataTypeLong, true), expected: `{"type":"map","keyType":"long","valueType":"long","valueContainsNull":true}`},
		"float":     {mapType: NewMapType(DataTypeFloat, DataTypeFloat, true), expected: `{"type":"map","keyType":"float","valueType":"float","valueContainsNull":true}`},
		"double":    {mapType: NewMapType(DataTypeDouble, DataTypeDouble, true), expected: `{"type":"map","keyType":"double","valueType":"double","valueContainsNull":true}`},
		"string":    {mapType: NewMapType(DataTypeString, DataTypeString, false), expected: `{"type":"map","keyType":"string","valueType":"string","valueContainsNull":false}`},
		"binary":    {mapType: NewMapType(DataTypeBinary, DataTypeBinary, false), expected: `{"type":"map","keyType":"binary","valueType":"binary","valueContainsNull":false}`},
		"date":      {mapType: NewMapType(DataTypeDate, DataTypeDate, false), expected: `{"type":"map","keyType":"date","valueType":"date","valueContainsNull":false}`},
		"timestamp": {mapType: NewMapType(DataTypeTimestamp, DataTypeTimestamp, false), expected: `{"type":"map","keyType":"timestamp","valueType":"timestamp","valueContainsNull":false}`},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := test.mapType.MarshalJSON()
			require.NoErrorf(t, err, "unexpected error: %s", err)
			require.JSONEq(t, test.expected, string(actual), "expected %s, got %s", test.expected, string(actual))
			t.Logf("json: %s", string(actual))
		})
	}
}
