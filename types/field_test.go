package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewField(t *testing.T) {
	tests := map[string]struct {
		name     string
		dtype    any
		nullable bool
		metadata map[string]string
	}{
		"null":      {name: "null", dtype: DataTypeNull, nullable: true, metadata: nil},
		"bool":      {name: "bool", dtype: DataTypeBool, nullable: true, metadata: nil},
		"int":       {name: "int", dtype: DataTypeInteger, nullable: true, metadata: nil},
		"long":      {name: "long", dtype: DataTypeLong, nullable: true, metadata: nil},
		"float":     {name: "float", dtype: DataTypeFloat, nullable: true, metadata: nil},
		"double":    {name: "double", dtype: DataTypeDouble, nullable: true, metadata: nil},
		"string":    {name: "string", dtype: DataTypeString, nullable: false, metadata: nil},
		"binary":    {name: "binary", dtype: DataTypeBinary, nullable: true, metadata: nil},
		"date":      {name: "date", dtype: DataTypeDate, nullable: true, metadata: nil},
		"timestamp": {name: "timestamp", dtype: DataTypeTimestamp, nullable: true, metadata: nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			field := NewStructField(test.name, test.dtype, test.nullable, test.metadata)
			require.Equal(t, test.name, field.Name)
			require.Equal(t, test.dtype, field.Type)
			require.Equal(t, test.nullable, field.Nullable)
			require.NotNil(t, field.Metadata)
			t.Logf("str: %s", field)
		})
	}
}

func TestField_MarshalJSON(t *testing.T) {
	tests := map[string]struct {
		field    *StructField
		expected string
	}{
		"null":      {field: NewStructField("null", DataTypeNull, true, nil), expected: `{"name":"null","type":"null","nullable":true,"metadata":{}}`},
		"bool":      {field: NewStructField("bool", DataTypeBool, true, nil), expected: `{"name":"bool","type":"bool","nullable":true,"metadata":{}}`},
		"integer":   {field: NewStructField("integer", DataTypeInteger, true, nil), expected: `{"name":"integer","type":"integer","nullable":true,"metadata":{}}`},
		"long":      {field: NewStructField("long", DataTypeLong, true, nil), expected: `{"name":"long","type":"long","nullable":true,"metadata":{}}`},
		"float":     {field: NewStructField("float", DataTypeFloat, true, nil), expected: `{"name":"float","type":"float","nullable":true,"metadata":{}}`},
		"double":    {field: NewStructField("double", DataTypeDouble, true, nil), expected: `{"name":"double","type":"double","nullable":true,"metadata":{}}`},
		"string":    {field: NewStructField("string", DataTypeString, false, nil), expected: `{"name":"string","type":"string","nullable":false,"metadata":{}}`},
		"binary":    {field: NewStructField("binary", DataTypeBinary, true, nil), expected: `{"name":"binary","type":"binary","nullable":true,"metadata":{}}`},
		"date":      {field: NewStructField("date", DataTypeDate, true, nil), expected: `{"name":"date","type":"date","nullable":true,"metadata":{}}`},
		"timestamp": {field: NewStructField("timestamp", DataTypeTimestamp, true, nil), expected: `{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}}`},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := test.field.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, test.expected, string(actual))
			t.Logf("json: %s", actual)
		})
	}
}
