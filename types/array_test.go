package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewArrayType(t *testing.T) {
	tests := map[string]struct {
		elementType  DataType
		containsNull bool
	}{
		"null":      {elementType: DataTypeNull, containsNull: true},
		"bool":      {elementType: DataTypeBool, containsNull: true},
		"int":       {elementType: DataTypeInteger, containsNull: true},
		"long":      {elementType: DataTypeLong, containsNull: true},
		"float":     {elementType: DataTypeFloat, containsNull: true},
		"double":    {elementType: DataTypeDouble, containsNull: true},
		"string":    {elementType: DataTypeString, containsNull: false},
		"binary":    {elementType: DataTypeBinary, containsNull: true},
		"date":      {elementType: DataTypeDate, containsNull: true},
		"timestamp": {elementType: DataTypeTimestamp, containsNull: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			arrayType := NewArrayType(test.elementType, test.containsNull)
			require.Equal(t, test.elementType, arrayType.ElementType)
			require.Equal(t, test.containsNull, arrayType.ContainsNull)
			t.Logf("str: %s", arrayType)
		})
	}
}

func TestArrayType_MarshalJSON(t *testing.T) {
	tests := map[string]struct {
		arrayType *ArrayType
		expected  string
	}{
		"null":      {arrayType: NewArrayType(DataTypeNull, true), expected: `{"type":"array","elementType":"null","containsNull":true}`},
		"bool":      {arrayType: NewArrayType(DataTypeBool, true), expected: `{"type":"array","elementType":"bool","containsNull":true}`},
		"int":       {arrayType: NewArrayType(DataTypeInteger, true), expected: `{"type":"array","elementType":"integer","containsNull":true}`},
		"long":      {arrayType: NewArrayType(DataTypeLong, true), expected: `{"type":"array","elementType":"long","containsNull":true}`},
		"float":     {arrayType: NewArrayType(DataTypeFloat, true), expected: `{"type":"array","elementType":"float","containsNull":true}`},
		"double":    {arrayType: NewArrayType(DataTypeDouble, true), expected: `{"type":"array","elementType":"double","containsNull":true}`},
		"string":    {arrayType: NewArrayType(DataTypeString, false), expected: `{"type":"array","elementType":"string","containsNull":false}`},
		"binary":    {arrayType: NewArrayType(DataTypeBinary, true), expected: `{"type":"array","elementType":"binary","containsNull":true}`},
		"date":      {arrayType: NewArrayType(DataTypeDate, true), expected: `{"type":"array","elementType":"date","containsNull":true}`},
		"timestamp": {arrayType: NewArrayType(DataTypeTimestamp, true), expected: `{"type":"array","elementType":"timestamp","containsNull":true}`},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := test.arrayType.MarshalJSON()
			require.NoErrorf(t, err, "unexpected error: %s", err)
			require.JSONEq(t, test.expected, string(actual), "expected %s, got %s", test.expected, string(actual))
			t.Logf("json: %s", string(actual))
		})
	}
}
