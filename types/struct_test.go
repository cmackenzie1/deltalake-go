package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSchema(t *testing.T) {
	tests := map[string]struct {
		fields []*StructField
	}{
		"empty": {fields: []*StructField{}},
		"one": {fields: []*StructField{NewStructField(
			"id",
			DataTypeInteger,
			false,
			nil,
		)}},
		"two": {fields: []*StructField{
			NewStructField("id", DataTypeInteger, false, nil),
			NewStructField("name", DataTypeString, false, nil),
		}},
		"map": {fields: []*StructField{
			NewStructField("id", DataTypeInteger, false, nil),
			NewStructField("data", NewMapType(DataTypeString, DataTypeString, false), false, nil),
		}},
		"nested": {fields: []*StructField{
			NewStructField("id", DataTypeInteger, false, nil),
			NewStructField("data", NewMapType(DataTypeString, DataTypeString, false), false, nil),
			NewStructField("nested", NewStruct([]*StructField{
				NewStructField("id", DataTypeInteger, false, nil),
				NewStructField("name", DataTypeString, false, nil),
			}...), false, nil),
		}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			schema := NewStruct(test.fields...)
			require.Len(t, schema.Fields, len(test.fields))
			t.Logf("str: %s", schema)
		})
	}
}

func TestSchema_MarshalJSON(t *testing.T) {
	tests := map[string]struct {
		schema   *StructType
		expected string
	}{
		"empty": {schema: NewStruct([]*StructField{}...), expected: `{"type":"struct","fields":[]}`},

		"one": {schema: NewStruct([]*StructField{NewStructField(
			"id",
			DataTypeInteger,
			false,
			nil,
		)}...),
			expected: `{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}}]}`},

		"two": {schema: NewStruct([]*StructField{
			NewStructField("id", DataTypeInteger, false, nil),
			NewStructField("name", DataTypeString, false, nil),
		}...),
			expected: `{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":false,"metadata":{}}]}`},

		"map": {schema: NewStruct([]*StructField{
			NewStructField("id", DataTypeInteger, false, nil),
			NewStructField("data", NewMapType(DataTypeString, DataTypeString, false), false, nil),
		}...),
			expected: `{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"data","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":false},"nullable":false,"metadata":{}}]}`},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := test.schema.MarshalJSON()
			require.NoErrorf(t, err, "unexpected error: %s", err)
			require.JSONEq(t, test.expected, string(actual), "expected %s, got %s", test.expected, string(actual))
			t.Logf("json: %s", string(actual))
		})
	}
}

func TestParseMapToStructField(t *testing.T) {
	tests := map[string]struct {
		input    map[string]interface{}
		expected *StructField
	}{
		"integer": {
			input: map[string]interface{}{
				"name":     "id",
				"type":     "integer",
				"nullable": false,
				"metadata": map[string]string{},
			},
			expected: NewStructField("id", DataTypeInteger, false, nil),
		},
		"array": {
			input: map[string]interface{}{
				"name":     "id",
				"type":     map[string]interface{}{"type": "array", "elementType": "string", "containsNull": false},
				"nullable": false,
				"metadata": map[string]string{},
			},
			expected: NewStructField("id", NewArrayType(DataTypeString, false), false, nil),
		},
		"map": {
			input: map[string]interface{}{
				"name":     "id",
				"type":     map[string]interface{}{"type": "map", "keyType": "string", "valueType": "string", "valueContainsNull": false},
				"nullable": false,
				"metadata": map[string]string{},
			},
			expected: NewStructField("id", NewMapType(DataTypeString, DataTypeString, false), false, nil),
		},
		"struct": {
			input: map[string]interface{}{
				"name":     "id",
				"type":     map[string]interface{}{"type": "struct", "fields": []map[string]interface{}{}},
				"nullable": false,
				"metadata": map[string]string{},
			},
			expected: NewStructField("id", NewStruct([]*StructField{}...), false, nil),
		},
		"nested": {
			input: map[string]interface{}{
				"name": "id",
				"type": map[string]interface{}{
					"type": "struct",
					"fields": []map[string]interface{}{{
						"name":     "id",
						"type":     "integer",
						"nullable": false,
						"metadata": map[string]string{},
					}},
				},
				"nullable": false,
				"metadata": map[string]string{},
			},
			expected: NewStructField("id", NewStruct([]*StructField{NewStructField(
				"id",
				DataTypeInteger,
				false,
				nil,
			)}...), false, nil),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := ParseMapToStructField(test.input)
			require.NoErrorf(t, err, "unexpected error: %s", err)
			require.Equal(t, test.expected, actual, "expected %s, got %s", test.expected, actual)
		})
	}
}
