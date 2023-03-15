package types

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// A StructType is a struct type containing a list of fields.
// This is used to represent a schema and is the top-level type in a schema.
// https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types/StructType.scala
type StructType struct {
	Fields []*StructField `json:"fields"`
}

func NewStruct(fields ...*StructField) *StructType {
	return &StructType{
		Fields: fields,
	}
}

// AddField adds a field to the struct
func (t *StructType) AddField(field *StructField) {
	t.Fields = append(t.Fields, field)
}

func (t *StructType) GetFieldByName(name string) (*StructField, error) {
	for _, field := range t.Fields {
		if field.Name == name {
			return field, nil
		}
	}
	return nil, fmt.Errorf("field %s not found", name)
}

func (t *StructType) String() string {
	sb := &strings.Builder{}
	sb.WriteString("StructType<")
	for i, field := range t.Fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(field.String())
	}
	sb.WriteString(">")
	return sb.String()
}

// MarshalJSON implements the json.Marshaler interface.
func (t *StructType) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string         `json:"type"`
		Fields []*StructField `json:"fields"`
	}{
		Type:   "struct",
		Fields: t.Fields,
	})
}

func ParseMapToStructField(m map[string]interface{}) (*StructField, error) {
	if len(m) == 0 {
		return nil, fmt.Errorf("empty map")
	}

	// Primitive types
	if reflect.TypeOf(m["type"]).Kind() == reflect.String {
		// If it's a primitive type, we can just return a new StructField
		if IsPrimitiveType(DataType(m["type"].(string))) {
			return &StructField{
				Name:     m["name"].(string),
				Type:     DataType(m["type"].(string)),
				Metadata: map[string]string{},
			}, nil
		}
	}

	if reflect.TypeOf(m["type"]).Kind() == reflect.Map {
		typeMap := m["type"].(map[string]interface{})
		// If it's an array type, we need to parse the element type
		if typeMap["type"].(string) == "array" {
			elementType := typeMap["elementType"].(string)
			containsNull := typeMap["containsNull"].(bool)
			return NewStructField(
				m["name"].(string),
				NewArrayType(DataType(elementType), containsNull),
				false,
				map[string]string{}), nil
		}

		// If it's a map type, we need to parse the key and value types
		if typeMap["type"].(string) == "map" {
			keyType := typeMap["keyType"].(string)
			valueType := typeMap["valueType"].(string)
			valueContainsNull := typeMap["valueContainsNull"].(bool)
			return NewStructField(
				m["name"].(string),
				NewMapType(DataType(keyType), DataType(valueType), valueContainsNull),
				false,
				map[string]string{}), nil
		}

		// If it's a struct type, we need to parse the fields recursively
		if typeMap["type"].(string) == "struct" {
			fields := typeMap["fields"].([]map[string]interface{})
			structFields := make([]*StructField, len(fields))
			for i, field := range fields {
				structField, err := ParseMapToStructField(field)
				if err != nil {
					return nil, err
				}
				structFields[i] = structField
			}
			return NewStructField(
				m["name"].(string),
				NewStruct(structFields...),
				false,
				map[string]string{}), nil
		}
	}
	return nil, fmt.Errorf("unsupported type: %v", m["type"])
}
