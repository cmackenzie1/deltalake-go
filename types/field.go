package types

import (
	"encoding/json"
	"fmt"
)

// A StructField is a field in a StructType containing a name, a type, and a flag for whether the field is nullable or not.
// The metadata is a map of string to string that can be used to store additional information about the field.
type StructField struct {
	Name     string            `json:"name"`
	Type     DataType          `json:"type"`
	Nullable bool              `json:"nullable"`
	Metadata map[string]string `json:"metadata"`

	// Support complex types such as array, map, and struct by storing the inner type instead of using reflection repeatedly
	innerArray  *ArrayType
	innerMap    *MapType
	innerStruct *StructType
}

func NewStructField(name string, dtype any, nullable bool, metadata map[string]string) *StructField {
	if metadata == nil {
		metadata = make(map[string]string) // initialize metadata to an empty map for json.Marshal to always output the metadata field as an empty object
	}

	var dataType DataType
	var innerArray *ArrayType
	var innerMap *MapType
	var innerStruct *StructType

	// Support complex types such as array, map, and struct
	switch dtype.(type) {
	case DataType:
		dataType = dtype.(DataType)
	case *ArrayType:
		dataType = "array"
		innerArray = dtype.(*ArrayType)
	case *MapType:
		dataType = "map"
		innerMap = dtype.(*MapType)
	case *StructType:
		dataType = "struct"
		innerStruct = dtype.(*StructType)
	default:
		panic(fmt.Sprintf("unsupported type: %T", dtype)) // TODO: return error instead of panic?
	}

	return &StructField{
		Name:        name,
		Type:        dataType,
		Nullable:    nullable,
		Metadata:    metadata,
		innerArray:  innerArray,
		innerMap:    innerMap,
		innerStruct: innerStruct,
	}
}

func (f *StructField) String() string {
	switch f.Type {
	case "array":
		return fmt.Sprintf("StructField<%s, %s, nullable = %t>", f.Name, f.innerArray, f.Nullable)
	case "map":
		return fmt.Sprintf("StructField<%s, %s, nullable = %t>", f.Name, f.innerMap, f.Nullable)
	case "struct":
		return fmt.Sprintf("StructField<%s, %s, nullable = %t>", f.Name, f.innerStruct, f.Nullable)
	default:
		return fmt.Sprintf("StructField<%s, %s, nullable = %t>", f.Name, f.Type, f.Nullable)
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (f *StructField) UnmarshalJSON(data []byte) error {
	var v struct {
		Name     string            `json:"name"`
		Type     DataType          `json:"type"`
		Nullable bool              `json:"nullable"`
		Metadata map[string]string `json:"metadata"`
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	f.Name = v.Name
	f.Type = v.Type
	f.Nullable = v.Nullable
	f.Metadata = v.Metadata

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (f *StructField) MarshalJSON() ([]byte, error) {
	var j struct {
		Name     string            `json:"name"`
		Type     any               `json:"type"`
		Nullable bool              `json:"nullable"`
		Metadata map[string]string `json:"metadata"`
	}

	j.Name = f.Name
	j.Nullable = f.Nullable
	j.Metadata = f.Metadata

	switch f.Type {
	case "array":
		j.Type = f.innerArray
		return json.Marshal(j)
	case "map":
		j.Type = f.innerMap
		return json.Marshal(j)
	case "struct":
		j.Type = f.innerStruct
		return json.Marshal(j)
	}

	j.Type = f.Type
	return json.Marshal(j)
}
