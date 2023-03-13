package types

import (
	"encoding/json"
	"fmt"
)

// An ArrayType is a type that represents an array of elements of a single type.
// https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types/ArrayType.scala
type ArrayType struct {
	ElementType  DataType `json:"elementType"`
	ContainsNull bool     `json:"containsNull"`
}

func NewArrayType(elementType DataType, containsNull bool) *ArrayType {
	return &ArrayType{
		ElementType:  elementType,
		ContainsNull: containsNull,
	}
}

func (t *ArrayType) Type() DataType {
	return "array"
}

func (t *ArrayType) String() string {
	return fmt.Sprintf("array<%s>", t.ElementType)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (t *ArrayType) UnmarshalJSON(data []byte) error {
	var v struct {
		ElementType  DataType `json:"elementType"`
		ContainsNull bool     `json:"containsNull"`
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	t.ElementType = v.ElementType
	t.ContainsNull = v.ContainsNull

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (t *ArrayType) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type         string   `json:"type"`
		ElementType  DataType `json:"elementType"`
		ContainsNull bool     `json:"containsNull"`
	}{
		Type:         "array",
		ElementType:  t.ElementType,
		ContainsNull: t.ContainsNull,
	})
}
