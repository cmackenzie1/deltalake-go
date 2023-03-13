package types

import (
	"encoding/json"
	"fmt"
)

// A MapType is a type that represents a map of keys of a single type to values of a single type. Keys are not allowed to be null.
// https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/types/MapType.scala
type MapType struct {
	KeyType           DataType `json:"keyType,omitempty"`
	ValueType         DataType `json:"valueType,omitempty"`
	ValueContainsNull bool     `json:"ValueContainsNull,omitempty"`
}

func NewMapType(keyType, valueType DataType, valueContainsNull bool) *MapType {
	return &MapType{
		KeyType:           keyType,
		ValueType:         valueType,
		ValueContainsNull: valueContainsNull,
	}
}

func (t *MapType) Type() DataType {
	return "map"
}

func (t *MapType) String() string {
	return fmt.Sprintf("map<%s, %s>", t.KeyType, t.ValueType)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (t *MapType) UnmarshalJSON(data []byte) error {
	var v struct {
		KeyType           DataType `json:"keyType"`
		ValueType         DataType `json:"valueType"`
		ValueContainsNull bool     `json:"ValueContainsNull"`
	}

	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	t.KeyType = v.KeyType
	t.ValueType = v.ValueType
	t.ValueContainsNull = v.ValueContainsNull

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (t *MapType) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type              string   `json:"type"`
		KeyType           DataType `json:"keyType"`
		ValueType         DataType `json:"valueType"`
		ValueContainsNull bool     `json:"valueContainsNull"`
	}{
		Type:              "map",
		KeyType:           t.KeyType,
		ValueType:         t.ValueType,
		ValueContainsNull: t.ValueContainsNull,
	})
}
