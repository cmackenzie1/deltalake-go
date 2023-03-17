package actions

import (
	"encoding/json"
	"fmt"

	"github.com/segmentio/parquet-go"
)

var _ Action = (*Metadata)(nil)

type Metadata struct {
	// ID is the unique identifier of the table.
	ID string `json:"id"`
	// TableName is the Name_ of the table.
	TableName string `json:"name,omitempty"`
	// Description is the description of the table.
	Description string `json:"description,omitempty"`
	// Format is the format of the table. Default is "parquet".
	Format Format `json:"format"`
	// SchemaString is the types of the table in JSON format.
	SchemaString string `json:"schemaString"`
	// PartitionColumns is an ordered list of partition columns.
	PartitionColumns []string `json:"partitionColumns"`
	// CreatedTime is the time the table was created. Milliseconds since epoch.
	CreatedTime int64 `json:"createdTime,omitempty"`
	// Configuration is a map of configuration key to value.
	Configuration map[string]string `json:"configuration"`
}

func NewMetadata(
	id string,
	name string,
	description string,
	format Format,
	schemaString string,
	partitionColumns []string,
	createdTime int64,
	configuration map[string]string,
) *Metadata {
	if partitionColumns == nil {
		partitionColumns = make([]string, 0)
	}
	if configuration == nil {
		configuration = make(map[string]string)
	}
	return &Metadata{
		ID:               id,
		TableName:        name,
		Description:      description,
		Format:           format,
		SchemaString:     schemaString,
		PartitionColumns: partitionColumns,
		CreatedTime:      createdTime,
		Configuration:    configuration,
	}
}

func (m *Metadata) Name() string {
	return "metaData"
}

// MarshalJSON is a custom JSON marshaller for the Metadata struct.
// The data wrapped in the key "metaData".
func (m *Metadata) MarshalJSON() ([]byte, error) {
	type Alias Metadata
	return json.Marshal(map[string]interface{}{
		"metaData": (*Alias)(m),
	})
}

// UnmarshalJSON is a custom JSON unmarshaller for the Metadata struct.
func (m *Metadata) UnmarshalJSON(data []byte) error {
	type Alias Metadata
	var wrapper struct {
		Metadata *Alias `json:"metaData"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	*m = Metadata(*wrapper.Metadata)
	return nil
}

func (m *Metadata) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	id, ok := schema.Lookup("metaData", "id")
	if !ok {
		return fmt.Errorf("could not find id in schema")
	}

	name, ok := schema.Lookup("metaData", "name")
	if !ok {
		return fmt.Errorf("could not find name in schema")
	}

	description, ok := schema.Lookup("metaData", "description")
	if !ok {
		return fmt.Errorf("could not find description in schema")
	}

	schemaString, ok := schema.Lookup("metaData", "schemaString")
	if !ok {
		return fmt.Errorf("could not find schemaString in schema")
	}

	// TODO: format

	createdTime, ok := schema.Lookup("metaData", "createdTime")
	if !ok {
		return fmt.Errorf("could not find createdTime in schema")
	}

	*m = Metadata{
		ID:           row[id.ColumnIndex].String(),
		TableName:    row[name.ColumnIndex].String(),
		Description:  row[description.ColumnIndex].String(),
		SchemaString: row[schemaString.ColumnIndex].String(),
		CreatedTime:  row[createdTime.ColumnIndex].Int64(),
	}

	return nil
}
