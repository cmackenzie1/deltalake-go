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

// UnmarshalJSON is a custom JSON unmarshaller for the Metadata struct.
func (m *Metadata) UnmarshalJSON(data []byte) error {
	m.Configuration = make(map[string]string)
	m.PartitionColumns = make([]string, 0)
	type Alias Metadata // prevent recursion
	return json.Unmarshal(data, (*Alias)(m))
}

func (m *Metadata) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	m.Configuration = make(map[string]string)
	m.PartitionColumns = make([]string, 0)

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

	m.ID = row[id.ColumnIndex].String()
	m.TableName = row[name.ColumnIndex].String()
	m.Description = row[description.ColumnIndex].String()
	m.SchemaString = row[schemaString.ColumnIndex].String()
	m.CreatedTime = row[createdTime.ColumnIndex].Int64()
	
	return nil
}
