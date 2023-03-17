package deltalake

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"deltalake/actions"
	"deltalake/types"
)

type TableMetadata struct {
	// ID is the unique identifier of the table.
	ID string
	// Name is the user-provided name of the table.
	Name string
	// Description is the user-provided description of the table.
	Description string
	// Format is the format of the table. Default is "parquet".
	Format actions.Format
	// Schema is the types of the table.
	Schema types.StructType
	// PartitionColumns is an	 ordered list of partition columns.
	PartitionColumns []string
	// CreatedTime is the time the table was created. Milliseconds since epoch.
	CreatedTime int64
	// Configuration is a map of configuration key to value.
	Configuration map[string]string
}

func NewTableMetadata(
	name string,
	description string,
	format actions.Format,
	schema types.StructType,
	partitionColumns []string,
	configuration map[string]string,
) *TableMetadata {
	return &TableMetadata{
		ID:               uuid.New().String(),
		Name:             name,
		Description:      description,
		Format:           format,
		Schema:           schema,
		PartitionColumns: partitionColumns,
		CreatedTime:      time.Now().UnixMilli(),
		Configuration:    configuration,
	}
}

func (m *TableMetadata) String() string {
	return fmt.Sprintf(
		"GUID=%s, Name_=%s, description=%s, partitionColumns=%s, createdTime=%d, configuration=%s",
		m.ID,
		m.Name,
		m.Description,
		m.PartitionColumns,
		m.CreatedTime,
		m.Configuration,
	)
}

func NewTableMetadataFromMap(m map[string]interface{}) (*TableMetadata, error) {
	metadata := &TableMetadata{}
	if id, ok := m["id"]; ok {
		metadata.ID = id.(string)
	}
	if name, ok := m["name"]; ok {
		metadata.Name = name.(string)
	}
	if description, ok := m["description"]; ok {
		metadata.Description = description.(string)
	}
	if format, ok := m["format"]; ok {
		formatMap := format.(map[string]interface{})
		if provider, ok := formatMap["provider"]; ok {
			metadata.Format.Provider = provider.(string)
		}
		if options, ok := formatMap["options"]; ok {
			metadata.Format.Options = options.(map[string]string)
		}
	}
	if schema, ok := m["schemaString"]; ok {
		var schemaType types.StructType
		err := json.Unmarshal([]byte(schema.(string)), &schemaType)
		if err != nil {
			return nil, err
		}
		metadata.Schema = schemaType
	}
	if partitionColumns, ok := m["partitionColumns"]; ok {
		metadata.PartitionColumns = partitionColumns.([]string)
	}
	if createdTime, ok := m["createdTime"]; ok {
		metadata.CreatedTime = int64(createdTime.(float64))
	}
	if configuration, ok := m["configuration"]; ok {
		metadata.Configuration = configuration.(map[string]string)
	}
	return metadata, nil
}

func (m *TableMetadata) TombstoneRetentionMillis() int64 {
	if m.Configuration != nil {
		tombstoneRetentionMillis, err := strconv.Atoi(m.Configuration["tombstoneRetentionDurationMillis"])
		if err != nil {
			return 0
		}
		return int64(tombstoneRetentionMillis)
	}
	return 0
}

func (m *TableMetadata) LogRetentionMillis() int64 {
	if m.Configuration != nil {
		logRetentionMillis, err := strconv.Atoi(m.Configuration["logRetentionDurationMillis"])
		if err != nil {
			return 0
		}
		return int64(logRetentionMillis)
	}
	return 0
}

func (m *TableMetadata) EnableLogExpiredCleanup() bool {
	if m.Configuration != nil {
		enableLogExpiredCleanup, err := strconv.ParseBool(m.Configuration["enableLogExpiredCleanup"])
		if err != nil {
			return false
		}
		return enableLogExpiredCleanup
	}
	return false
}
