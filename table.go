package delta

import (
	"deltalake/storage"
	"deltalake/types"
	"fmt"
	"github.com/google/uuid"
	"time"
)

type TableMetadata struct {
	// ID is the unique identifier of the table.
	ID string
	// Name is the user-provided name of the table.
	Name string
	// Description is the user-provided description of the table.
	Description string
	// Format is the format of the table. Default is "parquet".
	Format Format
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
	format Format,
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

// TableState is the state of a delta table.
type TableState struct {
	Version int64
}

type TableConfig struct {
	RequireTombstones bool
	RequireFiles      bool
}

var DefaultTableConfig = TableConfig{
	RequireTombstones: true,
	RequireFiles:      true,
}

// Table is a struct that represents a delta table.
type Table struct {
	State             TableState
	Config            TableConfig
	Storage           storage.ObjectStorage
	LastCheckpoint    *Checkpoint
	VersionTimestamps map[int64]int64
}

func NewTable(storage storage.ObjectStorage, config TableConfig) *Table {
	return &Table{
		State: TableState{
			Version: -1,
		},
		Storage:           storage,
		Config:            config,
		VersionTimestamps: make(map[int64]int64),
	}
}

func NewTableWithState(storage storage.ObjectStorage, config TableConfig, state TableState) *Table {
	return &Table{
		State:             state,
		Storage:           storage,
		Config:            config,
		VersionTimestamps: make(map[int64]int64),
	}
}

func (t *Table) TableURI() string {
	panic("implement me")
}
