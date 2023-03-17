package actions

import (
	"encoding/json"
	"fmt"

	"github.com/segmentio/parquet-go"
)

var _ Action = (*Add)(nil)

// Add is a struct that represents an add action to a delta table.
type Add struct {
	// Path is the relative path of the file to add from the root of the table.
	Path string `json:"path"`
	// Size is the size of the file in bytes.
	Size int64 `json:"size"`
	// PartitionValues is a map of partition column Name_ to value.
	PartitionValues map[string]string `json:"partitionValues"`
	// DataChange is a boolean that indicates whether the file contains data changes.
	// If false, the data in the file is a result of a compaction or other operation.
	DataChange bool `json:"dataChange"`
	// ModificationTime is the time the file was last modified.
	ModificationTime int64 `json:"modificationTime"`
	// Stats Contains statistics (e.g., count, min/max values for columns) about the data in this logical file
	Stats *Stats `json:"stats,omitempty"`
	// Tags contains additional information about the file.
	Tags map[string]string `json:"tags,omitempty"`
}

func (a *Add) Name() string {
	return "add"
}

func NewAdd(path string, size int64, partitionValues map[string]string, dataChange bool, modificationTime int64, stats *Stats, tags map[string]string) *Add {
	if partitionValues == nil {
		partitionValues = make(map[string]string)
	}
	if tags == nil {
		tags = make(map[string]string)
	}
	return &Add{
		Path:             path,
		Size:             size,
		PartitionValues:  partitionValues,
		DataChange:       dataChange,
		ModificationTime: modificationTime,
		Stats:            stats,
		Tags:             tags,
	}
}

// pathDecoded returns the decoded path of the add action.
func (a *Add) pathDecoded() (string, error) {
	return decodePath(a.Path)
}

// UnmarshalJSON unmarshals the add action from JSON.
// The data to unmarshal is wrapped in the key "add".
func (a *Add) UnmarshalJSON(data []byte) error {
	a.Tags = make(map[string]string)
	a.PartitionValues = make(map[string]string)
	type Alias Add // prevent recursion
	return json.Unmarshal(data, (*Alias)(a))
}

func (a *Add) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	path, ok := schema.Lookup("add", "path")
	if !ok {
		return fmt.Errorf("path not found in schema")
	}

	size, ok := schema.Lookup("add", "size")
	if !ok {
		return fmt.Errorf("size not found in schema")
	}

	dataChange, ok := schema.Lookup("add", "dataChange")
	if !ok {
		return fmt.Errorf("dataChange not found in schema")
	}

	modificationTime, ok := schema.Lookup("add", "modificationTime")
	if !ok {
		return fmt.Errorf("modificationTime not found in schema")
	}

	// TODO: handle stats, tags and partitionValues

	*a = Add{
		Path:             row[path.ColumnIndex].String(),
		Size:             row[size.ColumnIndex].Int64(),
		DataChange:       row[dataChange.ColumnIndex].Boolean(),
		ModificationTime: row[modificationTime.ColumnIndex].Int64(),
	}

	return nil
}
