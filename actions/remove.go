package actions

import (
	"encoding/json"
	"fmt"

	"github.com/segmentio/parquet-go"
)

var _ Action = (*Remove)(nil)

// Remove is a struct that represents a remove action to the delta log.
type Remove struct {
	// Path is the relative path of the file to remove from the root of the table.
	Path string `json:"path"`
	// DeletionTimestamp is the time the deletion occurred, represented as milliseconds since the epoch
	DeletionTimestamp int64 `json:"deletionTimestamp,omitempty"`
	// DataChange is a boolean that indicates whether the file contains data changes.
	// If false, the data in the file is a result of a compaction or other operation.
	DataChange bool `json:"dataChange"`
	// ExtendedFileMetadata when true the Fields_ partitionValues, size, and tags are present
	ExtendedFileMetadata bool `json:"extendedFileMetadata,omitempty"`
	// PartitionValues is a map of partition column Name_ to value.
	PartitionValues map[string]string `json:"partitionValues,omitempty"`
	// Size is the size of the file in bytes.
	Size int64 `json:"size,omitempty"`
	// Tags contains additional information about the file.
	Tags map[string]string `json:"tags,omitempty"`
}

func (r *Remove) Name() string {
	return "remove"
}

func NewRemove(
	path string,
	deletionTimestamp int64,
	dataChange bool,
	extendedFileMetadata bool,
	partitionValues map[string]string,
	size int64,
	tags map[string]string,
) *Remove {
	if partitionValues == nil {
		partitionValues = make(map[string]string)
	}
	if tags == nil {
		tags = make(map[string]string)
	}
	return &Remove{
		Path:                 path,
		DeletionTimestamp:    deletionTimestamp,
		DataChange:           dataChange,
		ExtendedFileMetadata: extendedFileMetadata,
		PartitionValues:      partitionValues,
		Size:                 size,
		Tags:                 tags,
	}
}

func (r *Remove) pathDecoded() (string, error) {
	return decodePath(r.Path)
}

// MarshalJSON marshals the remove action to JSON.
// The data wrapped in the key "remove".
func (r *Remove) MarshalJSON() ([]byte, error) {
	type Alias Remove // prevent recursion
	return json.Marshal(map[string]interface{}{
		"remove": (*Alias)(r),
	})
}

// UnmarshalJSON unmarshals the remove action from JSON.
func (r *Remove) UnmarshalJSON(data []byte) error {
	type Alias Remove // prevent recursion
	var wrapper struct {
		Remove *Alias `json:"remove"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	*r = Remove(*wrapper.Remove)
	if r.PartitionValues == nil {
		r.PartitionValues = make(map[string]string)
	}
	if r.Tags == nil {
		r.Tags = make(map[string]string)
	}
	return nil
}

func (r *Remove) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	if r.PartitionValues == nil {

	}
	if r.Tags == nil {
		r.Tags = make(map[string]string)
	}

	path, ok := schema.Lookup("remove", "path")
	if !ok {
		return fmt.Errorf("could not find path in schema")
	}

	deletionTimestamp, ok := schema.Lookup("remove", "deletionTimestamp")
	if !ok {
		return fmt.Errorf("could not find deletionTimestamp in schema")
	}

	dataChange, ok := schema.Lookup("remove", "dataChange")
	if !ok {
		return fmt.Errorf("could not find dataChange in schema")
	}

	extendedFileMeta, ok := schema.Lookup("remove", "extendedFileMetadata")
	if !ok {
		return fmt.Errorf("could not find extendedFileMetadata in schema")
	}

	size, ok := schema.Lookup("remove", "size")
	if !ok {
		return fmt.Errorf("could not find size in schema")
	}

	*r = Remove{
		Path:                 row[path.ColumnIndex].String(),
		DeletionTimestamp:    row[deletionTimestamp.ColumnIndex].Int64(),
		DataChange:           row[dataChange.ColumnIndex].Boolean(),
		ExtendedFileMetadata: row[extendedFileMeta.ColumnIndex].Boolean(),
		Size:                 row[size.ColumnIndex].Int64(),
		PartitionValues:      make(map[string]string),
		Tags:                 make(map[string]string),
	}

	return nil
}
