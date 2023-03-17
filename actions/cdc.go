package actions

import (
	"encoding/json"

	"github.com/segmentio/parquet-go"
)

var _ Action = (*CDC)(nil)

type CDC struct {
	Path            string            `json:"path"`
	PartitionValues map[string]string `json:"partitionValues"`
	Size            int64             `json:"size"`
	DataChange      bool              `json:"dataChange"`
	Tags            map[string]string `json:"tags,omitempty"`
}

func (c *CDC) Name() string {
	return "cdc"
}

func NewCDC(path string, partitionValues map[string]string, size int64, dataChange bool, tags map[string]string) *CDC {
	if partitionValues == nil {
		partitionValues = make(map[string]string)
	}
	if tags == nil {
		tags = make(map[string]string)
	}
	return &CDC{
		Path:            path,
		PartitionValues: partitionValues,
		Size:            size,
		DataChange:      dataChange,
		Tags:            tags,
	}
}

func (c *CDC) pathDecoded() (string, error) {
	return decodePath(c.Path)
}

// MarshalJSON marshals the cdc action to JSON.
// The data wrapped in the key "cdc".
func (c *CDC) MarshalJSON() ([]byte, error) {
	type Alias CDC // prevent recursion
	return json.Marshal(map[string]interface{}{
		"cdc": (*Alias)(c),
	})
}

// UnmarshalJSON unmarshals the cdc action from JSON.
func (c *CDC) UnmarshalJSON(data []byte) error {
	type Alias CDC // prevent recursion
	var wrapper struct {
		CDC *Alias `json:"cdc"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	*c = CDC(*wrapper.CDC)

	// Because of the way the JSON is unmarshaled, the maps will be nil if they are empty, so we need to
	// initialize them to empty maps.
	if c.PartitionValues == nil {
		c.PartitionValues = make(map[string]string)
	}
	if c.Tags == nil {
		c.Tags = make(map[string]string)
	}
	return nil
}

func (c *CDC) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	path, ok := schema.Lookup("cdc", "path")
	if !ok {
		return nil
	}

	size, ok := schema.Lookup("cdc", "size")
	if !ok {
		return nil
	}

	dataChange, ok := schema.Lookup("cdc", "dataChange")
	if !ok {
		return nil
	}

	*c = CDC{
		Path:       row[path.ColumnIndex].String(),
		Size:       row[size.ColumnIndex].Int64(),
		DataChange: row[dataChange.ColumnIndex].Boolean(),
	}

	return nil
}
