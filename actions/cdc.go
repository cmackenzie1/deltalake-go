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

// UnmarshalJSON unmarshals the cdc action from JSON.
func (c *CDC) UnmarshalJSON(data []byte) error {
	c.Tags = make(map[string]string)
	c.PartitionValues = make(map[string]string)
	type Alias CDC // prevent recursion
	return json.Unmarshal(data, (*Alias)(c))
}

func (c *CDC) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	c.Tags = make(map[string]string)
	c.PartitionValues = make(map[string]string)

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

	c.Path = row[path.ColumnIndex].String()
	c.Size = row[size.ColumnIndex].Int64()
	c.DataChange = row[dataChange.ColumnIndex].Boolean()

	return nil
}
