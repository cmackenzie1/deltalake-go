package actions

import "encoding/json"

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

// MarshalJSON marshals the add action to JSON.
// The data wrapped in the key "add".
func (a *Add) MarshalJSON() ([]byte, error) {
	type Alias Add // prevent recursion
	return json.Marshal(map[string]interface{}{
		"add": (*Alias)(a),
	})
}

// UnmarshalJSON unmarshals the add action from JSON.
// The data to unmarshal is wrapped in the key "add".
func (a *Add) UnmarshalJSON(data []byte) error {
	type Alias Add // prevent recursion
	var wrapper struct {
		Add *Alias `json:"add"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	*a = Add(*wrapper.Add)

	// Because of the way the JSON is unmarshaled, the maps will be nil if they are empty, so we need to
	// initialize them to empty maps.
	if a.PartitionValues == nil {
		a.PartitionValues = make(map[string]string)
	}
	if a.Tags == nil {
		a.Tags = make(map[string]string)
	}
	return nil
}
