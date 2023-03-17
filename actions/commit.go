package actions

import (
	"encoding/json"

	"github.com/segmentio/parquet-go"
)

var _ Action = (*CommitInfo)(nil)

// CommitInfo is a struct that represents the commit info of a delta table.
type CommitInfo map[string]interface{}

func (c *CommitInfo) Name() string {
	return "commitInfo"
}

// MarshalJSON is a custom JSON marshaler for CommitInfo.
// The data wrapped in the key "commitInfo".
func (c *CommitInfo) MarshalJSON() ([]byte, error) {
	type Alias CommitInfo // prevent recursion
	return json.Marshal(map[string]interface{}{
		"commitInfo": (*Alias)(c),
	})
}

// UnmarshalJSON is a custom JSON unmarshaler for CommitInfo.
func (c *CommitInfo) UnmarshalJSON(data []byte) error {
	type Alias CommitInfo // prevent recursion
	var wrapper struct {
		CommitInfo *Alias `json:"commitInfo"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	*c = CommitInfo(*wrapper.CommitInfo)
	return nil
}

// UnmarshalParquet is a custom Parquet unmarshaler for CommitInfo.
func (c *CommitInfo) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	// TODO: how to read the commitInfo map?
	*c = CommitInfo{}
	return nil
}
