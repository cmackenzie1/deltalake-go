package actions

import (
	"github.com/segmentio/parquet-go"
)

var _ Action = (*CommitInfo)(nil)

// CommitInfo is a struct that represents the commit info of a delta table.
type CommitInfo map[string]interface{}

func (c *CommitInfo) Name() string {
	return "commitInfo"
}

// UnmarshalParquet is a custom Parquet unmarshaler for CommitInfo.
func (c *CommitInfo) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	// TODO: how to read the commitInfo map?
	*c = CommitInfo{}
	return nil
}
