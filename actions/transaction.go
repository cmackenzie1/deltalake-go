package actions

import (
	"encoding/json"
	"fmt"

	"github.com/segmentio/parquet-go"
)

var _ Action = (*Protocol)(nil)

type Transaction struct {
	AppID       string `json:"appId"`
	Version     int64  `json:"version"`
	LastUpdated int64  `json:"lastUpdated,omitempty"`
}

func (t *Transaction) Name() string {
	return "txn"
}

func NewTransaction(appID string, version int64, lastUpdated int64) *Transaction {
	return &Transaction{
		AppID:       appID,
		Version:     version,
		LastUpdated: lastUpdated,
	}
}

// UnmarshalJSON unmarshals the transaction action from JSON.
func (t *Transaction) UnmarshalJSON(data []byte) error {
	type Alias Transaction // prevent recursion
	return json.Unmarshal(data, (*Alias)(t))
}

func (t *Transaction) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	appId, ok := schema.Lookup("txn", "appId")
	if !ok {
		return fmt.Errorf("could not find appId in schema")
	}

	version, ok := schema.Lookup("txn", "version")
	if !ok {
		return fmt.Errorf("could not find version in schema")
	}

	lastUpdated, ok := schema.Lookup("txn", "lastUpdated")
	if !ok {
		return fmt.Errorf("could not find lastUpdated in schema")
	}

	t.AppID = row[appId.ColumnIndex].String()
	t.Version = row[version.ColumnIndex].Int64()
	t.LastUpdated = row[lastUpdated.ColumnIndex].Int64()

	return nil
}
