package actions

import (
	"encoding/json"
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

// MarshalJSON marshals the transaction action to JSON.
// The data wrapped in the key "txn".
func (t *Transaction) MarshalJSON() ([]byte, error) {
	type Alias Transaction // prevent recursion
	return json.Marshal(map[string]interface{}{
		"txn": (*Alias)(t),
	})
}

// UnmarshalJSON unmarshals the transaction action from JSON.
func (t *Transaction) UnmarshalJSON(data []byte) error {
	type Alias Transaction // prevent recursion
	var w struct {
		Transaction *Alias `json:"txn"`
	}
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*t = Transaction(*w.Transaction)
	return nil
}
