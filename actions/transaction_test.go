package actions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransaction_MarshalUnmarshalJSONt(t *testing.T) {
	tests := map[string]struct {
		transaction *Transaction
		wantJSON    string
	}{
		"empty": {
			transaction: NewTransaction("", 0, 0),
			wantJSON:    `{"txn":{"appId":"","version":0}}`,
		},
		"full": {
			// Example from https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers
			transaction: NewTransaction("3ba13872-2d47-4e17-86a0-21afd2a22395", 364475, 0),
			wantJSON:    `{"txn":{"appId":"3ba13872-2d47-4e17-86a0-21afd2a22395","version":364475}}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			gotJSON, err := test.transaction.MarshalJSON()
			require.NoErrorf(t, err, "Transaction.MarshalJSON() failed with error: %v", err)
			require.JSONEq(t, test.wantJSON, string(gotJSON))
			t.Logf("Transaction.MarshalJSON() = %s", gotJSON)

			var gotTransaction Transaction
			err = json.Unmarshal(gotJSON, &gotTransaction)
			require.NoErrorf(t, err, "Transaction.UnmarshalJSON() failed with error: %v", err)
			require.Equal(t, test.transaction, &gotTransaction)
			t.Logf("Transaction.UnmarshalJSON() = %v", gotTransaction)
		})
	}
}
