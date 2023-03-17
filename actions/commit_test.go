package actions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitInfo_MarshalUnmarhshalJSON(t *testing.T) {
	tests := map[string]struct {
		commitInfo CommitInfo
		wantJSON   string
	}{
		"empty": {
			commitInfo: CommitInfo(map[string]interface{}{}),
			wantJSON:   `{"commitInfo":{}}`,
		},
		"full": { // Example from https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information
			commitInfo: CommitInfo(map[string]interface{}{
				"timestamp": float64(1515491537026), // Loses type information when unmarshalling into map[string]interface{} as an int64
				"userId":    "100121",
				"userName":  "michael@databricks.com",
				"operation": "INSERT",
				"operationParameters": map[string]interface{}{
					"mode":        "Append",
					"partitionBy": "[]",
				},
				"notebook": map[string]interface{}{
					"notebookId":   "4443029",
					"notebookPath": "Users/michael@databricks.com/actions",
				},
				"clusterId": "1027-202406-pooh991",
			}),
			wantJSON: `{"commitInfo":{"timestamp":1515491537026,"userId":"100121","userName":"michael@databricks.com","operation":"INSERT","operationParameters":{"mode":"Append","partitionBy":"[]"},"notebook":{"notebookId":"4443029","notebookPath":"Users/michael@databricks.com/actions"},"clusterId":"1027-202406-pooh991"}}`,
		},
		"tbd": {
			commitInfo: CommitInfo(map[string]interface{}{
				"timestamp": float64(1587968586154), // Loses type information when unmarshalling into map[string]interface{} as an int64
				"operation": "WRITE",
				"operationParameters": map[string]interface{}{
					"mode":        "ErrorIfExists",
					"partitionBy": "[]",
				},
				"isBlindAppend": true,
			}),
			wantJSON: `{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true}}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := test.commitInfo.MarshalJSON()
			require.NoErrorf(t, err, "CommitInfo.MarshalJSON() error = %v", err)
			require.JSONEq(t, test.wantJSON, string(got), "CommitInfo.MarshalJSON() = %v, wantJSON %v", string(got), test.wantJSON)
			t.Logf("CommitInfo.MarshalJSON() = %v", string(got))

			var gotCommitInfo CommitInfo
			err = json.Unmarshal(got, &gotCommitInfo)
			require.NoErrorf(t, err, "CommitInfo.UnmarshalJSON() error = %v", err)
			require.EqualValues(t, test.commitInfo, gotCommitInfo)
			t.Logf("CommitInfo.UnmarshalJSON() = %v", gotCommitInfo)
		})
	}
}
