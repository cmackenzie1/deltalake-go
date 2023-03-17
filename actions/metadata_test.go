package actions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadata_MarshalUnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		metadata *Metadata
		wantJSON string
	}{
		"empty": {
			metadata: NewMetadata("", "", "", DefaultFormat, "", nil, 0, nil),
			wantJSON: `{"id":"","format":{"provider":"parquet","options":{}},"schemaString":"","partitionColumns":[],"configuration":{}}`,
		},
		"full": {
			// Test case from https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
			metadata: NewMetadata("af23c9d7-fff1-4a5a-a2c8-55c59bd782aa", "", "", DefaultFormat, "...", nil, 0, map[string]string{"appendOnly": "true"}),
			wantJSON: `{"id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa","format":{"provider":"parquet","options":{}},"schemaString":"...","partitionColumns":[],"configuration":{"appendOnly":"true"}}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			gotJSON, err := json.Marshal(test.metadata)
			require.NoErrorf(t, err, "json.Marshal() failed with error: %v", err)
			require.JSONEq(t, test.wantJSON, string(gotJSON))
			t.Logf("Metadata.MarshalJSON() = %s", string(gotJSON))

			var gotMetadata Metadata
			err = json.Unmarshal(gotJSON, &gotMetadata)
			require.NoErrorf(t, err, "json.Unmarshal() failed with error: %v", err)
			require.Equal(t, test.metadata, &gotMetadata)
			t.Logf("Metadata.UnmarshalJSON() = %v", gotMetadata)
		})
	}
}
