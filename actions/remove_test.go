package actions

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRemove_MarshalUnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		remove   *Remove
		wantJSON string
	}{
		"empty": {
			remove:   NewRemove("", 0, false, false, nil, 0, nil),
			wantJSON: `{"remove":{"path":"","dataChange":false}}`,
		},
		"full": {
			//Example from https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
			remove:   NewRemove("part-00001-9.snappy.parquet", 1515488792485, true, false, nil, 0, nil),
			wantJSON: `{"remove":{"path":"part-00001-9.snappy.parquet","deletionTimestamp":1515488792485,"dataChange":true}}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			gotJSON, err := test.remove.MarshalJSON()
			require.NoErrorf(t, err, "Remove.MarshalJSON() failed with error: %v", err)
			require.JSONEq(t, test.wantJSON, string(gotJSON))
			t.Logf("Remove.MarshalJSON() = %s", string(gotJSON))

			var gotRemove Remove
			err = json.Unmarshal(gotJSON, &gotRemove)
			require.NoErrorf(t, err, "Remove.UnmarshalJSON() failed with error: %v", err)
			require.Equal(t, test.remove, &gotRemove)
			t.Logf("Remove.UnmarshalJSON() = %v", gotRemove)
		})
	}
}
