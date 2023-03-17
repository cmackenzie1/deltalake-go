package actions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCDC_MarshalUnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		cdc      *CDC
		wantJSON string
	}{
		"empty": {
			cdc:      NewCDC("", nil, 0, false, nil),
			wantJSON: `{"path":"","dataChange":false,"partitionValues":{},"size":0}`,
		},
		"full": {
			// Example from https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file
			cdc:      NewCDC("_change_data/cdc-00001-c.snappy.parquet", map[string]string{}, 1213, false, nil),
			wantJSON: `{"path":"_change_data/cdc-00001-c.snappy.parquet","partitionValues":{},"size":1213,"dataChange":false}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			gotJSON, err := json.Marshal(test.cdc)
			require.NoErrorf(t, err, "CDC.MarshalJSON() failed with error: %v", err)
			require.JSONEq(t, test.wantJSON, string(gotJSON))
			t.Logf("CDC.MarshalJSON() = %s", string(gotJSON))

			var gotCDC CDC
			err = json.Unmarshal(gotJSON, &gotCDC)
			require.NoErrorf(t, err, "CDC.UnmarshalJSON() failed with error: %v", err)
			require.EqualValues(t, test.cdc, &gotCDC)
			t.Logf("CDC.UnmarshalJSON() = %v", gotCDC)
		})
	}
}
