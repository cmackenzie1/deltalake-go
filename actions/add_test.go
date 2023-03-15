package actions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdd_MarshalUnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		add      *Add
		wantJSON string
	}{
		"empty": {
			add:      NewAdd("", 0, nil, false, 0, nil, nil),
			wantJSON: `{"add":{"path":"","size":0,"partitionValues":{},"dataChange":false,"modificationTime":0}}`,
		},
		"full": {
			add:      NewAdd("date=2017-12-10/part-000...c000.gz.parquet", 841454, map[string]string{"date": "2017-12-10"}, true, 1512909768000, nil, nil),
			wantJSON: `{"add":{"path":"date=2017-12-10/part-000...c000.gz.parquet","partitionValues":{"date":"2017-12-10"},"size":841454,"modificationTime":1512909768000,"dataChange":true}}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			gotJSON, err := json.Marshal(test.add)
			require.NoErrorf(t, err, "Add.MarshalJSON() failed with error: %v", err)
			require.JSONEq(t, test.wantJSON, string(gotJSON))
			t.Logf("Add.MarshalJSON() = %v", string(gotJSON))

			var gotAdd Add
			err = json.Unmarshal(gotJSON, &gotAdd)
			require.NoErrorf(t, err, "Add.UnmarshalJSON() failed with error: %v", err)
			require.EqualValues(t, test.add, &gotAdd)
			t.Logf("Add.UnmarshalJSON() = %v", gotAdd)
		})
	}
}
