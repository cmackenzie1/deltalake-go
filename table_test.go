package deltalake

import (
	"testing"

	"github.com/stretchr/testify/require"

	"deltalake/storage"
)

func TestLoadTable(t *testing.T) {
	tests := map[string]struct {
		path        string
		wantVersion int64
		wantErr     bool
	}{
		"simple":                 {path: "testdata/simple_table", wantVersion: 4},
		"simple with checkpoint": {path: "testdata/simple_table_with_checkpoint", wantVersion: 10},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store, err := storage.NewLocalStorage(test.path)
			require.NoErrorf(t, err, "failed to create local storage at %s", test.path)
			tbl, err := LoadTable(store, nil)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantVersion, tbl.State.Version)
			}
		})
	}
}
