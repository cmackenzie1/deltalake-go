package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWhichStorageProvider(t *testing.T) {
	tests := map[string]struct {
		uri     string
		want    string
		wantErr bool
	}{
		"empty": {uri: "", want: "", wantErr: true},
		"file":  {uri: "file:///tmp", want: "file", wantErr: false},
		"s3":    {uri: "s3://bucket/path", want: "s3", wantErr: false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := WhichStorageProvider(test.uri)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoErrorf(t, err, "WhichStorageProvider() failed with error: %v", err)
				require.EqualValues(t, test.want, got)
			}
		})
	}
}
