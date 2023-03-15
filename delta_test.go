package deltalake

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitURIFromVersion(t *testing.T) {
	tests := map[string]struct {
		version int64
		want    string
	}{
		"0": {0, "_delta_log/00000000000000000000.json"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := CommitURIFromVersion(test.version); got != test.want {
				t.Errorf("CommitURIFromVersion() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_formatPart(t *testing.T) {
	tests := map[string]struct {
		part int
		want string
	}{
		"0":         {0, "0000000000"},
		"MAX_INT32": {math.MaxInt32, "2147483647"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := formatPart(test.part)
			require.Equal(t, test.want, got)
		})
	}
}

func Test_formatVersion(t *testing.T) {
	tests := map[string]struct {
		version int64
		want    string
	}{
		"0":         {0, "00000000000000000000"},
		"MAX_INT64": {math.MaxInt64, "09223372036854775807"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := formatVersion(test.version)
			require.Equal(t, test.want, got)
		})
	}
}
