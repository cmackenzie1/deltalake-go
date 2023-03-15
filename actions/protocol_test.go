package actions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocol_MarshalUnmarshalJSON(t *testing.T) {
	tests := map[string]struct {
		protocol *Protocol
		wantJSON string
	}{
		"empty": {
			protocol: NewProtocol(0, 0, nil, nil),
			wantJSON: `{"protocol":{"minReaderVersion":0,"minWriterVersion":0}}`,
		},
		"full": {
			protocol: NewProtocol(1, 2, nil, nil),
			wantJSON: `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}`,
		},
		"writerFeatures": {
			protocol: NewProtocol(2, 7, nil, []string{"columnMapping", "identityColumns"}),
			wantJSON: `{"protocol":{"minReaderVersion":2,"minWriterVersion":7,"writerFeatures":["columnMapping","identityColumns"]}}`,
		},
		"readerWriterFeatures": {
			protocol: NewProtocol(3, 7, []string{"columnMapping"}, []string{"columnMapping", "identityColumns"}),
			wantJSON: `{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping","identityColumns"]}}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := test.protocol.MarshalJSON()
			require.NoErrorf(t, err, "Transaction.MarshalJSON() error = %v", err)
			require.JSONEq(t, test.wantJSON, string(got), "Transaction.MarshalJSON() = %v, wantJSON %v", string(got), test.wantJSON)
			t.Logf("Transaction.MarshalJSON() = %v", string(got))

			var gotProtocol Protocol
			err = json.Unmarshal(got, &gotProtocol)
			require.NoErrorf(t, err, "Transaction.UnmarshalJSON() error = %v", err)
			require.Equal(t, test.protocol, &gotProtocol, "Transaction.UnmarshalJSON() = %v, want %v", gotProtocol, test.protocol)
			t.Logf("Transaction.UnmarshalJSON() = %v", gotProtocol)
		})
	}
}
