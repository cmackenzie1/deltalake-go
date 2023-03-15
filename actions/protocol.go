package actions

import "encoding/json"

var _ Action = (*Protocol)(nil)

type Protocol struct {
	// MinReaderVersion is the minimum version of the delta reader that can read this table.
	MinReaderVersion int `json:"minReaderVersion"`
	// MinWriterVersion is the minimum version of the delta writer that can write to this table.
	MinWriterVersion int `json:"minWriterVersion"`
	// ReaderFeatures is a collection of features that a client must implement in order to correctly read this table (exist only when minReaderVersion is set to 3)
	ReaderFeatures []string `json:"readerFeatures,omitempty"`
	// WriterFeatures is a collection of features that a client must implement in order to correctly write this table (exist only when minWriterVersion is set to 7)
	WriterFeatures []string `json:"writerFeatures,omitempty"`
}

func NewProtocol(minReaderVersion int, minWriterVersion int, readerFeatures []string, writerFeatures []string) *Protocol {
	return &Protocol{
		MinReaderVersion: minReaderVersion,
		MinWriterVersion: minWriterVersion,
		ReaderFeatures:   readerFeatures,
		WriterFeatures:   writerFeatures,
	}
}

func (p *Protocol) Name() string {
	return "protocol"
}

// MarshalJSON marshals the protocol action to JSON.
// The data wrapped in the key "protocol".
func (p *Protocol) MarshalJSON() ([]byte, error) {
	type Alias Protocol // prevent recursion
	return json.Marshal(map[string]interface{}{
		"protocol": (*Alias)(p),
	})
}

// UnmarshalJSON unmarshals the protocol action from JSON.
func (p *Protocol) UnmarshalJSON(data []byte) error {
	type Alias Protocol // prevent recursion
	var wrapper struct {
		Protocol *Alias `json:"protocol"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	*p = Protocol(*wrapper.Protocol)
	return nil
}
