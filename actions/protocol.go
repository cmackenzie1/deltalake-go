package actions

import (
	"encoding/json"
	"fmt"

	"github.com/segmentio/parquet-go"
)

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

// UnmarshalJSON unmarshals the protocol action from JSON.
func (p *Protocol) UnmarshalJSON(data []byte) error {
	type Alias Protocol // prevent recursion
	return json.Unmarshal(data, (*Alias)(p))
}

func (p *Protocol) UnmarshalParquet(schema *parquet.Schema, row parquet.Row) error {
	minReaderVersion, ok := schema.Lookup("protocol", "minReaderVersion")
	if !ok {
		return fmt.Errorf("could not find minReaderVersion in schema")
	}

	minWriterVersion, ok := schema.Lookup("protocol", "minWriterVersion")
	if !ok {
		return fmt.Errorf("could not find minWriterVersion in schema")
	}

	p.MinReaderVersion = int(row[minReaderVersion.ColumnIndex].Int32())
	p.MinWriterVersion = int(row[minWriterVersion.ColumnIndex].Int32())
	p.ReaderFeatures = make([]string, 0)
	p.WriterFeatures = make([]string, 0)

	return nil
}
