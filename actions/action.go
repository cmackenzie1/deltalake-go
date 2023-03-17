package actions

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/segmentio/parquet-go"
)

const AddAction = "add"
const CDCAction = "cdc"
const CommitInfoAction = "commitInfo"
const MetadataAction = "metaData"
const ProtocolAction = "protocol"
const RemoveAction = "remove"
const TransactionAction = "txn"

type Action interface {
	Name() string
}

func SerializeActionJSON(a Action) ([]byte, error) {
	return json.Marshal(map[string]any{
		a.Name(): a,
	})
}

func decodePath(path string) (string, error) {
	return url.QueryUnescape(path)
}

// ParseActionJSON parses a JSON-encoded action and returns the action.
// TODO: determine if we should keep this or change each actions UnmarshalJSON to only use nested object
func ParseActionJSON(data []byte) (Action, error) {
	a := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, err
	}

	for k, v := range a { // v is a json.RawMessage and must be unmarshaled to the correct type
		switch k {
		case AddAction:
			var add Add
			if err := json.Unmarshal(v, &add); err != nil {
				return nil, err
			}
			return &add, nil
		case CDCAction:
			var cdc CDC
			if err := json.Unmarshal(v, &cdc); err != nil {
				return nil, err
			}
			return &cdc, nil
		case CommitInfoAction:
			var commitInfo CommitInfo
			if err := json.Unmarshal(v, &commitInfo); err != nil {
				return nil, err
			}
			return &commitInfo, nil
		case MetadataAction:
			var metaData Metadata
			if err := json.Unmarshal(v, &metaData); err != nil {
				return nil, err
			}
			return &metaData, nil
		case ProtocolAction:
			var protocol Protocol
			if err := json.Unmarshal(v, &protocol); err != nil {
				return nil, err
			}
			return &protocol, nil
		case RemoveAction:
			var remove Remove
			if err := json.Unmarshal(v, &remove); err != nil {
				return nil, err
			}
			return &remove, nil
		case TransactionAction:
			var txn Transaction
			if err := json.Unmarshal(v, &txn); err != nil {
				return nil, err
			}
			return &txn, nil
		default:
			return nil, fmt.Errorf("unknown action: %s", k)
		}
	}
	return nil, fmt.Errorf("no valid action found")
}

func ParseParquetRecord(schema *parquet.Schema, row parquet.Row) (Action, error) {
	columns := schema.Columns()
	values := []parquet.Value(row)

	start := firstNonNull(values)
	if start == -1 {
		return nil, fmt.Errorf("no non-null values found")
	}
	path := columns[start] // looks like ["add", "path"] or ["remove", "path"]

	switch path[0] {
	case AddAction:
		var add Add
		if err := add.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &add, nil
	case CDCAction:
		var cdc CDC
		if err := cdc.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &cdc, nil
	case CommitInfoAction:
		var commitInfo CommitInfo
		if err := commitInfo.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &commitInfo, nil
	case MetadataAction:
		var metaData Metadata
		if err := metaData.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &metaData, nil
	case ProtocolAction:
		var protocol Protocol
		if err := protocol.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &protocol, nil
	case RemoveAction:
		var remove Remove
		if err := remove.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &remove, nil
	case TransactionAction:
		var txn Transaction
		if err := txn.UnmarshalParquet(schema, row); err != nil {
			return nil, err
		}
		return &txn, nil
	}

	return nil, nil
}

func firstNonNull(values []parquet.Value) int {
	for _, v := range values {
		if !v.IsNull() {
			return v.Column()
		}
	}
	return -1
}
