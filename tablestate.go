package deltalake

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/parquet-go"

	"deltalake/actions"
	"deltalake/types"
)

// TableState is the state of a delta table.
type TableState struct {
	Version                  int64
	Files                    []*actions.Add
	Tombstones               map[string]*actions.Remove // used for fast lookup by path
	CommitInfos              []*actions.CommitInfo
	MinReaderVersion         int
	MinWriterVersion         int
	CurrentMetadata          *TableMetadata
	TombstoneRetentionMillis int64
	LogRetentionMillis       int64
	AppTransactionVersion    map[string]int64 // appId -> version
}

func NewTableState(options ...TableStateOption) *TableState {
	state := &TableState{
		Version:               -1,
		Files:                 make([]*actions.Add, 0),
		Tombstones:            make(map[string]*actions.Remove),
		CommitInfos:           make([]*actions.CommitInfo, 0),
		MinReaderVersion:      1,
		MinWriterVersion:      1,
		CurrentMetadata:       nil,
		AppTransactionVersion: make(map[string]int64),
	}
	for _, option := range options {
		option(state)
	}
	return state
}

func NewTableStateFromActions(actions []actions.Action, options ...TableStateOption) (*TableState, error) {
	state := NewTableState(options...)
	for _, action := range actions {
		if err := state.DoAction(action, true, true); err != nil {
			return nil, err
		}
	}
	return state, nil
}

type TableStateOption func(*TableState)

func WithVersion(version int64) TableStateOption {
	return func(s *TableState) {
		s.Version = version
	}
}

func WithMinReaderVersion(version int) TableStateOption {
	return func(s *TableState) {
		s.MinReaderVersion = version
	}
}

func WithMinWriterVersion(version int) TableStateOption {
	return func(s *TableState) {
		s.MinWriterVersion = version
	}
}

// DoAction applies an action to the table state.
// https://github.com/delta-io/delta-rs/blob/main/rust/src/table_state.rs#L316
func (s *TableState) DoAction(action actions.Action, requireFiles bool, requireTombstones bool) error {
	if action == nil {
		return errors.New("action is nil")
	}
	log.Debug().
		Str("action", action.Name()).
		Msgf("applying action to table state (version: %d)", s.Version)
	switch a := action.(type) {
	case *actions.Add:
		if requireFiles {
			s.Files = append(s.Files, a)
		}
	case *actions.CDC: // TODO: optionally support CDC
	case *actions.CommitInfo:
		s.CommitInfos = append(s.CommitInfos, a)
	case *actions.Protocol:
		s.MinReaderVersion = a.MinReaderVersion
		s.MinWriterVersion = a.MinWriterVersion
	case *actions.Metadata:
		var schema types.StructType
		if err := json.Unmarshal([]byte(a.SchemaString), &schema); err != nil {
			return err
		}
		md := &TableMetadata{
			ID:               a.ID,
			Name:             a.TableName,
			Description:      a.Description,
			Format:           a.Format,
			Schema:           schema,
			PartitionColumns: a.PartitionColumns,
			CreatedTime:      a.CreatedTime,
			Configuration:    a.Configuration,
		}
		s.TombstoneRetentionMillis = md.TombstoneRetentionMillis()
		s.LogRetentionMillis = md.LogRetentionMillis()
		s.CurrentMetadata = md
	case *actions.Remove:
		if requireTombstones {
			s.Tombstones[a.Path] = a
		}
	case *actions.Transaction:
		// if not in the map, add it, otherwise update it if the version is larger
		if _, ok := s.AppTransactionVersion[a.AppID]; !ok {
			s.AppTransactionVersion[a.AppID] = a.Version
		} else if a.Version > s.AppTransactionVersion[a.AppID] {
			s.AppTransactionVersion[a.AppID] = a.Version
		}
	default:
		return fmt.Errorf("unknown action type: %T", action)
	}
	return nil
}

// Merge merges the state of another table state into this one.
// TableState also carries the version information for the given state,
// as there is a one-to-one match between a table state and a version. In merge/update
// scenarios we cannot infer the intended / correct version number. By default, this
// function will update the tracked version if the version on `new_state` is larger than the
// currently set version however it is up to the caller to update the `version` field according
// to the version the merged state represents.
// https://github.com/delta-io/delta-rs/blob/main/rust/src/table_state.rs#L257
func (s *TableState) Merge(other *TableState, requireFiles bool, requireTombstones bool) {
	// If there are tombstones in the other state, we need to remove the relevant files from the current state.
	if len(other.Tombstones) > 0 {
		files := make([]*actions.Add, 0)
		for _, file := range s.Files {
			if _, ok := other.Tombstones[file.Path]; !ok {
				files = append(files, file)
			}
		}
		s.Files = files
	}

	if requireTombstones && requireFiles {
		for _, tombstone := range other.Tombstones {
			s.Tombstones[tombstone.Path] = tombstone
		}

		for _, file := range other.Files {
			delete(s.Tombstones, file.Path)
		}
	}

	if requireFiles {
		s.Files = append(s.Files, other.Files...)
	}

	if other.MinReaderVersion > 0 {
		s.MinReaderVersion = other.MinReaderVersion
		s.MinWriterVersion = other.MinWriterVersion
	}

	if other.CurrentMetadata != nil {
		s.TombstoneRetentionMillis = other.TombstoneRetentionMillis
		s.LogRetentionMillis = other.LogRetentionMillis
		s.CurrentMetadata = other.CurrentMetadata
	}

	for appID, version := range other.AppTransactionVersion {
		// if not in the map, add it, otherwise update it if the version is larger
		if _, ok := s.AppTransactionVersion[appID]; !ok {
			s.AppTransactionVersion[appID] = version
		} else if version > s.AppTransactionVersion[appID] {
			s.AppTransactionVersion[appID] = version
		}
	}

	if other.CommitInfos != nil {
		s.CommitInfos = append(s.CommitInfos, other.CommitInfos...)
	}

	if s.Version < other.Version {
		s.Version = other.Version
	}
}

func NewTableStateFromCheckpoint(table *Table, checkpoint *Checkpoint) (*TableState, error) {
	checkpointPaths := ListCheckpointParts(checkpoint)
	log.Debug().
		Strs("paths", checkpointPaths).
		Int("num_parts", len(checkpointPaths)).Msg("loading checkpoint")
	state := NewTableState(WithVersion(checkpoint.Version))
	for _, path := range checkpointPaths {
		obj, err := table.Storage.Get(path)
		if err != nil {
			return nil, err
		}
		data := &bytes.Buffer{}
		if _, err := io.Copy(data, obj); err != nil {
			return nil, err
		}
		obj.Close()

		if err := state.ParseCheckpointBytes(data.Bytes()); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (s *TableState) ParseCheckpointBytes(data []byte) error {
	reader := parquet.NewGenericReader[any](bytes.NewReader(data))
	defer reader.Close()

	schema := reader.Schema()

	rows := make([]parquet.Row, reader.NumRows()) // TODO: Read in batches instead of all at once? Do using for (while) loop and fixed batch size
	n, err := reader.ReadRows(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if int64(n) != reader.NumRows() {
		return fmt.Errorf("expected %d rows, got %d", reader.NumRows(), n)
	}

	acts := make([]actions.Action, 0, len(rows))
	for _, row := range rows {
		action, err := actions.ParseParquetRecord(schema, row)
		if err != nil {
			return err
		}
		acts = append(acts, action)
	}

	for _, action := range acts {
		if err := s.DoAction(action, true, true); err != nil {
			return err
		}
	}

	return nil
}
