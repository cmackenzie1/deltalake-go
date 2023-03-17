package deltalake

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path"

	"github.com/rs/zerolog/log"

	"deltalake/actions"
	"deltalake/storage"
)

type TableConfig struct {
	RequireTombstones bool
	RequireFiles      bool
}

var DefaultTableConfig = TableConfig{
	RequireTombstones: true,
	RequireFiles:      true,
}

// Table is a struct that represents a delta table.
type Table struct {
	State             *TableState
	Config            *TableConfig
	Storage           storage.ObjectStorage
	LastCheckpoint    *Checkpoint
	VersionTimestamps map[int64]int64
}

func NewTable(storage storage.ObjectStorage, config *TableConfig) *Table {
	if config == nil {
		config = &DefaultTableConfig
	}
	return &Table{
		State: &TableState{
			Version: -1,
		},
		Storage:           storage,
		Config:            config,
		VersionTimestamps: make(map[int64]int64),
	}
}

func NewTableWithState(storage storage.ObjectStorage, config *TableConfig, state *TableState) *Table {
	if config == nil {
		config = &DefaultTableConfig
	}
	if state == nil {
		state = &TableState{
			Version: -1,
		}
	}
	return &Table{
		State:             state,
		Storage:           storage,
		Config:            config,
		VersionTimestamps: make(map[int64]int64),
	}
}

func (t *Table) TableURI() string {
	panic("implement me")
}

func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func LoadTable(storage storage.ObjectStorage, config *TableConfig) (*Table, error) {
	table := NewTable(storage, config)
	err := table.load()
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (t *Table) load() error {
	t.LastCheckpoint = nil
	t.State = NewTableState(WithVersion(-1))
	return t.update()
}

func (t *Table) update() error {
	checkpoint, err := t.MostRecentCheckpoint()
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) { // no checkpoint found, load from beginning
			log.Debug().Msg("no checkpoint found, loading from beginning")
			return t.updateIncremental(-1)
		}
		return err
	}

	log.Debug().
		Int64("checkpointVersion", checkpoint.Version).
		Msg("updating from checkpoint")

	// If table is already on the most recent checkpoint, check if there are any new commits to load.
	if checkpoint.Version == t.State.Version {
		log.Debug().
			Int64("checkpointVersion", checkpoint.Version).
			Msg("table is already on the most recent checkpoint, checking for new commits")
		return t.updateIncremental(-1)
	}

	// If the table is not on the most recent checkpoint, load the table state from the checkpoint instead of incrementally.
	// This leap-ahead is possible because the checkpoint contains the full state of the table.
	t.LastCheckpoint = checkpoint
	if err := t.loadCheckpoint(checkpoint); err != nil {
		return err
	}
	return t.updateIncremental(checkpoint.Version)
}

// updateIncremental updates the table state incrementally
// if maxVersion is -1, it will update all versions since the last version
// It is assumed that the table state is already updated to `t.State.Version`
func (t *Table) updateIncremental(maxVersion int64) error {
	log.Debug().
		Int64("currentVersion", t.State.Version).
		Int64("targetVersion", maxVersion).Msg("incremental update")

	// https://github.com/delta-io/delta-rs/blob/main/rust/src/delta.rs#L766
	for {
		acts, err := t.peakNextCommit(t.State.Version)
		if err != nil {
			return err
		}
		if len(acts) == 0 { // no more commits to load
			break
		}
		if maxVersion != -1 && t.State.Version >= maxVersion { // reached max version
			break
		}

		newState, err := NewTableStateFromActions(acts, WithVersion(t.State.Version+1))
		if err != nil {
			return err
		}
		t.State.Merge(newState, t.Config.RequireFiles, t.Config.RequireTombstones)
	}

	if t.State.Version == -1 { // after loading, if the table is still empty, return an error
		log.Debug().
			Str("table_uri", t.TableURI()).
			Msg("no commits found")
		return errors.New("no commits found")
	}

	return nil
}

func (t *Table) loadTimestamp(timestamp int64) error {
	panic("implement me")
}

// peakNextCommit returns a list of actions that will update the table state to the next version.
// If the next version is not found, an empty list is returned with no error indicating that the table is up to date.
func (t *Table) peakNextCommit(currentVersion int64) ([]actions.Action, error) {
	acts := make([]actions.Action, 0)
	uri := CommitURIFromVersion(currentVersion + 1)
	oplog, err := t.Storage.Get(uri)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			log.Debug().
				Str("uri", uri).
				Int("latest_version", int(currentVersion)).
				Msg("no more commits, table is up to date")
			return acts, nil
		}
		return nil, err
	}

	log.Debug().
		Str("uri", uri).
		Int("version", int(currentVersion+1)).
		Msg("loading commit")
	scanner := bufio.NewScanner(oplog)
	for scanner.Scan() {
		action, err := actions.ParseActionJSON(copyBytes(scanner.Bytes()))
		if err != nil {
			return nil, err
		}
		acts = append(acts, action)
	}

	return acts, nil
}

func (t *Table) MostRecentCheckpoint() (*Checkpoint, error) {
	uri := path.Join(LogDirName, LastCheckpointFileName)
	log.Debug().Str("uri", uri).Msg("loading checkpoint")
	check, err := t.Storage.Get(uri)
	if err != nil {
		return nil, err
	}
	defer check.Close()

	data := &bytes.Buffer{}
	if _, n := io.Copy(data, check); n != nil {
		return nil, n
	}

	return ParseCheckpoint(data.Bytes())
}

func ParseCheckpoint(data []byte) (*Checkpoint, error) {
	var checkpoint Checkpoint
	err := json.Unmarshal(data, &checkpoint)
	if err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func (t *Table) loadCheckpoint(checkpoint *Checkpoint) error {
	state, err := NewTableStateFromCheckpoint(t, checkpoint)
	if err != nil {
		return err
	}
	t.State = state
	return nil
}
