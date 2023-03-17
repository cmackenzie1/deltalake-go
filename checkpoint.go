package deltalake

import (
	"fmt"
)

type Checkpoint struct {
	// Version is the version of the delta table.
	// When formatted as a string, it is left padded with 0s to 20 digits.
	Version int64
	// Size in bytes of the files in the version.
	Size int64
	// When formatted as a string, it is left padded with 0s to 10 digits.
	Parts int
}

// ListCheckpointParts enumerates the paths of the parts of the checkpoint.
// This does not check if the parts exist in the storage. It only returns the
// paths that should be used.
//
// A checkpoint with a single part has a name like
//
//	"00000000000000000010.checkpoint.parquet"
//
// A checkpoint with multiple parts has a name like
//
//	"00000000000000000010.checkpoint.0000000001.0000000003.parquet"
//	"00000000000000000010.checkpoint.0000000002.0000000003.parquet"
//	"00000000000000000010.checkpoint.0000000003.0000000003.parquet"
func ListCheckpointParts(checkpoint *Checkpoint) []string {
	var parts []string
	if checkpoint.Parts == 1 || checkpoint.Parts == 0 {
		parts = []string{checkpointPath(checkpoint.Version)}
	} else {
		for i := 1; i <= checkpoint.Parts; i++ {
			parts = append(parts, checkpointPartPath(checkpoint.Version, i, checkpoint.Parts))
		}
	}
	return parts
}

// checkpointPath returns the path of the checkpoint file.
// This does not check if the file exists in the storage. It only returns the
// path that should be used.
//
//	"_delta_log/00000000000000000010.checkpoint.parquet"
func checkpointPath(version int64) string {
	return fmt.Sprintf("%s/%020d.checkpoint.parquet", LogDirName, version)
}

// checkpointPartPath returns the path of the checkpoint part file.
// This does not check if the file exists in the storage. It only returns the
// path that should be used.
//
//	"_delta_log/00000000000000000010.checkpoint.0000000001.0000000003.parquet"
func checkpointPartPath(version int64, part, parts int) string {
	return fmt.Sprintf("%s/%020d.checkpoint.%010d.%010d.parquet", LogDirName, version, part, parts)
}
