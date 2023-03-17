package deltalake

import (
	"fmt"
)

const (
	LogDirName             = "_delta_log"
	LastCheckpointFileName = "_last_checkpoint"
)

// formatVersion formats the version as a string.
// The version is left padded with 0s to 20 digits.
func formatVersion(version int64) string {
	return fmt.Sprintf("%020d", version)
}

// formatPart formats the part as a string.
// The part is left padded with 0s to 10 digits.
func formatPart(part int) string {
	return fmt.Sprintf("%010d", part)
}

// CommitURIFromVersion returns the commit URI for the given version.
// The commit URI is the path to the commit file in the delta log,
// relative to the root of the table.
//
//	uri := CommitURIFromVersion(0)
//	fmt.Println(uri) // _delta_log/00000000000000000000.json
func CommitURIFromVersion(version int64) string {
	return fmt.Sprintf("%s/%s.json", LogDirName, formatVersion(version))
}
