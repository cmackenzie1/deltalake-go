package delta

import (
	"fmt"
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
