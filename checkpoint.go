package deltalake

type Checkpoint struct {
	// Version is the version of the delta table.
	// When formatted as a string, it is left padded with 0s to 20 digits.
	Version int64
	// Size in bytes of the files in the version.
	Size int64
	// When formatted as a string, it is left padded with 0s to 10 digits.
	Parts int
}
