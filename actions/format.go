package actions

type Format struct {
	// Provider is the Name_ of the format provider.
	Provider string `json:"provider"`
	// Options is a map of format option key to value.
	Options map[string]string `json:"options"`
}

var DefaultFormat = Format{
	Provider: "parquet",
	Options:  map[string]string{},
}
