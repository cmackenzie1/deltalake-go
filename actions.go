package delta

import (
	"net/url"
	"time"
)

type Action string

const (
	// AddAction is the action type for adding a file to the delta table.
	AddAction Action = "add"
	// RemoveAction is the action type for removing a file from the delta table.
	RemoveAction Action = "remove"
	// MetadataAction is the action type for updating the metadata of the delta table.
	MetadataAction Action = "metaData"
	// ProtocolAction is the action type for updating the protocol of the delta table.
	ProtocolAction Action = "protocol"
	// CommitInfoAction is the action type for updating the commit info of the delta table.
	CommitInfoAction Action = "commitInfo"
)

func (a Action) String() string {
	return string(a)
}

func IsValidAction(action string) bool {
	switch action {
	case AddAction.String(), RemoveAction.String(), MetadataAction.String(), ProtocolAction.String(), CommitInfoAction.String():
		return true
	default:
		return false
	}
}

func decodePath(path string) (string, error) {
	return url.QueryUnescape(path)
}

// Add is a struct that represents an add action to a delta table.
type Add struct {
	// Path is the relative path of the file to add from the root of the table.
	Path string `json:"path"`
	// Size is the size of the file in bytes.
	Size int64 `json:"size"`
	// PartitionValues is a map of partition column Name_ to value.
	PartitionValues map[string]string `json:"partitionValues"`
	// DataChange is a boolean that indicates whether the file contains data changes.
	// If false, the data in the file is a result of a compaction or other operation.
	DataChange bool `json:"dataChange"`
	// ModificationTime is the time the file was last modified.
	ModificationTime int64 `json:"modificationTime"`
	// Stats Contains statistics (e.g., count, min/max values for columns) about the data in this logical file
	Stats Stats `json:"stats"`
	// Tags contains additional information about the file.
	Tags map[string]string `json:"tags"`
}

// pathDecoded returns the decoded path of the add action.
func (a *Add) pathDecoded() (string, error) {
	return decodePath(a.Path)
}

// Remove is a struct that represents a remove action to the delta log.
type Remove struct {
	// Path is the relative path of the file to remove from the root of the table.
	Path string `json:"path"`
	// DeletionTimestamp is the time the deletion occurred, represented as milliseconds since the epoch
	DeletionTimestamp time.Time `json:"deletionTimestamp"`
	// DataChange is a boolean that indicates whether the file contains data changes.
	// If false, the data in the file is a result of a compaction or other operation.
	DataChange bool `json:"dataChange"`
	// ExtendedFileMetadata when true the Fields_ partitionValues, size, and tags are present
	ExtendedFileMetadata bool `json:"extendedFileMetadata"`
	// PartitionValues is a map of partition column Name_ to value.
	PartitionValues map[string]string `json:"partitionValues"`
	// Size is the size of the file in bytes.
	Size int64 `json:"size"`
	// Tags contains additional information about the file.
	Tags map[string]string `json:"tags"`
}

func (r *Remove) pathDecoded() (string, error) {
	return decodePath(r.Path)
}

type Stats struct {
}

type Metadata struct {
	// ID is the unique identifier of the table.
	ID string `json:"id"`
	// Name is the Name_ of the table.
	Name string `json:"Name_"`
	// Description is the description of the table.
	Description string `json:"description"`
	// Format is the format of the table. Default is "parquet".
	Format Format `json:"format"`
	// SchemaString is the types of the table in JSON format.
	SchemaString string `json:"schemaString"`
	// PartitionColumns is a ordered list of partition columns.
	PartitionColumns []string `json:"partitionColumns"`
	// CreatedTime is the time the table was created. Milliseconds since epoch.
	CreatedTime int64 `json:"createdTime"`
	// Configuration is a map of configuration key to value.
	Configuration map[string]string `json:"configuration"`
}

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

type Operation string

const (
	// InsertOperation is the operation type for inserting data into the delta table.
	InsertOperation Operation = "INSERT"
)

// CommitInfo is a struct that represents the commit info of a delta table.
type CommitInfo struct {
	// Timestamp is the time the commit was made.
	Timestamp int64 `json:"timestamp,omitempty"`
	// UserID is the user that made the commit.
	UserID string `json:"userID,omitempty"`
	// UserName is the Name_ of the user that made the commit.
	UserName string `json:"userName,omitempty"`
	// Operation is the operation that was performed.
	Operation string `json:"operation,omitempty"`
}
