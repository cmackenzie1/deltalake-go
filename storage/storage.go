package storage

import (
	"io"
	"time"
)

type ObjectInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
}

// IsZero returns true if the object info is zero.
func (o ObjectInfo) IsZero() bool {
	return o == ObjectInfo{}
}

type ObjectStorage interface {
	// Put writes the data to the given path.
	Put(path string, data io.Reader) error
	// Get returns a reader for the given path.
	Get(path string) (io.ReadCloser, error)
	// Head returns the object info for the given path.
	Head(path string) (ObjectInfo, error)
	// Delete deletes the object at the given path.
	Delete(path string) error
	// List returns a list of objects with the given prefix.
	List(prefix string) ([]ObjectInfo, error)
	// RootURI returns the root URI of the storage.
	RootURI() string
}
