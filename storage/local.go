package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var _ ObjectStorage = &LocalStorage{}

type LocalStorage struct {
	rootDir string
}

func (l *LocalStorage) RootURI() string {
	return fmt.Sprintf("file://%s", l.rootDir)
}

func NewLocalStorage(rootDir string) (*LocalStorage, error) {
	rootDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	return &LocalStorage{
		rootDir: rootDir,
	}, nil
}

func (l *LocalStorage) fullpath(path string) string {
	return filepath.Join(l.rootDir, path)
}

func (l *LocalStorage) Put(path string, data io.Reader) error {
	path = l.fullpath(path)

	// Atomically create the file.
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	// Write the data.
	if _, err := io.Copy(f, data); err != nil {
		return err
	}

	// Close the file.
	if err := f.Close(); err != nil {
		return err
	}

	// Rename the file.
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}

	return nil
}

func (l *LocalStorage) Get(path string) (io.ReadCloser, error) {
	path = l.fullpath(path)
	return os.Open(path)
}

func (l *LocalStorage) Head(path string) (ObjectInfo, error) {
	path = l.fullpath(path)
	info, err := os.Stat(path)
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Path:         path,
		Size:         info.Size(),
		LastModified: info.ModTime(),
	}, nil

}

func (l *LocalStorage) Delete(path string) error {
	return os.Remove(l.fullpath(path))
}

func (l *LocalStorage) List(prefix string) ([]ObjectInfo, error) {
	// Find all files with the prefix.
	fis, err := filepath.Glob(l.fullpath(prefix) + "*")
	if err != nil {
		return nil, err
	}

	// Read file info for each file.
	var a []ObjectInfo
	for _, fi := range fis {
		info, err := os.Stat(fi)
		if err != nil {
			return nil, err
		}

		a = append(a, ObjectInfo{
			Path:         fi,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
	}

	return a, nil
}
