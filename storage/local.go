package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
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
	log.Debug().Str("rootDir", rootDir).Msg("opened local storage")
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

	log.Debug().Str("path", path).Msg("put file")
	return nil
}

// exists returns true if the path exists.
// it expects the path to be absolute from l.fullpath()
func (l *LocalStorage) exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (l *LocalStorage) Get(path string) (io.ReadCloser, error) {
	path = l.fullpath(path)
	if !l.exists(path) {
		log.Debug().
			Str("path", path).
			Str("action", "get").
			Msg("file not found")

		return nil, ErrNotFound
	}
	log.Debug().Str("path", path).Msg("get file")
	return os.Open(path)
}

func (l *LocalStorage) Head(path string) (ObjectInfo, error) {
	path = l.fullpath(path)
	if !l.exists(path) {
		log.Debug().
			Str("path", path).
			Str("action", "head").
			Msg("file not found")
		return ObjectInfo{}, ErrNotFound
	}

	info, err := os.Stat(path)
	if err != nil {
		return ObjectInfo{}, err
	}

	log.Debug().Str("path", path).Msg("head file")
	return ObjectInfo{
		Path:         strings.TrimPrefix(path, l.rootDir),
		Size:         info.Size(),
		LastModified: info.ModTime(),
	}, nil

}

func (l *LocalStorage) Delete(path string) error {
	log.Debug().Str("path", path).Msg("delete file")
	return os.Remove(l.fullpath(path))
}

func (l *LocalStorage) List(prefix string) ([]ObjectInfo, error) {
	// Find all files with the prefix.
	// Read file info for each file.
	var infos []ObjectInfo

	// Find all files with the prefix.
	err := filepath.Walk(l.fullpath(prefix), func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			infos = append(infos, ObjectInfo{
				Path:         strings.TrimPrefix(path, l.rootDir),
				Size:         info.Size(),
				LastModified: info.ModTime(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	log.Debug().Str("prefix", prefix).
		Int("count", len(infos)).
		Msg("list files")
	return infos, nil
}
