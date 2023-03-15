package actions

import "net/url"

type Action interface {
	Name() string
}

func decodePath(path string) (string, error) {
	return url.QueryUnescape(path)
}
