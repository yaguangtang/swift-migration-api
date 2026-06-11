package proxy

import (
	"errors"
	"strings"
)

type ResourceKind string

const (
	ResourceAccount   ResourceKind = "account"
	ResourceContainer ResourceKind = "container"
	ResourceObject    ResourceKind = "object"
)

var ErrInvalidSwiftPath = errors.New("invalid swift path")

type SwiftPath struct {
	Account     string
	Container   string
	Object      string
	Kind        ResourceKind
	BackendPath string
}

func ParseSwiftPath(rawPath string) (SwiftPath, error) {
	if !strings.HasPrefix(rawPath, "/v1/") {
		return SwiftPath{}, ErrInvalidSwiftPath
	}

	suffix := strings.TrimPrefix(rawPath, "/v1/")
	suffix = strings.TrimPrefix(suffix, "/")
	if suffix == "" {
		return SwiftPath{}, ErrInvalidSwiftPath
	}

	parts := strings.SplitN(suffix, "/", 3)
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return SwiftPath{}, ErrInvalidSwiftPath
		}
		return SwiftPath{
			Account:     parts[0],
			Kind:        ResourceAccount,
			BackendPath: "/" + suffix,
		}, nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return SwiftPath{}, ErrInvalidSwiftPath
		}
		return SwiftPath{
			Account:     parts[0],
			Container:   parts[1],
			Kind:        ResourceContainer,
			BackendPath: "/" + suffix,
		}, nil
	default:
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return SwiftPath{}, ErrInvalidSwiftPath
		}
		return SwiftPath{
			Account:     parts[0],
			Container:   parts[1],
			Object:      parts[2],
			Kind:        ResourceObject,
			BackendPath: "/" + suffix,
		}, nil
	}
}
