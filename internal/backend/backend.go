package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"swift-migration-api/internal/config"
)

type Client interface {
	Name() string
	Do(ctx context.Context, req Request) (*http.Response, error)
}

type Request struct {
	Method        string
	Path          string
	Query         url.Values
	Headers       http.Header
	Body          io.Reader
	ContentLength int64
}

type HTTPClient struct {
	name    string
	baseURL string
	client  *http.Client
}

func New(name string, cfg config.BackendConfig) (*HTTPClient, error) {
	if _, err := url.Parse(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("parse %s backend url: %w", name, err)
	}
	return &HTTPClient{
		name:    name,
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		client: &http.Client{
			Timeout: cfg.Timeout.Std(),
		},
	}, nil
}

func (c *HTTPClient) Name() string {
	return c.name
}

func (c *HTTPClient) Do(ctx context.Context, req Request) (*http.Response, error) {
	target := c.baseURL + ensureLeadingSlash(req.Path)
	if len(req.Query) > 0 {
		target += "?" + req.Query.Encode()
	}

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, target, req.Body)
	if err != nil {
		return nil, fmt.Errorf("build %s request to %s: %w", req.Method, c.name, err)
	}
	if req.Headers != nil {
		httpReq.Header = req.Headers.Clone()
	}
	if req.ContentLength >= 0 {
		httpReq.ContentLength = req.ContentLength
	}
	return c.client.Do(httpReq)
}

func BuildObjectPath(account, container, object string) string {
	segs := []string{escapePathSegment(account), escapePathSegment(container)}
	if object != "" {
		for _, piece := range strings.Split(object, "/") {
			segs = append(segs, escapePathSegment(piece))
		}
	}
	return "/" + strings.Join(segs, "/")
}

func BuildContainerPath(account, container string) string {
	return "/" + strings.Join([]string{escapePathSegment(account), escapePathSegment(container)}, "/")
}

func BuildAccountPath(account string) string {
	return "/" + escapePathSegment(account)
}

func ensureLeadingSlash(in string) string {
	if in == "" {
		return "/"
	}
	if strings.HasPrefix(in, "/") {
		return in
	}
	return "/" + in
}

func escapePathSegment(in string) string {
	escaped := url.PathEscape(in)
	return strings.ReplaceAll(escaped, "+", "%20")
}

func CloneQuery(in url.Values) url.Values {
	out := make(url.Values, len(in))
	for key, values := range in {
		for _, value := range values {
			out.Add(key, value)
		}
	}
	return out
}

func IsSuccess(status int) bool {
	return status >= 200 && status < 300
}

func NormalizeETag(in string) string {
	return strings.Trim(strings.TrimSpace(in), "\"")
}

func SleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func JoinURLPath(base string, elems ...string) string {
	all := []string{strings.TrimSuffix(base, "/")}
	for _, elem := range elems {
		all = append(all, strings.TrimPrefix(elem, "/"))
	}
	return path.Clean(strings.Join(all, "/"))
}
