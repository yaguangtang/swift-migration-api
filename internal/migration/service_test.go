package migration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"swift-migration-api/internal/auth"
	"swift-migration-api/internal/backend"
	"swift-migration-api/internal/config"
	"swift-migration-api/internal/store"
)

func TestRejectReasonForStaleFreshTokenJob(t *testing.T) {
	t.Parallel()

	expiredSoon := time.Now().UTC().Add(5 * time.Second)
	svc := &Service{
		cfg:    config.MigrationConfig{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	reject, reason := svc.rejectReason(store.Job{
		ID:                "job-1",
		Type:              store.JobTypeObjectCopy,
		RequireFreshToken: true,
		AuthExpiry:        &expiredSoon,
	})
	if !reject {
		t.Fatal("expected job to be rejected")
	}
	if reason == "" {
		t.Fatal("expected rejection reason")
	}
}

func TestVerifyObjectCopyComparesETag(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg: config.MigrationConfig{VerifyETag: true},
	}

	source := mapHeaders("ETag", `"abc"`, "X-Object-Meta-Foo", "bar")
	target := mapHeaders("ETag", `"abc"`, "X-Object-Meta-Foo", "bar")
	if err := svc.verifyObjectCopy(source, target); err != nil {
		t.Fatalf("verify object copy: %v", err)
	}

	targetMismatch := mapHeaders("ETag", `"def"`, "X-Object-Meta-Foo", "bar")
	if err := svc.verifyObjectCopy(source, targetMismatch); err == nil {
		t.Fatal("expected etag mismatch")
	}
}

func TestEnsureContainerWithSourceAuthUsesCallerHeadersForTargetWrites(t *testing.T) {
	t.Parallel()

	containerPath := backend.BuildContainerPath("AUTH_demo", "images")
	sourceHeaders := mapHeaders("X-Container-Read", ".r:*", "X-Container-Meta-Owner", "demo")
	var cephRequests []backend.Request

	ceph := backendFunc(func(_ context.Context, req backend.Request) (*http.Response, error) {
		cephRequests = append(cephRequests, cloneRequest(req))
		switch req.Method {
		case http.MethodPut:
			return newHTTPResponse(http.StatusCreated, nil, "")
		case http.MethodPost:
			return newHTTPResponse(http.StatusNoContent, nil, "")
		case http.MethodHead:
			return newHTTPResponse(http.StatusNoContent, sourceHeaders, "")
		default:
			t.Fatalf("unexpected ceph method: %s", req.Method)
			return nil, nil
		}
	})

	swift := backendFunc(func(_ context.Context, req backend.Request) (*http.Response, error) {
		if req.Method != http.MethodHead || req.Path != containerPath {
			t.Fatalf("unexpected swift request: %s %s", req.Method, req.Path)
		}
		return newHTTPResponse(http.StatusNoContent, sourceHeaders, "")
	})

	svc := &Service{
		ceph:   ceph,
		swift:  swift,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	_, err := svc.EnsureContainerWithSourceAuth(context.Background(), "AUTH_demo", "images", auth.Credentials{
		AuthToken:     "token-123",
		Authorization: "Bearer token-123",
	})
	if err != nil {
		t.Fatalf("ensure container: %v", err)
	}

	if len(cephRequests) != 3 {
		t.Fatalf("expected 3 ceph requests, got %d", len(cephRequests))
	}
	assertHeader(t, cephRequests[0].Headers, "X-Auth-Token", "token-123")
	assertHeader(t, cephRequests[0].Headers, "Authorization", "Bearer token-123")
	assertHeader(t, cephRequests[0].Headers, "X-Container-Read", ".r:*")
	assertHeader(t, cephRequests[1].Headers, "X-Auth-Token", "token-123")
	assertHeader(t, cephRequests[1].Headers, "Authorization", "Bearer token-123")
	assertHeader(t, cephRequests[1].Headers, "X-Container-Meta-Owner", "demo")
}

func TestCopyObjectNowWithSourceAuthUsesCallerHeadersForTargetPut(t *testing.T) {
	t.Parallel()

	containerPath := backend.BuildContainerPath("AUTH_demo", "images")
	objectPath := backend.BuildObjectPath("AUTH_demo", "images", "a.qcow2")
	containerHeaders := mapHeaders("X-Container-Read", ".r:*")
	objectHeaders := mapHeaders("Content-Type", "application/octet-stream", "X-Object-Meta-Foo", "bar", "ETag", `"abc"`)
	objectBody := "from-swift"
	var cephRequests []backend.Request

	ceph := backendFunc(func(_ context.Context, req backend.Request) (*http.Response, error) {
		cephRequests = append(cephRequests, cloneRequest(req))
		switch {
		case req.Method == http.MethodPut && req.Path == containerPath:
			return newHTTPResponse(http.StatusCreated, nil, "")
		case req.Method == http.MethodPost && req.Path == containerPath:
			return newHTTPResponse(http.StatusNoContent, nil, "")
		case req.Method == http.MethodHead && req.Path == containerPath:
			return newHTTPResponse(http.StatusNoContent, containerHeaders, "")
		case req.Method == http.MethodPut && req.Path == objectPath:
			return newHTTPResponse(http.StatusCreated, nil, "")
		case req.Method == http.MethodHead && req.Path == objectPath:
			return newHTTPResponse(http.StatusNoContent, objectHeaders, "")
		default:
			t.Fatalf("unexpected ceph request: %s %s", req.Method, req.Path)
			return nil, nil
		}
	})

	swift := backendFunc(func(_ context.Context, req backend.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodHead && req.Path == containerPath:
			return newHTTPResponse(http.StatusNoContent, containerHeaders, "")
		case req.Method == http.MethodHead && req.Path == objectPath:
			return newHTTPResponse(http.StatusNoContent, objectHeaders, "")
		case req.Method == http.MethodGet && req.Path == objectPath:
			return newHTTPResponse(http.StatusOK, nil, objectBody)
		default:
			t.Fatalf("unexpected swift request: %s %s", req.Method, req.Path)
			return nil, nil
		}
	})

	svc := &Service{
		ceph:   ceph,
		swift:  swift,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err := svc.CopyObjectNowWithSourceAuth(context.Background(), "AUTH_demo", "images", "a.qcow2", auth.Credentials{
		AuthToken:     "token-123",
		Authorization: "Bearer token-123",
	})
	if err != nil {
		t.Fatalf("copy object: %v", err)
	}

	if len(cephRequests) != 5 {
		t.Fatalf("expected 5 ceph requests, got %d", len(cephRequests))
	}
	objectPut := cephRequests[3]
	assertHeader(t, objectPut.Headers, "X-Auth-Token", "token-123")
	assertHeader(t, objectPut.Headers, "Authorization", "Bearer token-123")
	assertHeader(t, objectPut.Headers, "Content-Type", "application/octet-stream")
	assertHeader(t, objectPut.Headers, "X-Object-Meta-Foo", "bar")
}

func TestSyncContainerNowWithSourceAuthCopiesObjectsImmediately(t *testing.T) {
	t.Parallel()

	containerPath := backend.BuildContainerPath("AUTH_demo", "images")
	objectPath := backend.BuildObjectPath("AUTH_demo", "images", "a.qcow2")
	containerHeaders := mapHeaders("X-Container-Read", ".r:*")
	objectHeaders := mapHeaders("Content-Type", "application/octet-stream", "X-Object-Meta-Foo", "bar", "ETag", `"abc"`)
	var cephRequests []backend.Request

	ceph := backendFunc(func(_ context.Context, req backend.Request) (*http.Response, error) {
		cephRequests = append(cephRequests, cloneRequest(req))
		switch {
		case req.Method == http.MethodPut && req.Path == containerPath:
			return newHTTPResponse(http.StatusCreated, nil, "")
		case req.Method == http.MethodPost && req.Path == containerPath:
			return newHTTPResponse(http.StatusNoContent, nil, "")
		case req.Method == http.MethodHead && req.Path == containerPath:
			return newHTTPResponse(http.StatusNoContent, containerHeaders, "")
		case req.Method == http.MethodPut && req.Path == objectPath:
			return newHTTPResponse(http.StatusCreated, nil, "")
		case req.Method == http.MethodHead && req.Path == objectPath:
			return newHTTPResponse(http.StatusNoContent, objectHeaders, "")
		default:
			t.Fatalf("unexpected ceph request: %s %s", req.Method, req.Path)
			return nil, nil
		}
	})

	swift := backendFunc(func(_ context.Context, req backend.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodHead && req.Path == containerPath:
			return newHTTPResponse(http.StatusNoContent, containerHeaders, "")
		case req.Method == http.MethodGet && req.Path == containerPath && req.Query.Get("format") == "json":
			return newHTTPResponse(http.StatusOK, nil, `[{"name":"a.qcow2"}]`)
		case req.Method == http.MethodHead && req.Path == objectPath:
			return newHTTPResponse(http.StatusNoContent, objectHeaders, "")
		case req.Method == http.MethodGet && req.Path == objectPath:
			return newHTTPResponse(http.StatusOK, nil, "from-swift")
		default:
			t.Fatalf("unexpected swift request: %s %s", req.Method, req.Path)
			return nil, nil
		}
	})

	svc := &Service{
		ceph:   ceph,
		swift:  swift,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err := svc.SyncContainerNowWithSourceAuth(context.Background(), "AUTH_demo", "images", auth.Credentials{
		AuthToken:     "token-123",
		Authorization: "Bearer token-123",
	})
	if err != nil {
		t.Fatalf("sync container: %v", err)
	}

	if len(cephRequests) != 5 {
		t.Fatalf("expected 5 ceph requests, got %d", len(cephRequests))
	}
	objectPut := cephRequests[3]
	assertHeader(t, objectPut.Headers, "X-Auth-Token", "token-123")
	assertHeader(t, objectPut.Headers, "Authorization", "Bearer token-123")
	assertHeader(t, objectPut.Headers, "Content-Type", "application/octet-stream")
	assertHeader(t, objectPut.Headers, "X-Object-Meta-Foo", "bar")
}

func mapHeaders(kv ...string) http.Header {
	headers := make(http.Header)
	for i := 0; i < len(kv); i += 2 {
		headers.Set(kv[i], kv[i+1])
	}
	return headers
}

type backendFunc func(context.Context, backend.Request) (*http.Response, error)

func (f backendFunc) Name() string {
	return "test"
}

func (f backendFunc) Do(ctx context.Context, req backend.Request) (*http.Response, error) {
	return f(ctx, req)
}

func cloneRequest(req backend.Request) backend.Request {
	return backend.Request{
		Method:        req.Method,
		Path:          req.Path,
		Query:         backend.CloneQuery(req.Query),
		Headers:       auth.CloneHeader(req.Headers),
		ContentLength: req.ContentLength,
	}
}

func newHTTPResponse(status int, headers http.Header, body string) (*http.Response, error) {
	resp := &http.Response{
		StatusCode:    status,
		Header:        auth.CloneHeader(headers),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	return resp, nil
}

func assertHeader(t *testing.T, headers http.Header, key, want string) {
	t.Helper()
	if got := headers.Get(key); got != want {
		t.Fatalf("header %s = %q, want %q", key, got, want)
	}
}
