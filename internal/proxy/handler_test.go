package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"swift-migration-api/internal/auth"
	"swift-migration-api/internal/backend"
	"swift-migration-api/internal/config"
	"swift-migration-api/internal/store"
)

type fakeMigrator struct {
	mu              sync.Mutex
	enqueuedObjects []string
	enqueuedConts   []string
	lastObjectCreds auth.Credentials
	lastContCreds   auth.Credentials
}

func (f *fakeMigrator) EnqueueObjectWithSourceAuth(_ context.Context, account, container, object string, creds auth.Credentials) (store.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueuedObjects = append(f.enqueuedObjects, account+"/"+container+"/"+object)
	f.lastObjectCreds = creds
	return store.Job{ID: "object-job"}, nil
}

func (f *fakeMigrator) EnqueueContainerWithSourceAuth(_ context.Context, account, container string, creds auth.Credentials) (store.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueuedConts = append(f.enqueuedConts, account+"/"+container)
	f.lastContCreds = creds
	return store.Job{ID: "container-job"}, nil
}

func (f *fakeMigrator) EnsureContainerWithSourceAuth(_ context.Context, _, _ string, _ auth.Credentials) (bool, error) {
	return true, nil
}

func (f *fakeMigrator) CopyObjectNowWithSourceAuth(_ context.Context, _, _, _ string, _ auth.Credentials) error {
	return nil
}

func TestObjectReadUsesCephWithoutFallback(t *testing.T) {
	t.Parallel()

	var swiftCalls int
	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("from-ceph"))
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		swiftCalls++
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", &fakeMigrator{})
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo/images/a.qcow2", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}
	if string(body) != "from-ceph" {
		t.Fatalf("unexpected body: %q", string(body))
	}
	if swiftCalls != 0 {
		t.Fatalf("expected no swift fallback, got %d calls", swiftCalls)
	}
}

func TestObjectReadFallsBackToSwiftAndEnqueuesMigration(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{}
	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from-swift"))
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", migrator)
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo/images/a.qcow2", nil)
	req.Header.Set("X-Auth-Token", "token-123")
	req.Header.Set("Authorization", "Bearer token-123")
	req.Header.Set("X-Auth-Token-Expires-At", "2030-01-02T03:04:05Z")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}
	if string(body) != "from-swift" {
		t.Fatalf("unexpected body: %q", string(body))
	}

	migrator.mu.Lock()
	defer migrator.mu.Unlock()
	if len(migrator.enqueuedObjects) != 1 || migrator.enqueuedObjects[0] != "AUTH_demo/images/a.qcow2" {
		t.Fatalf("unexpected enqueued objects: %#v", migrator.enqueuedObjects)
	}
	if migrator.lastObjectCreds.AuthToken != "token-123" || migrator.lastObjectCreds.ExpiresAt == nil {
		t.Fatalf("expected caller credentials to be captured, got %#v", migrator.lastObjectCreds)
	}
}

func TestObjectReadSkipsQueuedMigrationWhenTokenExpiryMissing(t *testing.T) {
	t.Parallel()

	migrator := &fakeMigrator{}
	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from-swift"))
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", migrator)
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo/images/a.qcow2", nil)
	req.Header.Set("X-Auth-Token", "token-123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	migrator.mu.Lock()
	defer migrator.mu.Unlock()
	if len(migrator.enqueuedObjects) != 0 {
		t.Fatalf("expected migration enqueue to be skipped, got %#v", migrator.enqueuedObjects)
	}
}

func TestAccountListingMergesCephAndSwift(t *testing.T) {
	t.Parallel()

	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"alpha"},{"name":"beta"}]`))
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"beta"},{"name":"gamma"}]`))
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", &fakeMigrator{})
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo?format=json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}

	var items []map[string]any
	if err := json.NewDecoder(res.Body).Decode(&items); err != nil {
		t.Fatalf("decode merged response: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("unexpected merged listing length: %d", len(items))
	}
}

func TestAccountListingReturnsSwiftErrorWhenCephIsMissing(t *testing.T) {
	t.Parallel()

	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "swift denied account listing", http.StatusForbidden)
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", &fakeMigrator{})
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo?format=json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}
	if string(body) != "swift denied account listing\n" {
		t.Fatalf("unexpected body: %q", string(body))
	}
}

func TestAccountListingDoesNotHideSwiftFailureBehindPartialCephResult(t *testing.T) {
	t.Parallel()

	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"alpha"}]`))
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "swift listing failed", http.StatusBadGateway)
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", &fakeMigrator{})
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo?format=json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}
	if string(body) != "swift listing failed\n" {
		t.Fatalf("unexpected body: %q", string(body))
	}
}

func TestAccountListingWithTrailingSlashFallsBackToSwift(t *testing.T) {
	t.Parallel()

	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/AUTH_demo" {
			t.Fatalf("unexpected ceph path: %s", r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/AUTH_demo" {
			t.Fatalf("unexpected swift path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"legacy"}]`))
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", &fakeMigrator{})
	req := httptest.NewRequest(http.MethodGet, "/v1/AUTH_demo/?format=json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}

	var items []map[string]any
	if err := json.NewDecoder(res.Body).Decode(&items); err != nil {
		t.Fatalf("decode fallback response: %v", err)
	}
	if len(items) != 1 || items[0]["name"] != "legacy" {
		t.Fatalf("unexpected merged listing: %#v", items)
	}
}

func TestAccountListingWithPublicPrefixFallsBackToSwift(t *testing.T) {
	t.Parallel()

	cephServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/AUTH_demo" {
			t.Fatalf("unexpected ceph path: %s", r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	defer cephServer.Close()

	swiftServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/AUTH_demo" {
			t.Fatalf("unexpected swift path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"legacy"}]`))
	}))
	defer swiftServer.Close()

	handler := newTestHandler(t, cephServer.URL+"/v1", swiftServer.URL+"/v1", &fakeMigrator{})
	req := httptest.NewRequest(http.MethodGet, "/swift/v1/AUTH_demo?format=json", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.StatusCode)
	}

	var items []map[string]any
	if err := json.NewDecoder(res.Body).Decode(&items); err != nil {
		t.Fatalf("decode fallback response: %v", err)
	}
	if len(items) != 1 || items[0]["name"] != "legacy" {
		t.Fatalf("unexpected merged listing: %#v", items)
	}
}

func newTestHandler(t *testing.T, cephURL, swiftURL string, migrator *fakeMigrator) *Handler {
	t.Helper()

	cephClient, err := backend.New("ceph", config.BackendConfig{BaseURL: cephURL})
	if err != nil {
		t.Fatalf("new ceph backend: %v", err)
	}
	swiftClient, err := backend.New("swift", config.BackendConfig{BaseURL: swiftURL})
	if err != nil {
		t.Fatalf("new swift backend: %v", err)
	}

	return NewHandler(
		cephClient,
		swiftClient,
		migrator,
		auth.NewHeaderForwarder([]string{"X-Auth-Token", "Authorization"}),
		true,
		true,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}
