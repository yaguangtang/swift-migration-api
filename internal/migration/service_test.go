package migration

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

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

func mapHeaders(kv ...string) http.Header {
	headers := make(http.Header)
	for i := 0; i < len(kv); i += 2 {
		headers.Set(kv[i], kv[i+1])
	}
	return headers
}
