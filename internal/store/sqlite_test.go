package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStoreEnqueueDedupAndClaim(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "jobs.sqlite")
	jobStore, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer jobStore.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	job := Job{
		ID:                "job-1",
		Type:              JobTypeObjectCopy,
		Account:           "AUTH_demo",
		Container:         "images",
		Object:            "ubuntu.qcow2",
		Status:            StatusPending,
		DedupeKey:         "object|AUTH_demo|images|ubuntu.qcow2",
		AuthToken:         "token-a",
		Authz:             "Bearer token-a",
		RequireFreshToken: true,
		NextRunAt:         now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	first, err := jobStore.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("enqueue first job: %v", err)
	}
	second, err := jobStore.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("enqueue duplicate job: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected deduplicated job id, got %s and %s", first.ID, second.ID)
	}
	if !second.RequireFreshToken || second.AuthToken != "token-a" {
		t.Fatalf("expected stored auth metadata, got %#v", second)
	}

	claimed, err := jobStore.ClaimNext(ctx, 5)
	if err != nil {
		t.Fatalf("claim next job: %v", err)
	}
	if claimed.Attempts != 1 || claimed.Status != StatusRunning {
		t.Fatalf("unexpected claimed job: %#v", claimed)
	}

	if err := jobStore.MarkFailed(ctx, claimed.ID, "temporary error", time.Now().UTC().Add(-time.Second)); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	reclaimed, err := jobStore.ClaimNext(ctx, 5)
	if err != nil {
		t.Fatalf("reclaim failed job: %v", err)
	}
	if reclaimed.Attempts != 2 {
		t.Fatalf("expected attempt count 2, got %d", reclaimed.Attempts)
	}
}

func TestSQLiteStoreDuplicateRefreshesFresherAuth(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "jobs.sqlite")
	jobStore, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer jobStore.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	later := now.Add(time.Hour)
	original := Job{
		ID:                "job-1",
		Type:              JobTypeObjectCopy,
		Account:           "AUTH_demo",
		Container:         "images",
		Object:            "ubuntu.qcow2",
		Status:            StatusPending,
		DedupeKey:         "object|AUTH_demo|images|ubuntu.qcow2",
		AuthToken:         "token-old",
		Authz:             "Bearer token-old",
		AuthExpiry:        &now,
		RequireFreshToken: true,
		NextRunAt:         now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	refreshed := original
	refreshed.ID = "job-2"
	refreshed.AuthToken = "token-new"
	refreshed.Authz = "Bearer token-new"
	refreshed.AuthExpiry = &later

	if _, err := jobStore.Enqueue(ctx, original); err != nil {
		t.Fatalf("enqueue original job: %v", err)
	}
	got, err := jobStore.Enqueue(ctx, refreshed)
	if err != nil {
		t.Fatalf("enqueue refreshed job: %v", err)
	}
	if got.AuthToken != "token-new" {
		t.Fatalf("expected updated auth token, got %q", got.AuthToken)
	}
	if got.AuthExpiry == nil || !got.AuthExpiry.Equal(later) {
		t.Fatalf("expected updated auth expiry, got %#v", got.AuthExpiry)
	}
}
