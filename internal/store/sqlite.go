package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type JobType string

const (
	JobTypeObjectCopy    JobType = "object-copy"
	JobTypeContainerSync JobType = "container-sync"
	JobTypeAccountScan   JobType = "account-scan"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusRejected  Status = "rejected"
)

var ErrNoJobReady = errors.New("no job ready")

type Job struct {
	ID                string     `json:"id"`
	Type              JobType    `json:"type"`
	Account           string     `json:"account"`
	Container         string     `json:"container,omitempty"`
	Object            string     `json:"object,omitempty"`
	Status            Status     `json:"status"`
	Attempts          int        `json:"attempts"`
	LastError         string     `json:"last_error,omitempty"`
	DedupeKey         string     `json:"dedupe_key"`
	AuthToken         string     `json:"-"`
	Authz             string     `json:"-"`
	AuthExpiry        *time.Time `json:"auth_expiry,omitempty"`
	RequireFreshToken bool       `json:"require_fresh_token,omitempty"`
	NextRunAt         time.Time  `json:"next_run_at"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type Stats struct {
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Rejected  int `json:"rejected"`
}

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}
	store := &SQLiteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) init() error {
	const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  account TEXT NOT NULL,
  container_name TEXT NOT NULL DEFAULT '',
  object_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  dedupe_key TEXT NOT NULL UNIQUE,
  auth_token TEXT NOT NULL DEFAULT '',
  authorization TEXT NOT NULL DEFAULT '',
  auth_expiry TIMESTAMP NULL,
  require_fresh_token INTEGER NOT NULL DEFAULT 0,
  next_run_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_next_run ON jobs(status, next_run_at);
`
	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("init sqlite schema: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Enqueue(ctx context.Context, job Job) (Job, error) {
	const insert = `
INSERT INTO jobs (
  id, type, account, container_name, object_name, status, attempts,
  last_error, dedupe_key, auth_token, authorization, auth_expiry,
  require_fresh_token, next_run_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

	_, err := s.db.ExecContext(ctx, insert,
		job.ID, string(job.Type), job.Account, job.Container, job.Object, string(job.Status),
		job.Attempts, job.LastError, job.DedupeKey, job.AuthToken, job.Authz,
		nullTime(job.AuthExpiry), boolToInt(job.RequireFreshToken), job.NextRunAt.UTC(), job.CreatedAt.UTC(), job.UpdatedAt.UTC(),
	)
	if err == nil {
		return job, nil
	}

	const selectByKey = `
SELECT id, type, account, container_name, object_name, status, attempts,
       last_error, dedupe_key, auth_token, authorization, auth_expiry,
       require_fresh_token, next_run_at, created_at, updated_at
FROM jobs WHERE dedupe_key = ?
`
	existing, selectErr := s.getOne(ctx, selectByKey, job.DedupeKey)
	if selectErr != nil {
		return Job{}, fmt.Errorf("enqueue job: %w", err)
	}
	if refreshed, updateErr := s.refreshAuthIfNeeded(ctx, existing, job); updateErr == nil {
		return refreshed, nil
	}
	return existing, nil
}

func (s *SQLiteStore) Get(ctx context.Context, id string) (Job, error) {
	const query = `
SELECT id, type, account, container_name, object_name, status, attempts,
       last_error, dedupe_key, auth_token, authorization, auth_expiry,
       require_fresh_token, next_run_at, created_at, updated_at
FROM jobs WHERE id = ?
`
	return s.getOne(ctx, query, id)
}

func (s *SQLiteStore) List(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, type, account, container_name, object_name, status, attempts,
       last_error, dedupe_key, auth_token, authorization, auth_expiry,
       require_fresh_token, next_run_at, created_at, updated_at
FROM jobs
ORDER BY created_at DESC
LIMIT ?
`, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *SQLiteStore) ClaimNext(ctx context.Context, maxAttempts int) (Job, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return Job{}, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	row := tx.QueryRowContext(ctx, `
SELECT id, type, account, container_name, object_name, status, attempts,
       last_error, dedupe_key, auth_token, authorization, auth_expiry,
       require_fresh_token, next_run_at, created_at, updated_at
FROM jobs
WHERE status IN (?, ?)
  AND attempts < ?
  AND next_run_at <= ?
ORDER BY next_run_at ASC, created_at ASC
LIMIT 1
`, string(StatusPending), string(StatusFailed), maxAttempts, time.Now().UTC())

	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, ErrNoJobReady
		}
		return Job{}, err
	}

	job.Attempts++
	job.Status = StatusRunning
	job.UpdatedAt = time.Now().UTC()

	_, err = tx.ExecContext(ctx, `
UPDATE jobs
SET status = ?, attempts = ?, updated_at = ?
WHERE id = ?
`, string(job.Status), job.Attempts, job.UpdatedAt, job.ID)
	if err != nil {
		return Job{}, fmt.Errorf("claim job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("commit claim job: %w", err)
	}
	return job, nil
}

func (s *SQLiteStore) MarkSucceeded(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = ?, last_error = '', updated_at = ?
WHERE id = ?
`, string(StatusSucceeded), time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("mark job succeeded: %w", err)
	}
	return nil
}

func (s *SQLiteStore) MarkFailed(ctx context.Context, id, lastError string, nextRun time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = ?, last_error = ?, next_run_at = ?, updated_at = ?
WHERE id = ?
`, string(StatusFailed), lastError, nextRun.UTC(), time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("mark job failed: %w", err)
	}
	return nil
}

func (s *SQLiteStore) MarkRejected(ctx context.Context, id, lastError string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET status = ?, last_error = ?, updated_at = ?
WHERE id = ?
`, string(StatusRejected), lastError, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("mark job rejected: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Stats(ctx context.Context) (Stats, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT status, COUNT(*)
FROM jobs
GROUP BY status
`)
	if err != nil {
		return Stats{}, fmt.Errorf("job stats: %w", err)
	}
	defer rows.Close()

	stats := Stats{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return Stats{}, fmt.Errorf("scan stats: %w", err)
		}
		switch Status(status) {
		case StatusPending:
			stats.Pending = count
		case StatusRunning:
			stats.Running = count
		case StatusSucceeded:
			stats.Succeeded = count
		case StatusFailed:
			stats.Failed = count
		case StatusRejected:
			stats.Rejected = count
		}
	}
	return stats, rows.Err()
}

func (s *SQLiteStore) getOne(ctx context.Context, query string, arg any) (Job, error) {
	row := s.db.QueryRowContext(ctx, query, arg)
	return scanJob(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(row scanner) (Job, error) {
	var job Job
	var typ string
	var status string
	var authExpiry sql.NullTime
	var requireFresh int
	if err := row.Scan(
		&job.ID, &typ, &job.Account, &job.Container, &job.Object, &status,
		&job.Attempts, &job.LastError, &job.DedupeKey, &job.AuthToken, &job.Authz,
		&authExpiry, &requireFresh, &job.NextRunAt, &job.CreatedAt, &job.UpdatedAt,
	); err != nil {
		return Job{}, err
	}
	job.Type = JobType(typ)
	job.Status = Status(status)
	if authExpiry.Valid {
		value := authExpiry.Time.UTC()
		job.AuthExpiry = &value
	}
	job.RequireFreshToken = requireFresh == 1
	return job, nil
}

func (s *SQLiteStore) refreshAuthIfNeeded(ctx context.Context, existing, incoming Job) (Job, error) {
	if !shouldRefreshAuth(existing, incoming) {
		return existing, nil
	}

	_, err := s.db.ExecContext(ctx, `
UPDATE jobs
SET auth_token = ?, authorization = ?, auth_expiry = ?, require_fresh_token = ?, updated_at = ?
WHERE id = ?
`, incoming.AuthToken, incoming.Authz, nullTime(incoming.AuthExpiry), boolToInt(incoming.RequireFreshToken), time.Now().UTC(), existing.ID)
	if err != nil {
		return Job{}, fmt.Errorf("refresh job auth: %w", err)
	}
	return s.Get(ctx, existing.ID)
}

func shouldRefreshAuth(existing, incoming Job) bool {
	if incoming.AuthToken == "" && incoming.Authz == "" {
		return false
	}
	if existing.Status == StatusSucceeded || existing.Status == StatusRejected {
		return false
	}
	if existing.AuthToken == "" && existing.Authz == "" {
		return true
	}
	if existing.AuthExpiry == nil {
		return incoming.AuthExpiry != nil
	}
	if incoming.AuthExpiry == nil {
		return false
	}
	return incoming.AuthExpiry.After(*existing.AuthExpiry)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullTime(value *time.Time) sql.NullTime {
	if value == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: value.UTC(), Valid: true}
}
