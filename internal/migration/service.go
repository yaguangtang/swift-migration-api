package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"swift-migration-api/internal/auth"
	"swift-migration-api/internal/backend"
	"swift-migration-api/internal/config"
	"swift-migration-api/internal/store"
)

var ErrNotFound = errors.New("resource not found")

const minFreshTokenLifetime = 30 * time.Second

type JobStore interface {
	Enqueue(ctx context.Context, job store.Job) (store.Job, error)
	Get(ctx context.Context, id string) (store.Job, error)
	List(ctx context.Context, limit int) ([]store.Job, error)
	ClaimNext(ctx context.Context, maxAttempts int) (store.Job, error)
	MarkSucceeded(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id, lastError string, nextRun time.Time) error
	MarkRejected(ctx context.Context, id, lastError string) error
	Stats(ctx context.Context) (store.Stats, error)
}

type Service struct {
	ceph        backend.Client
	swift       backend.Client
	store       JobStore
	tokenSource auth.TokenSource
	cfg         config.MigrationConfig
	logger      *slog.Logger
}

func NewService(
	ceph backend.Client,
	swift backend.Client,
	store JobStore,
	tokenSource auth.TokenSource,
	cfg config.MigrationConfig,
	logger *slog.Logger,
) *Service {
	return &Service{
		ceph:        ceph,
		swift:       swift,
		store:       store,
		tokenSource: tokenSource,
		cfg:         cfg,
		logger:      logger,
	}
}

func (s *Service) Start(ctx context.Context) {
	for i := 0; i < s.cfg.WorkerConcurrency; i++ {
		go s.worker(ctx, i+1)
	}
}

func (s *Service) EnqueueObject(ctx context.Context, account, container, object string) (store.Job, error) {
	return s.enqueue(ctx, store.JobTypeObjectCopy, account, container, object, auth.Credentials{})
}

func (s *Service) EnqueueObjectWithSourceAuth(ctx context.Context, account, container, object string, creds auth.Credentials) (store.Job, error) {
	return s.enqueue(ctx, store.JobTypeObjectCopy, account, container, object, creds)
}

func (s *Service) EnqueueContainer(ctx context.Context, account, container string) (store.Job, error) {
	return s.enqueue(ctx, store.JobTypeContainerSync, account, container, "", auth.Credentials{})
}

func (s *Service) EnqueueContainerWithSourceAuth(ctx context.Context, account, container string, creds auth.Credentials) (store.Job, error) {
	return s.enqueue(ctx, store.JobTypeContainerSync, account, container, "", creds)
}

func (s *Service) EnqueueAccount(ctx context.Context, account string) (store.Job, error) {
	return s.enqueue(ctx, store.JobTypeAccountScan, account, "", "", auth.Credentials{})
}

func (s *Service) EnqueueAccountWithSourceAuth(ctx context.Context, account string, creds auth.Credentials) (store.Job, error) {
	return s.enqueue(ctx, store.JobTypeAccountScan, account, "", "", creds)
}

func (s *Service) ListJobs(ctx context.Context, limit int) ([]store.Job, error) {
	return s.store.List(ctx, limit)
}

func (s *Service) GetJob(ctx context.Context, id string) (store.Job, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) Stats(ctx context.Context) (store.Stats, error) {
	return s.store.Stats(ctx)
}

func (s *Service) EnsureContainer(ctx context.Context, account, container string) (bool, error) {
	return s.EnsureContainerWithSourceAuth(ctx, account, container, auth.Credentials{})
}

func (s *Service) EnsureContainerWithSourceAuth(ctx context.Context, account, container string, creds auth.Credentials) (bool, error) {
	headers, err := s.headersForCredentials(ctx, creds, false)
	if err != nil {
		return false, err
	}

	path := backend.BuildContainerPath(account, container)
	sourceHead, err := s.swift.Do(ctx, backend.Request{
		Method:  http.MethodHead,
		Path:    path,
		Headers: headers,
	})
	if err != nil {
		return false, fmt.Errorf("head source container: %w", err)
	}
	defer sourceHead.Body.Close()

	if sourceHead.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if !backend.IsSuccess(sourceHead.StatusCode) {
		return false, fmt.Errorf("head source container returned status %d", sourceHead.StatusCode)
	}

	containerHeaders := filterContainerHeaders(sourceHead.Header)
	putResp, err := s.ceph.Do(ctx, backend.Request{
		Method:  http.MethodPut,
		Path:    path,
		Headers: containerHeaders,
	})
	if err != nil {
		return false, fmt.Errorf("create target container: %w", err)
	}
	defer putResp.Body.Close()
	if !backend.IsSuccess(putResp.StatusCode) {
		return false, fmt.Errorf("create target container returned status %d", putResp.StatusCode)
	}

	if len(containerHeaders) > 0 {
		postResp, err := s.ceph.Do(ctx, backend.Request{
			Method:  http.MethodPost,
			Path:    path,
			Headers: containerHeaders,
		})
		if err != nil {
			return false, fmt.Errorf("sync target container metadata: %w", err)
		}
		defer postResp.Body.Close()
		if !backend.IsSuccess(postResp.StatusCode) {
			return false, fmt.Errorf("sync target container metadata returned status %d", postResp.StatusCode)
		}
	}

	if err := s.verifyContainerSync(ctx, path, headers, sourceHead.Header); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Service) CopyObjectNow(ctx context.Context, account, container, object string) error {
	return s.CopyObjectNowWithSourceAuth(ctx, account, container, object, auth.Credentials{})
}

func (s *Service) CopyObjectNowWithSourceAuth(ctx context.Context, account, container, object string, creds auth.Credentials) error {
	headers, err := s.headersForCredentials(ctx, creds, false)
	if err != nil {
		return err
	}

	if _, err := s.EnsureContainerWithSourceAuth(ctx, account, container, creds); err != nil {
		return err
	}

	path := backend.BuildObjectPath(account, container, object)
	sourceHead, err := s.swift.Do(ctx, backend.Request{
		Method:  http.MethodHead,
		Path:    path,
		Headers: headers,
	})
	if err != nil {
		return fmt.Errorf("head source object: %w", err)
	}
	defer sourceHead.Body.Close()

	if sourceHead.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if !backend.IsSuccess(sourceHead.StatusCode) {
		return fmt.Errorf("head source object returned status %d", sourceHead.StatusCode)
	}

	sourceResp, err := s.swift.Do(ctx, backend.Request{
		Method:  http.MethodGet,
		Path:    path,
		Headers: headers,
	})
	if err != nil {
		return fmt.Errorf("get source object: %w", err)
	}
	defer sourceResp.Body.Close()

	if sourceResp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if !backend.IsSuccess(sourceResp.StatusCode) {
		return fmt.Errorf("get source object returned status %d", sourceResp.StatusCode)
	}

	objectHeaders := filterObjectHeaders(sourceHead.Header)
	putResp, err := s.ceph.Do(ctx, backend.Request{
		Method:        http.MethodPut,
		Path:          path,
		Headers:       objectHeaders,
		Body:          sourceResp.Body,
		ContentLength: sourceResp.ContentLength,
	})
	if err != nil {
		return fmt.Errorf("put target object: %w", err)
	}
	defer putResp.Body.Close()

	if !backend.IsSuccess(putResp.StatusCode) {
		return fmt.Errorf("put target object returned status %d", putResp.StatusCode)
	}

	targetHead, err := s.ceph.Do(ctx, backend.Request{
		Method:  http.MethodHead,
		Path:    path,
		Headers: headers,
	})
	if err != nil {
		return fmt.Errorf("head target object after copy: %w", err)
	}
	defer targetHead.Body.Close()
	if !backend.IsSuccess(targetHead.StatusCode) {
		return fmt.Errorf("head target object returned status %d after copy", targetHead.StatusCode)
	}

	if err := s.verifyObjectCopy(sourceHead.Header, targetHead.Header); err != nil {
		return err
	}

	return nil
}

func (s *Service) enqueue(ctx context.Context, jobType store.JobType, account, container, object string, creds auth.Credentials) (store.Job, error) {
	if creds.HasAuth() && creds.ExpiresAt == nil {
		return store.Job{}, fmt.Errorf("queued caller-token migration requires a token expiry header")
	}
	now := time.Now().UTC()
	job := store.Job{
		ID:                fmt.Sprintf("%d-%s", now.UnixNano(), strings.ReplaceAll(dedupeKey(jobType, account, container, object), "/", "_")),
		Type:              jobType,
		Account:           account,
		Container:         container,
		Object:            object,
		Status:            store.StatusPending,
		DedupeKey:         dedupeKey(jobType, account, container, object),
		AuthToken:         creds.AuthToken,
		Authz:             creds.Authorization,
		AuthExpiry:        cloneTime(creds.ExpiresAt),
		RequireFreshToken: creds.HasAuth(),
		NextRunAt:         now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	return s.store.Enqueue(ctx, job)
}

func (s *Service) worker(ctx context.Context, workerID int) {
	logger := s.logger.With("worker", workerID)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := s.store.ClaimNext(ctx, s.cfg.MaxAttempts)
		if err != nil {
			if errors.Is(err, store.ErrNoJobReady) {
				_ = backend.SleepContext(ctx, s.cfg.PollInterval.Std())
				continue
			}
			logger.Error("failed to claim job", "error", err)
			_ = backend.SleepContext(ctx, s.cfg.PollInterval.Std())
			continue
		}

		logger.Info("processing job", "job_id", job.ID, "type", job.Type, "account", job.Account, "container", job.Container, "object", job.Object)
		if rejected, reason := s.rejectReason(job); rejected {
			logger.Warn("rejecting job before execution", "job_id", job.ID, "reason", reason)
			if markErr := s.store.MarkRejected(ctx, job.ID, reason); markErr != nil {
				logger.Error("failed to mark job rejected", "job_id", job.ID, "error", markErr)
			}
			continue
		}
		if err := s.processJob(ctx, job); err != nil {
			backoff := s.cfg.RetryBase.Std() * time.Duration(1<<(max(job.Attempts-1, 0)))
			nextRun := time.Now().UTC().Add(backoff)
			logger.Error("job failed", "job_id", job.ID, "error", err, "next_run_at", nextRun)
			if markErr := s.store.MarkFailed(ctx, job.ID, err.Error(), nextRun); markErr != nil {
				logger.Error("failed to mark job failed", "job_id", job.ID, "error", markErr)
			}
			continue
		}

		if err := s.store.MarkSucceeded(ctx, job.ID); err != nil {
			logger.Error("failed to mark job succeeded", "job_id", job.ID, "error", err)
		}
	}
}

func (s *Service) processJob(ctx context.Context, job store.Job) error {
	switch job.Type {
	case store.JobTypeObjectCopy:
		return s.CopyObjectNowWithSourceAuth(ctx, job.Account, job.Container, job.Object, credentialsFromJob(job))
	case store.JobTypeContainerSync:
		return s.processContainerJob(ctx, job)
	case store.JobTypeAccountScan:
		return s.processAccountJob(ctx, job)
	default:
		return fmt.Errorf("unsupported job type %q", job.Type)
	}
}

func (s *Service) processAccountJob(ctx context.Context, job store.Job) error {
	headers, err := s.headersForCredentials(ctx, credentialsFromJob(job), false)
	if err != nil {
		return err
	}

	marker := ""
	for {
		query := url.Values{
			"format": []string{"json"},
			"limit":  []string{"1000"},
		}
		if marker != "" {
			query.Set("marker", marker)
		}
		resp, err := s.swift.Do(ctx, backend.Request{
			Method:  http.MethodGet,
			Path:    backend.BuildAccountPath(job.Account),
			Query:   query,
			Headers: headers,
		})
		if err != nil {
			return fmt.Errorf("list source account: %w", err)
		}
		items, err := decodeJSONItems(resp)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}

		for _, item := range items {
			name, _ := item["name"].(string)
			if name == "" {
				continue
			}
			if _, err := s.EnqueueContainerWithSourceAuth(ctx, job.Account, name, credentialsFromJob(job)); err != nil {
				s.logger.Error("failed to enqueue container sync", "account", job.Account, "container", name, "error", err)
			}
			marker = name
		}

		if len(items) < 1000 {
			return nil
		}
	}
}

func (s *Service) processContainerJob(ctx context.Context, job store.Job) error {
	creds := credentialsFromJob(job)
	exists, err := s.EnsureContainerWithSourceAuth(ctx, job.Account, job.Container, creds)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	headers, err := s.headersForCredentials(ctx, creds, false)
	if err != nil {
		return err
	}

	marker := ""
	for {
		query := url.Values{
			"format": []string{"json"},
			"limit":  []string{"1000"},
		}
		if marker != "" {
			query.Set("marker", marker)
		}
		resp, err := s.swift.Do(ctx, backend.Request{
			Method:  http.MethodGet,
			Path:    backend.BuildContainerPath(job.Account, job.Container),
			Query:   query,
			Headers: headers,
		})
		if err != nil {
			return fmt.Errorf("list source container: %w", err)
		}
		items, err := decodeJSONItems(resp)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}

		for _, item := range items {
			name, _ := item["name"].(string)
			if name == "" {
				continue
			}
			if _, err := s.EnqueueObjectWithSourceAuth(ctx, job.Account, job.Container, name, creds); err != nil {
				s.logger.Error("failed to enqueue object copy", "account", job.Account, "container", job.Container, "object", name, "error", err)
			}
			marker = name
		}

		if len(items) < 1000 {
			return nil
		}
	}
}

func decodeJSONItems(resp *http.Response) ([]map[string]any, error) {
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if !backend.IsSuccess(resp.StatusCode) {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("unexpected list status %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	var items []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode listing response: %w", err)
	}
	return items, nil
}

func dedupeKey(jobType store.JobType, account, container, object string) string {
	return strings.Join([]string{string(jobType), account, container, object}, "|")
}

func filterContainerHeaders(in http.Header) http.Header {
	out := make(http.Header)
	for key, values := range in {
		canonical := http.CanonicalHeaderKey(key)
		if canonical == "X-Container-Read" || canonical == "X-Container-Write" || strings.HasPrefix(canonical, "X-Container-Meta-") {
			for _, value := range values {
				out.Add(key, value)
			}
		}
	}
	return out
}

func filterObjectHeaders(in http.Header) http.Header {
	out := make(http.Header)
	copyIfPresent(out, in, "Content-Type")
	copyIfPresent(out, in, "Content-Encoding")
	copyIfPresent(out, in, "Content-Disposition")
	copyIfPresent(out, in, "Cache-Control")
	copyIfPresent(out, in, "Content-Language")
	copyIfPresent(out, in, "X-Delete-At")
	copyIfPresent(out, in, "X-Delete-After")
	copyIfPresent(out, in, "X-Object-Manifest")
	copyIfPresent(out, in, "X-Static-Large-Object")
	for key, values := range in {
		canonical := http.CanonicalHeaderKey(key)
		if strings.HasPrefix(canonical, "X-Object-Meta-") {
			for _, value := range values {
				out.Add(key, value)
			}
		}
	}
	return out
}

func copyIfPresent(out, in http.Header, key string) {
	for _, value := range in.Values(key) {
		out.Add(key, value)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Service) headersForCredentials(ctx context.Context, creds auth.Credentials, enforceFresh bool) (http.Header, error) {
	if creds.HasAuth() {
		if creds.ExpiresAt != nil {
			if time.Until(creds.ExpiresAt.UTC()) <= 0 {
				return nil, fmt.Errorf("caller token is expired")
			}
			if enforceFresh && time.Until(creds.ExpiresAt.UTC()) < minFreshTokenLifetime {
				return nil, fmt.Errorf("caller token expires too soon")
			}
		}
		return creds.Headers(), nil
	}

	headers, err := auth.WorkerHeaders(ctx, s.tokenSource)
	if err != nil {
		return nil, fmt.Errorf("get worker token: %w", err)
	}
	return headers, nil
}

func (s *Service) rejectReason(job store.Job) (bool, string) {
	if !job.RequireFreshToken {
		return false, ""
	}
	if job.AuthExpiry == nil {
		return true, "job requires caller token freshness but has no token expiry"
	}
	if time.Until(job.AuthExpiry.UTC()) < minFreshTokenLifetime {
		return true, fmt.Sprintf("caller token expired or expires too soon at %s", job.AuthExpiry.UTC().Format(time.RFC3339))
	}
	return false, ""
}

func credentialsFromJob(job store.Job) auth.Credentials {
	return auth.Credentials{
		AuthToken:     job.AuthToken,
		Authorization: job.Authz,
		ExpiresAt:     cloneTime(job.AuthExpiry),
	}
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := value.UTC()
	return &clone
}

func (s *Service) verifyContainerSync(ctx context.Context, path string, headers http.Header, sourceHeaders http.Header) error {
	targetHead, err := s.ceph.Do(ctx, backend.Request{
		Method:  http.MethodHead,
		Path:    path,
		Headers: headers,
	})
	if err != nil {
		return fmt.Errorf("head target container after sync: %w", err)
	}
	defer targetHead.Body.Close()
	if !backend.IsSuccess(targetHead.StatusCode) {
		return fmt.Errorf("head target container returned status %d after sync", targetHead.StatusCode)
	}

	if err := compareHeaders(sourceHeaders, targetHead.Header, containerVerifyMatcher); err != nil {
		return fmt.Errorf("container verification failed: %w", err)
	}
	return nil
}

func (s *Service) verifyObjectCopy(sourceHeaders http.Header, targetHeaders http.Header) error {
	if err := compareHeaders(sourceHeaders, targetHeaders, objectVerifyMatcher); err != nil {
		return fmt.Errorf("object verification failed: %w", err)
	}

	if s.cfg.VerifyETag {
		sourceETag := backend.NormalizeETag(sourceHeaders.Get("ETag"))
		targetETag := backend.NormalizeETag(targetHeaders.Get("ETag"))
		if sourceETag != "" && targetETag != "" && sourceETag != targetETag {
			return fmt.Errorf("etag mismatch after copy: source=%s target=%s", sourceETag, targetETag)
		}
	}
	return nil
}

func compareHeaders(source, target http.Header, matcher func(string) bool) error {
	keys := make(map[string]struct{})
	for key := range source {
		canonical := http.CanonicalHeaderKey(key)
		if matcher(canonical) {
			keys[canonical] = struct{}{}
		}
	}
	for key := range target {
		canonical := http.CanonicalHeaderKey(key)
		if matcher(canonical) {
			keys[canonical] = struct{}{}
		}
	}

	for key := range keys {
		sourceValue := strings.Join(source.Values(key), ",")
		targetValue := strings.Join(target.Values(key), ",")
		if sourceValue != targetValue {
			return fmt.Errorf("header %s mismatch: source=%q target=%q", key, sourceValue, targetValue)
		}
	}
	return nil
}

func containerVerifyMatcher(header string) bool {
	switch header {
	case "X-Container-Read", "X-Container-Write", "X-Container-Owner":
		return true
	default:
		return strings.HasPrefix(header, "X-Container-Meta-")
	}
}

func objectVerifyMatcher(header string) bool {
	switch header {
	case "Content-Type", "Content-Encoding", "Content-Disposition", "Cache-Control", "Content-Language",
		"X-Delete-At", "X-Delete-After", "X-Object-Manifest", "X-Static-Large-Object",
		"X-Object-Read", "X-Object-Write", "X-Object-Owner", "ETag":
		return true
	default:
		return strings.HasPrefix(header, "X-Object-Meta-")
	}
}
