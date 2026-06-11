package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"swift-migration-api/internal/store"
)

type Service interface {
	Stats(ctx context.Context) (store.Stats, error)
	ListJobs(ctx context.Context, limit int) ([]store.Job, error)
	GetJob(ctx context.Context, id string) (store.Job, error)
	EnqueueObject(ctx context.Context, account, container, object string) (store.Job, error)
	EnqueueContainer(ctx context.Context, account, container string) (store.Job, error)
	EnqueueAccount(ctx context.Context, account string) (store.Job, error)
}

type Handler struct {
	svc       Service
	authToken string
}

func NewHandler(svc Service, authToken string) *Handler {
	return &Handler{svc: svc, authToken: authToken}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/_migration/healthz":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	case "/_migration/readyz":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}

	if !h.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/_migration/stats":
		stats, err := h.svc.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, stats)
	case r.Method == http.MethodGet && r.URL.Path == "/_migration/jobs":
		limit := 100
		if raw := r.URL.Query().Get("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		jobs, err := h.svc.ListJobs(r.Context(), limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, jobs)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_migration/jobs/"):
		id := strings.TrimPrefix(r.URL.Path, "/_migration/jobs/")
		if id == "" || strings.Contains(id, "/") {
			http.NotFound(w, r)
			return
		}
		job, err := h.svc.GetJob(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, job)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/_migration/jobs/object/"):
		account, container, object, ok := splitObjectPath(strings.TrimPrefix(r.URL.Path, "/_migration/jobs/object/"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		job, err := h.svc.EnqueueObject(r.Context(), account, container, object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/_migration/jobs/container/"):
		account, container, ok := splitContainerPath(strings.TrimPrefix(r.URL.Path, "/_migration/jobs/container/"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		job, err := h.svc.EnqueueContainer(r.Context(), account, container)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/_migration/jobs/account/"):
		account := strings.TrimPrefix(r.URL.Path, "/_migration/jobs/account/")
		account = strings.Trim(account, "/")
		if account == "" || strings.Contains(account, "/") {
			http.NotFound(w, r)
			return
		}
		job, err := h.svc.EnqueueAccount(r.Context(), account)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) authorized(r *http.Request) bool {
	if h.authToken == "" {
		return false
	}
	if r.Header.Get("X-Admin-Token") == h.authToken {
		return true
	}
	authHeader := r.Header.Get("Authorization")
	return authHeader == "Bearer "+h.authToken
}

func splitContainerPath(path string) (account, container string, ok bool) {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return "", "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func splitObjectPath(path string) (account, container, object string, ok bool) {
	path = strings.Trim(path, "/")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) != 3 {
		return "", "", "", false
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
