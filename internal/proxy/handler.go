package proxy

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

	"swift-migration-api/internal/auth"
	"swift-migration-api/internal/backend"
	"swift-migration-api/internal/migration"
	"swift-migration-api/internal/store"
)

type Migrator interface {
	EnqueueObjectWithSourceAuth(ctx context.Context, account, container, object string, creds auth.Credentials) (store.Job, error)
	EnqueueContainerWithSourceAuth(ctx context.Context, account, container string, creds auth.Credentials) (store.Job, error)
	EnsureContainerWithSourceAuth(ctx context.Context, account, container string, creds auth.Credentials) (bool, error)
	CopyObjectNowWithSourceAuth(ctx context.Context, account, container, object string, creds auth.Credentials) error
}

type Handler struct {
	ceph                backend.Client
	swift               backend.Client
	migrator            Migrator
	headerForwarder     *auth.HeaderForwarder
	copyOnHeadMiss      bool
	scanOnContainerList bool
	logger              *slog.Logger
}

func NewHandler(
	ceph backend.Client,
	swift backend.Client,
	migrator Migrator,
	headerForwarder *auth.HeaderForwarder,
	copyOnHeadMiss bool,
	scanOnContainerList bool,
	logger *slog.Logger,
) *Handler {
	return &Handler{
		ceph:                ceph,
		swift:               swift,
		migrator:            migrator,
		headerForwarder:     headerForwarder,
		copyOnHeadMiss:      copyOnHeadMiss,
		scanOnContainerList: scanOnContainerList,
		logger:              logger,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parsed, err := ParseSwiftPath(r.URL.Path)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	switch parsed.Kind {
	case ResourceAccount:
		h.handleAccount(w, r, parsed)
	case ResourceContainer:
		h.handleContainer(w, r, parsed)
	case ResourceObject:
		h.handleObject(w, r, parsed)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleAccount(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	switch r.Method {
	case http.MethodGet:
		h.handleMergedListing(w, r, parsed)
	case http.MethodHead:
		h.proxyWith404Fallback(w, r, parsed)
	default:
		http.Error(w, "method not supported", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleContainer(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	switch r.Method {
	case http.MethodGet:
		h.handleMergedListing(w, r, parsed)
	case http.MethodHead:
		h.proxyWith404Fallback(w, r, parsed)
	case http.MethodPut:
		if !h.ensureContainerIfNeeded(w, r, parsed) {
			return
		}
		h.proxyToCeph(w, r, parsed)
	case http.MethodPost:
		if !h.ensureContainerIfNeeded(w, r, parsed) {
			return
		}
		h.proxyToCeph(w, r, parsed)
	case http.MethodDelete:
		h.handleContainerDelete(w, r, parsed)
	default:
		http.Error(w, "method not supported", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleObject(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		h.handleObjectRead(w, r, parsed)
	case http.MethodPut:
		if !h.ensureTargetContainer(w, r, parsed) {
			return
		}
		h.proxyToCeph(w, r, parsed)
	case http.MethodPost:
		if !h.ensureObjectPresentInCeph(w, r, parsed) {
			return
		}
		h.proxyToCeph(w, r, parsed)
	case http.MethodDelete:
		h.handleObjectDelete(w, r, parsed)
	default:
		http.Error(w, "method not supported", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) proxyWith404Fallback(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	headers := h.headerForwarder.CloneForProxy(r.Header)
	cephResp, err := h.ceph.Do(r.Context(), backend.Request{
		Method:  r.Method,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	})
	if err != nil {
		h.writeProxyError(w, parsed, "request ceph", err)
		return
	}
	if cephResp.StatusCode != http.StatusNotFound {
		writeUpstreamResponse(w, cephResp, r.Method == http.MethodHead)
		return
	}
	cephResp.Body.Close()

	swiftResp, err := h.swift.Do(r.Context(), backend.Request{
		Method:  r.Method,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	})
	if err != nil {
		h.writeProxyError(w, parsed, "request swift", err)
		return
	}
	writeUpstreamResponse(w, swiftResp, r.Method == http.MethodHead)
}

func (h *Handler) handleMergedListing(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	format := listingFormat(r.URL.Query())
	if format != "plain" && format != "json" {
		http.Error(w, "unsupported listing format", http.StatusNotImplemented)
		return
	}

	headers := h.headerForwarder.CloneForProxy(r.Header)
	sourceCreds, credErr := auth.CredentialsFromHeaders(r.Header)
	if credErr != nil {
		h.logger.Warn("failed to parse caller token expiry", "account", parsed.Account, "container", parsed.Container, "error", credErr)
	}
	query := backend.CloneQuery(r.URL.Query())

	cephResp, err := h.ceph.Do(r.Context(), backend.Request{
		Method:  http.MethodGet,
		Path:    parsed.BackendPath,
		Query:   query,
		Headers: headers,
	})
	if err != nil {
		h.writeProxyError(w, parsed, "request ceph listing", err)
		return
	}
	defer cephResp.Body.Close()

	if cephResp.StatusCode != http.StatusNotFound && !backend.IsSuccess(cephResp.StatusCode) {
		writeUpstreamResponse(w, cephResp, false)
		return
	}

	swiftResp, err := h.swift.Do(r.Context(), backend.Request{
		Method:  http.MethodGet,
		Path:    parsed.BackendPath,
		Query:   query,
		Headers: headers,
	})
	if err != nil {
		if backend.IsSuccess(cephResp.StatusCode) {
			writeUpstreamResponse(w, cephResp, false)
			return
		}
		h.writeProxyError(w, parsed, "request swift listing", err)
		return
	}
	defer swiftResp.Body.Close()

	if cephResp.StatusCode == http.StatusNotFound && swiftResp.StatusCode == http.StatusNotFound {
		writeUpstreamResponse(w, swiftResp, false)
		return
	}
	if backend.IsSuccess(cephResp.StatusCode) && swiftResp.StatusCode == http.StatusNotFound {
		writeUpstreamResponse(w, cephResp, false)
		return
	}
	if cephResp.StatusCode == http.StatusNotFound && backend.IsSuccess(swiftResp.StatusCode) {
		h.enqueueContainerScan(r.Context(), parsed, sourceCreds)
		writeUpstreamResponse(w, swiftResp, false)
		return
	}
	if !backend.IsSuccess(swiftResp.StatusCode) {
		writeUpstreamResponse(w, swiftResp, false)
		return
	}

	cephBody, err := io.ReadAll(cephResp.Body)
	if err != nil {
		h.writeProxyError(w, parsed, "read ceph listing", err)
		return
	}
	swiftBody, err := io.ReadAll(swiftResp.Body)
	if err != nil {
		h.writeProxyError(w, parsed, "read swift listing", err)
		return
	}

	mergedBody, contentType, err := mergeListingBodies(query, cephBody, swiftBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotImplemented)
		return
	}

	h.enqueueContainerScan(r.Context(), parsed, sourceCreds)
	copyHeaders(w.Header(), auth.FilterResponseHeaders(preferredHeader(cephResp, swiftResp)))
	w.Header().Set("Content-Type", contentType)
	w.Header().Del("Content-Length")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(mergedBody)
}

func (h *Handler) handleObjectRead(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	headers := h.headerForwarder.CloneForProxy(r.Header)
	sourceCreds, credErr := auth.CredentialsFromHeaders(r.Header)
	if credErr != nil {
		h.logger.Warn("failed to parse caller token expiry", "account", parsed.Account, "container", parsed.Container, "object", parsed.Object, "error", credErr)
	}
	req := backend.Request{
		Method:  r.Method,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	}

	cephResp, err := h.ceph.Do(r.Context(), req)
	if err != nil {
		h.writeProxyError(w, parsed, "request ceph object", err)
		return
	}
	if cephResp.StatusCode != http.StatusNotFound {
		writeUpstreamResponse(w, cephResp, r.Method == http.MethodHead)
		return
	}
	cephResp.Body.Close()

	swiftResp, err := h.swift.Do(r.Context(), req)
	if err != nil {
		h.writeProxyError(w, parsed, "request swift object", err)
		return
	}

	if backend.IsSuccess(swiftResp.StatusCode) && (r.Method == http.MethodGet || h.copyOnHeadMiss) {
		if sourceCreds.HasAuth() && sourceCreds.ExpiresAt == nil {
			h.logger.Warn("skipping queued object migration because caller token expiry is unavailable", "account", parsed.Account, "container", parsed.Container, "object", parsed.Object)
		} else if _, err := h.migrator.EnqueueObjectWithSourceAuth(r.Context(), parsed.Account, parsed.Container, parsed.Object, sourceCreds); err != nil {
			h.logger.Error("failed to enqueue object migration", "account", parsed.Account, "container", parsed.Container, "object", parsed.Object, "error", err)
		}
	}
	writeUpstreamResponse(w, swiftResp, r.Method == http.MethodHead)
}

func (h *Handler) ensureContainerIfNeeded(w http.ResponseWriter, r *http.Request, parsed SwiftPath) bool {
	sourceCreds, credErr := auth.CredentialsFromHeaders(r.Header)
	if credErr != nil {
		h.logger.Warn("failed to parse caller token expiry", "account", parsed.Account, "container", parsed.Container, "error", credErr)
	}
	exists, err := h.existsInBackend(r.Context(), h.ceph, backend.BuildContainerPath(parsed.Account, parsed.Container), r.Header)
	if err != nil {
		h.writeProxyError(w, parsed, "head ceph container", err)
		return false
	}
	if exists {
		return true
	}
	if _, err := h.migrator.EnsureContainerWithSourceAuth(r.Context(), parsed.Account, parsed.Container, sourceCreds); err != nil {
		h.writeProxyError(w, parsed, "ensure target container", err)
		return false
	}
	return true
}

func (h *Handler) ensureTargetContainer(w http.ResponseWriter, r *http.Request, parsed SwiftPath) bool {
	sourceCreds, credErr := auth.CredentialsFromHeaders(r.Header)
	if credErr != nil {
		h.logger.Warn("failed to parse caller token expiry", "account", parsed.Account, "container", parsed.Container, "error", credErr)
	}
	exists, err := h.existsInBackend(r.Context(), h.ceph, backend.BuildContainerPath(parsed.Account, parsed.Container), r.Header)
	if err != nil {
		h.writeProxyError(w, parsed, "head ceph container", err)
		return false
	}
	if exists {
		return true
	}
	if _, err := h.migrator.EnsureContainerWithSourceAuth(r.Context(), parsed.Account, parsed.Container, sourceCreds); err != nil {
		h.writeProxyError(w, parsed, "ensure target container", err)
		return false
	}
	return true
}

func (h *Handler) ensureObjectPresentInCeph(w http.ResponseWriter, r *http.Request, parsed SwiftPath) bool {
	sourceCreds, credErr := auth.CredentialsFromHeaders(r.Header)
	if credErr != nil {
		h.logger.Warn("failed to parse caller token expiry", "account", parsed.Account, "container", parsed.Container, "object", parsed.Object, "error", credErr)
	}
	exists, err := h.existsInBackend(r.Context(), h.ceph, parsed.BackendPath, r.Header)
	if err != nil {
		h.writeProxyError(w, parsed, "head ceph object", err)
		return false
	}
	if exists {
		return true
	}
	if err := h.migrator.CopyObjectNowWithSourceAuth(r.Context(), parsed.Account, parsed.Container, parsed.Object, sourceCreds); err != nil {
		if errors.Is(err, migration.ErrNotFound) {
			http.NotFound(w, r)
			return false
		}
		h.writeProxyError(w, parsed, "copy object into ceph", err)
		return false
	}
	return true
}

func (h *Handler) proxyToCeph(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	headers := h.headerForwarder.CloneForProxy(r.Header)
	resp, err := h.ceph.Do(r.Context(), backend.Request{
		Method:        r.Method,
		Path:          parsed.BackendPath,
		Query:         backend.CloneQuery(r.URL.Query()),
		Headers:       headers,
		Body:          r.Body,
		ContentLength: r.ContentLength,
	})
	if err != nil {
		h.writeProxyError(w, parsed, "proxy request to ceph", err)
		return
	}
	writeUpstreamResponse(w, resp, r.Method == http.MethodHead)
}

func (h *Handler) handleObjectDelete(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	headers := h.headerForwarder.CloneForProxy(r.Header)
	cephResp, cephErr := h.ceph.Do(r.Context(), backend.Request{
		Method:  http.MethodDelete,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	})
	if cephErr != nil {
		h.writeProxyError(w, parsed, "delete object from ceph", cephErr)
		return
	}
	defer cephResp.Body.Close()

	swiftResp, swiftErr := h.swift.Do(r.Context(), backend.Request{
		Method:  http.MethodDelete,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	})
	if swiftErr != nil {
		if backend.IsSuccess(cephResp.StatusCode) {
			writeUpstreamResponse(w, cephResp, false)
			return
		}
		h.writeProxyError(w, parsed, "delete object from swift", swiftErr)
		return
	}
	defer swiftResp.Body.Close()

	switch {
	case backend.IsSuccess(cephResp.StatusCode):
		writeUpstreamResponse(w, cephResp, false)
	case backend.IsSuccess(swiftResp.StatusCode):
		writeUpstreamResponse(w, swiftResp, false)
	case cephResp.StatusCode == http.StatusNotFound && swiftResp.StatusCode == http.StatusNotFound:
		writeUpstreamResponse(w, swiftResp, false)
	case cephResp.StatusCode != http.StatusNotFound:
		writeUpstreamResponse(w, cephResp, false)
	default:
		writeUpstreamResponse(w, swiftResp, false)
	}
}

func (h *Handler) handleContainerDelete(w http.ResponseWriter, r *http.Request, parsed SwiftPath) {
	headers := h.headerForwarder.CloneForProxy(r.Header)
	cephExists, cephEmpty, cephErr := h.containerEmpty(r.Context(), h.ceph, parsed.BackendPath, headers)
	if cephErr != nil {
		h.writeProxyError(w, parsed, "check ceph container contents", cephErr)
		return
	}
	swiftExists, swiftEmpty, swiftErr := h.containerEmpty(r.Context(), h.swift, parsed.BackendPath, headers)
	if swiftErr != nil {
		h.writeProxyError(w, parsed, "check swift container contents", swiftErr)
		return
	}
	if !cephEmpty || !swiftEmpty {
		http.Error(w, "container is not empty", http.StatusConflict)
		return
	}
	if !cephExists && !swiftExists {
		http.NotFound(w, r)
		return
	}

	cephResp, cephDeleteErr := h.ceph.Do(r.Context(), backend.Request{
		Method:  http.MethodDelete,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	})
	if cephDeleteErr != nil {
		h.writeProxyError(w, parsed, "delete container from ceph", cephDeleteErr)
		return
	}
	defer cephResp.Body.Close()

	swiftResp, swiftDeleteErr := h.swift.Do(r.Context(), backend.Request{
		Method:  http.MethodDelete,
		Path:    parsed.BackendPath,
		Query:   backend.CloneQuery(r.URL.Query()),
		Headers: headers,
	})
	if swiftDeleteErr != nil {
		if backend.IsSuccess(cephResp.StatusCode) {
			writeUpstreamResponse(w, cephResp, false)
			return
		}
		h.writeProxyError(w, parsed, "delete container from swift", swiftDeleteErr)
		return
	}
	defer swiftResp.Body.Close()

	switch {
	case backend.IsSuccess(cephResp.StatusCode):
		writeUpstreamResponse(w, cephResp, false)
	case backend.IsSuccess(swiftResp.StatusCode):
		writeUpstreamResponse(w, swiftResp, false)
	case cephResp.StatusCode != http.StatusNotFound:
		writeUpstreamResponse(w, cephResp, false)
	default:
		writeUpstreamResponse(w, swiftResp, false)
	}
}

func (h *Handler) existsInBackend(ctx context.Context, client backend.Client, path string, requestHeaders http.Header) (bool, error) {
	resp, err := client.Do(ctx, backend.Request{
		Method:  http.MethodHead,
		Path:    path,
		Headers: h.headerForwarder.CloneForProxy(requestHeaders),
	})
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	switch {
	case backend.IsSuccess(resp.StatusCode):
		return true, nil
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

func (h *Handler) containerEmpty(ctx context.Context, client backend.Client, path string, headers http.Header) (exists bool, empty bool, err error) {
	query := url.Values{
		"format": []string{"json"},
		"limit":  []string{"1"},
	}
	resp, err := client.Do(ctx, backend.Request{
		Method:  http.MethodGet,
		Path:    path,
		Query:   query,
		Headers: headers,
	})
	if err != nil {
		return false, false, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return false, true, nil
	case !backend.IsSuccess(resp.StatusCode):
		return false, false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, false, fmt.Errorf("read container listing: %w", err)
	}
	if listingFormat(query) == "json" && len(strings.TrimSpace(string(body))) > 0 {
		var items []map[string]any
		if err := json.Unmarshal(body, &items); err != nil {
			return false, false, fmt.Errorf("decode container listing: %w", err)
		}
		return true, len(items) == 0, nil
	}
	return true, len(strings.TrimSpace(string(body))) == 0, nil
}

func (h *Handler) enqueueContainerScan(ctx context.Context, parsed SwiftPath, creds auth.Credentials) {
	if !h.scanOnContainerList || parsed.Kind != ResourceContainer {
		return
	}
	if creds.HasAuth() && creds.ExpiresAt == nil {
		h.logger.Warn("skipping queued container scan because caller token expiry is unavailable", "account", parsed.Account, "container", parsed.Container)
		return
	}
	if _, err := h.migrator.EnqueueContainerWithSourceAuth(ctx, parsed.Account, parsed.Container, creds); err != nil {
		h.logger.Error("failed to enqueue container scan", "account", parsed.Account, "container", parsed.Container, "error", err)
	}
}

func (h *Handler) writeProxyError(w http.ResponseWriter, parsed SwiftPath, action string, err error) {
	h.logger.Error("proxy error", "action", action, "account", parsed.Account, "container", parsed.Container, "object", parsed.Object, "error", err)
	http.Error(w, "upstream request failed", http.StatusBadGateway)
}

func preferredHeader(cephResp, swiftResp *http.Response) http.Header {
	if cephResp != nil && backend.IsSuccess(cephResp.StatusCode) {
		return cephResp.Header
	}
	if swiftResp != nil {
		return swiftResp.Header
	}
	return nil
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func writeUpstreamResponse(w http.ResponseWriter, resp *http.Response, headOnly bool) {
	defer resp.Body.Close()
	copyHeaders(w.Header(), auth.FilterResponseHeaders(resp.Header))
	w.WriteHeader(resp.StatusCode)
	if headOnly {
		return
	}
	_, _ = io.Copy(w, resp.Body)
}
