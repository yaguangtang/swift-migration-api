package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"swift-migration-api/internal/config"
)

var hopByHopHeaders = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

type HeaderForwarder struct {
	allowedAuth map[string]struct{}
}

var tokenExpiryHeaders = []string{
	"X-Auth-Token-Expires-At",
	"X-Token-Expires-At",
	"X-Authorization-Expires-At",
}

func NewHeaderForwarder(allowed []string) *HeaderForwarder {
	set := make(map[string]struct{}, len(allowed))
	for _, header := range allowed {
		set[http.CanonicalHeaderKey(header)] = struct{}{}
	}
	return &HeaderForwarder{allowedAuth: set}
}

func (f *HeaderForwarder) CloneForProxy(in http.Header) http.Header {
	out := make(http.Header, len(in))
	for key, values := range in {
		canonical := http.CanonicalHeaderKey(key)
		if _, skip := hopByHopHeaders[canonical]; skip {
			continue
		}
		if isAuthHeader(canonical) {
			if _, ok := f.allowedAuth[canonical]; !ok {
				continue
			}
		}
		for _, value := range values {
			out.Add(key, value)
		}
	}
	return out
}

func CloneHeader(in http.Header) http.Header {
	out := make(http.Header, len(in))
	for key, values := range in {
		for _, value := range values {
			out.Add(key, value)
		}
	}
	return out
}

func FilterResponseHeaders(in http.Header) http.Header {
	out := make(http.Header, len(in))
	for key, values := range in {
		canonical := http.CanonicalHeaderKey(key)
		if _, skip := hopByHopHeaders[canonical]; skip {
			continue
		}
		for _, value := range values {
			out.Add(key, value)
		}
	}
	return out
}

type Credentials struct {
	AuthToken     string
	Authorization string
	ExpiresAt     *time.Time
}

func CredentialsFromHeaders(in http.Header) (Credentials, error) {
	creds := Credentials{
		AuthToken:     strings.TrimSpace(in.Get("X-Auth-Token")),
		Authorization: strings.TrimSpace(in.Get("Authorization")),
	}

	expiresAt, err := tokenExpiryFromHeaders(in)
	if err != nil {
		return creds, err
	}
	creds.ExpiresAt = expiresAt
	return creds, nil
}

func (c Credentials) HasAuth() bool {
	return c.AuthToken != "" || c.Authorization != ""
}

func (c Credentials) Headers() http.Header {
	headers := make(http.Header)
	if c.AuthToken != "" {
		headers.Set("X-Auth-Token", c.AuthToken)
	}
	if c.Authorization != "" {
		headers.Set("Authorization", c.Authorization)
	}
	return headers
}

func tokenExpiryFromHeaders(in http.Header) (*time.Time, error) {
	for _, header := range tokenExpiryHeaders {
		value := strings.TrimSpace(in.Get(header))
		if value == "" {
			continue
		}
		expiresAt, err := parseExpiry(value)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", header, err)
		}
		return &expiresAt, nil
	}
	return nil, nil
}

func parseExpiry(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported expiry format %q", value)
}

func isAuthHeader(name string) bool {
	switch name {
	case "Authorization", "X-Auth-Token":
		return true
	default:
		return false
	}
}

type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

type KeystoneTokenSource struct {
	client *http.Client
	cfg    config.WorkerKeystoneConfig

	mu      sync.Mutex
	token   string
	expires time.Time
}

func NewKeystoneTokenSource(client *http.Client, cfg config.WorkerKeystoneConfig) *KeystoneTokenSource {
	return &KeystoneTokenSource{
		client: client,
		cfg:    cfg,
	}
}

func (s *KeystoneTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token != "" && time.Until(s.expires) > time.Minute {
		return s.token, nil
	}

	body := map[string]any{
		"auth": map[string]any{
			"identity": map[string]any{
				"methods": []string{"application_credential"},
				"application_credential": map[string]any{
					"id":     s.cfg.ApplicationCredentialID,
					"secret": s.cfg.ApplicationCredentialSecret,
				},
			},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal keystone auth request: %w", err)
	}

	url := strings.TrimRight(s.cfg.AuthURL, "/")
	if !strings.HasSuffix(url, "/auth/tokens") {
		url += "/auth/tokens"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("create keystone auth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request keystone token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusCreated {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("keystone token request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	token := resp.Header.Get("X-Subject-Token")
	if token == "" {
		return "", fmt.Errorf("keystone token response missing X-Subject-Token")
	}

	var decoded struct {
		Token struct {
			ExpiresAt string `json:"expires_at"`
		} `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode keystone token response: %w", err)
	}

	expires, err := time.Parse(time.RFC3339, decoded.Token.ExpiresAt)
	if err != nil {
		return "", fmt.Errorf("parse keystone token expiry: %w", err)
	}

	s.token = token
	s.expires = expires
	return token, nil
}

func WorkerHeaders(ctx context.Context, tokenSource TokenSource) (http.Header, error) {
	headers := make(http.Header)
	if tokenSource == nil {
		return headers, nil
	}
	token, err := tokenSource.Token(ctx)
	if err != nil {
		return nil, err
	}
	headers.Set("X-Auth-Token", token)
	headers.Set("Authorization", "Bearer "+token)
	return headers, nil
}
