package auth

import (
	"net/http"
	"testing"
	"time"
)

func TestCredentialsFromHeadersParsesExpiry(t *testing.T) {
	t.Parallel()

	headers := make(http.Header)
	headers.Set("X-Auth-Token", "token-123")
	headers.Set("Authorization", "Bearer token-123")
	headers.Set("X-Auth-Token-Expires-At", "2030-01-02T03:04:05Z")

	creds, err := CredentialsFromHeaders(headers)
	if err != nil {
		t.Fatalf("parse credentials: %v", err)
	}
	if !creds.HasAuth() {
		t.Fatal("expected auth credentials")
	}
	if creds.ExpiresAt == nil {
		t.Fatal("expected token expiry")
	}
	if got := creds.ExpiresAt.UTC().Format(time.RFC3339); got != "2030-01-02T03:04:05Z" {
		t.Fatalf("unexpected expiry: %s", got)
	}
}

func TestCredentialsFromHeadersRejectsInvalidExpiry(t *testing.T) {
	t.Parallel()

	headers := make(http.Header)
	headers.Set("X-Auth-Token", "token-123")
	headers.Set("X-Auth-Token-Expires-At", "not-a-time")

	if _, err := CredentialsFromHeaders(headers); err == nil {
		t.Fatal("expected expiry parse error")
	}
}
