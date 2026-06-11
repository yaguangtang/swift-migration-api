package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAMLAndJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	jsonPath := filepath.Join(dir, "config.json")

	yamlConfig := `
server:
  listen: ":8443"
backends:
  ceph:
    base_url: "https://ceph.example.com/swift/v1"
    timeout: 10s
  swift:
    base_url: "https://swift.example.com/v1"
    timeout: 11s
migration:
  queue_store: "./jobs.sqlite"
`
	jsonConfig := `{
  "server": {"listen": ":8081"},
  "backends": {
    "ceph": {"base_url": "https://ceph.example.com/swift/v1", "timeout": "12s"},
    "swift": {"base_url": "https://swift.example.com/v1", "timeout": "13s"}
  },
  "migration": {"queue_store": "./jobs.sqlite"}
}`

	if err := os.WriteFile(yamlPath, []byte(yamlConfig), 0o600); err != nil {
		t.Fatalf("write yaml config: %v", err)
	}
	if err := os.WriteFile(jsonPath, []byte(jsonConfig), 0o600); err != nil {
		t.Fatalf("write json config: %v", err)
	}

	yamlLoaded, err := Load(yamlPath)
	if err != nil {
		t.Fatalf("load yaml config: %v", err)
	}
	if yamlLoaded.Server.Listen != ":8443" {
		t.Fatalf("unexpected yaml listen addr: %s", yamlLoaded.Server.Listen)
	}
	if got := yamlLoaded.Auth.ForwardHeaders; len(got) != 2 || got[0] != "X-Auth-Token" {
		t.Fatalf("yaml defaults not applied: %#v", got)
	}

	jsonLoaded, err := Load(jsonPath)
	if err != nil {
		t.Fatalf("load json config: %v", err)
	}
	if jsonLoaded.Server.Listen != ":8081" {
		t.Fatalf("unexpected json listen addr: %s", jsonLoaded.Server.Listen)
	}
	if jsonLoaded.Migration.WorkerConcurrency != defaultWorkerConcurrency {
		t.Fatalf("json defaults not applied: %d", jsonLoaded.Migration.WorkerConcurrency)
	}
}

func TestValidateTLSRequiresCertAndKey(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Backends.Ceph.BaseURL = "https://ceph.example.com/swift/v1"
	cfg.Backends.Swift.BaseURL = "https://swift.example.com/v1"
	cfg.Migration.QueueStore = "./jobs.sqlite"
	cfg.Server.TLS.Enabled = true

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected tls validation error")
	}
}
