package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen            = ":8080"
	defaultBackendTimeout    = 120 * time.Second
	defaultReadTimeout       = 60 * time.Second
	defaultWorkerConcurrency = 4
	defaultPollInterval      = 2 * time.Second
	defaultRetryBase         = 2 * time.Second
	defaultMaxAttempts       = 5
)

type Config struct {
	Server    ServerConfig    `json:"server" yaml:"server"`
	Backends  BackendsConfig  `json:"backends" yaml:"backends"`
	Auth      AuthConfig      `json:"auth" yaml:"auth"`
	Migration MigrationConfig `json:"migration" yaml:"migration"`
	Admin     AdminConfig     `json:"admin" yaml:"admin"`
}

type ServerConfig struct {
	Listen       string    `json:"listen" yaml:"listen"`
	ReadTimeout  Duration  `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout Duration  `json:"write_timeout" yaml:"write_timeout"`
	TLS          TLSConfig `json:"tls" yaml:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	CertFile string `json:"cert_file" yaml:"cert_file"`
	KeyFile  string `json:"key_file" yaml:"key_file"`
}

type BackendsConfig struct {
	Ceph  BackendConfig `json:"ceph" yaml:"ceph"`
	Swift BackendConfig `json:"swift" yaml:"swift"`
}

type BackendConfig struct {
	BaseURL string   `json:"base_url" yaml:"base_url"`
	Timeout Duration `json:"timeout" yaml:"timeout"`
}

type AuthConfig struct {
	ForwardHeaders []string             `json:"forward_headers" yaml:"forward_headers"`
	WorkerKeystone WorkerKeystoneConfig `json:"worker_keystone" yaml:"worker_keystone"`
}

type WorkerKeystoneConfig struct {
	AuthURL                     string `json:"auth_url" yaml:"auth_url"`
	ApplicationCredentialID     string `json:"application_credential_id" yaml:"application_credential_id"`
	ApplicationCredentialSecret string `json:"application_credential_secret" yaml:"application_credential_secret"`
	Region                      string `json:"region" yaml:"region"`
	Interface                   string `json:"interface" yaml:"interface"`
}

type MigrationConfig struct {
	Mode                  string   `json:"mode" yaml:"mode"`
	QueueStore            string   `json:"queue_store" yaml:"queue_store"`
	WorkerConcurrency     int      `json:"worker_concurrency" yaml:"worker_concurrency"`
	CopyOnHeadMiss        bool     `json:"copy_on_head_miss" yaml:"copy_on_head_miss"`
	ScanOnContainerList   bool     `json:"scan_on_container_list" yaml:"scan_on_container_list"`
	VerifyETag            bool     `json:"verify_etag" yaml:"verify_etag"`
	DeleteSourceAfterCopy bool     `json:"delete_source_after_copy" yaml:"delete_source_after_copy"`
	PollInterval          Duration `json:"poll_interval" yaml:"poll_interval"`
	RetryBase             Duration `json:"retry_base" yaml:"retry_base"`
	MaxAttempts           int      `json:"max_attempts" yaml:"max_attempts"`
}

type AdminConfig struct {
	AuthToken string `json:"auth_token" yaml:"auth_token"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	cfg := defaultConfig()
	if err := decode(path, data, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Listen:       defaultListen,
			ReadTimeout:  Duration(defaultReadTimeout),
			WriteTimeout: 0,
		},
		Backends: BackendsConfig{
			Ceph: BackendConfig{
				Timeout: Duration(defaultBackendTimeout),
			},
			Swift: BackendConfig{
				Timeout: Duration(defaultBackendTimeout),
			},
		},
		Auth: AuthConfig{
			ForwardHeaders: []string{"X-Auth-Token", "Authorization"},
		},
		Migration: MigrationConfig{
			Mode:                "lazy_queue",
			WorkerConcurrency:   defaultWorkerConcurrency,
			CopyOnHeadMiss:      true,
			ScanOnContainerList: true,
			VerifyETag:          true,
			PollInterval:        Duration(defaultPollInterval),
			RetryBase:           Duration(defaultRetryBase),
			MaxAttempts:         defaultMaxAttempts,
		},
	}
}

func decode(path string, data []byte, cfg *Config) error {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode json config: %w", err)
		}
		return nil
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode yaml config: %w", err)
		}
		return nil
	}

	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") {
		if err := json.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode json config: %w", err)
		}
		return nil
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("decode yaml config: %w", err)
	}
	return nil
}

func (c Config) Validate() error {
	if c.Backends.Ceph.BaseURL == "" {
		return errors.New("backends.ceph.base_url is required")
	}
	if c.Backends.Swift.BaseURL == "" {
		return errors.New("backends.swift.base_url is required")
	}
	if c.Server.Listen == "" {
		return errors.New("server.listen is required")
	}
	if c.Server.TLS.Enabled {
		if c.Server.TLS.CertFile == "" || c.Server.TLS.KeyFile == "" {
			return errors.New("server.tls.cert_file and server.tls.key_file are required when TLS is enabled")
		}
	}
	if c.Backends.Ceph.Timeout.Std() <= 0 {
		return errors.New("backends.ceph.timeout must be positive")
	}
	if c.Backends.Swift.Timeout.Std() <= 0 {
		return errors.New("backends.swift.timeout must be positive")
	}
	if c.Migration.WorkerConcurrency <= 0 {
		return errors.New("migration.worker_concurrency must be positive")
	}
	if c.Migration.PollInterval.Std() <= 0 {
		return errors.New("migration.poll_interval must be positive")
	}
	if c.Migration.RetryBase.Std() <= 0 {
		return errors.New("migration.retry_base must be positive")
	}
	if c.Migration.MaxAttempts <= 0 {
		return errors.New("migration.max_attempts must be positive")
	}
	if c.Migration.Mode == "" {
		return errors.New("migration.mode is required")
	}
	if c.Migration.QueueStore == "" {
		return errors.New("migration.queue_store is required")
	}
	return nil
}

type Duration time.Duration

func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*d = 0
		return nil
	}

	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		parsed, err := time.ParseDuration(asString)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
		return nil
	}

	var asInt int64
	if err := json.Unmarshal(data, &asInt); err != nil {
		return err
	}
	*d = Duration(time.Duration(asInt))
	return nil
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar")
	}

	parsed, err := time.ParseDuration(node.Value)
	if err == nil {
		*d = Duration(parsed)
		return nil
	}

	var asInt int64
	if err := node.Decode(&asInt); err != nil {
		return err
	}
	*d = Duration(time.Duration(asInt))
	return nil
}
