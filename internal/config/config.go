// Package config reads settings from environment variables (12-factor style), which is how
// Kubernetes passes ConfigMaps and Secrets into containers.
package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/crypto"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/engine"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
)

// Config holds every setting. See README for the full list with defaults.
type Config struct {
	DatabaseURL   string        // CBS_DATABASE_URL: catalog database
	VaultURL      string        // CBS_VAULT_URL: s3://bucket or file:///path
	S3Endpoint    string        // CBS_S3_ENDPOINT: e.g. http://localstack:4566; empty for AWS
	EncryptionKey string        // CBS_ENCRYPTION_KEY: 64 hex chars
	Retention     time.Duration // CBS_RETENTION: Object Lock period for new objects
	LockMode      vault.LockMode
	HTTPAddr      string        // CBS_HTTP_ADDR
	WorkerID      string        // CBS_WORKER_ID, default: hostname (the pod name in Kubernetes)
	Lease         time.Duration // CBS_LEASE
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func duration(key, def string) (time.Duration, error) {
	d, err := time.ParseDuration(env(key, def))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

// Load reads the environment. defaultAddr differs per binary so they can run side by side.
func Load(defaultAddr string) (*Config, error) {
	host, _ := os.Hostname()
	c := &Config{
		DatabaseURL:   os.Getenv("CBS_DATABASE_URL"),
		VaultURL:      env("CBS_VAULT_URL", "file://./vault"),
		S3Endpoint:    os.Getenv("CBS_S3_ENDPOINT"),
		EncryptionKey: os.Getenv("CBS_ENCRYPTION_KEY"),
		LockMode:      vault.LockMode(env("CBS_LOCK_MODE", string(vault.Governance))),
		HTTPAddr:      env("CBS_HTTP_ADDR", defaultAddr),
		WorkerID:      env("CBS_WORKER_ID", host),
	}
	var err error
	if c.Retention, err = duration("CBS_RETENTION", "720h"); err != nil {
		return nil, err
	}
	if c.Lease, err = duration("CBS_LEASE", "30s"); err != nil {
		return nil, err
	}
	return c, nil
}

// Engine builds the vault store and backup engine. With ensureBucket, an S3 vault bucket is
// created with Object Lock if missing (workers do this on start; the API only reads).
func (c *Config) Engine(ctx context.Context, ensureBucket bool) (*engine.Engine, error) {
	if c.EncryptionKey == "" {
		return nil, errors.New("CBS_ENCRYPTION_KEY is not set (generate one with: cbs keygen)")
	}
	key, err := crypto.ParseKey(c.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("CBS_ENCRYPTION_KEY: %w", err)
	}
	codec, err := crypto.NewCodec(key)
	if err != nil {
		return nil, err
	}
	store, err := vault.Open(ctx, c.VaultURL, c.S3Endpoint, c.Retention, c.LockMode)
	if err != nil {
		return nil, err
	}
	if s3, ok := store.(*vault.S3); ok && ensureBucket {
		if err := s3.EnsureBucket(ctx); err != nil {
			return nil, err
		}
	}
	return engine.New(store, codec), nil
}

// Logger returns a JSON logger: one JSON object per line, which log collectors (Loki,
// CloudWatch, Elasticsearch) index without custom parsing.
func Logger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
}
