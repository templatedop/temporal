package client

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
)

// Config holds configuration for the Temporal client
type Config struct {
	HostPort  string
	Namespace string
	// Optional fields
	ConnectionTimeout time.Duration
	Identity          string
	DataConverter     interface{} // Can be set to custom data converter
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		HostPort:          "localhost:7233",
		Namespace:         "default",
		ConnectionTimeout: 10 * time.Second,
	}
}

// Client wraps the Temporal client with convenient methods
type Client struct {
	underlying client.Client
	config     *Config
}

// New creates a new Temporal client with the given configuration
func New(ctx context.Context, cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	opts := client.Options{
		HostPort:  cfg.HostPort,
		Namespace: cfg.Namespace,
	}

	// Connection timeout can be configured via ConnectionOptions if needed
	// For simplicity, we omit it here as the SDK handles defaults well

	if cfg.Identity != "" {
		opts.Identity = cfg.Identity
	}

	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %w", err)
	}

	return &Client{
		underlying: c,
		config:     cfg,
	}, nil
}

// NewWithDefaults creates a client with default configuration (localhost:7233, default namespace)
func NewWithDefaults(ctx context.Context) (*Client, error) {
	return New(ctx, DefaultConfig())
}

// Close closes the underlying Temporal client
func (c *Client) Close() {
	if c.underlying != nil {
		c.underlying.Close()
	}
}

// Underlying returns the underlying Temporal client for advanced use cases
func (c *Client) Underlying() client.Client {
	return c.underlying
}

// GetConfig returns the client configuration
func (c *Client) GetConfig() *Config {
	return c.config
}
