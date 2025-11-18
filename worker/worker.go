package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/worker"

	temporalClient "github.com/templatedop/temporal/client"
)

// Config holds worker configuration
type Config struct {
	TaskQueue                    string
	MaxConcurrentActivityExecutions int
	MaxConcurrentWorkflowExecutions int
	EnableLogging                   bool
}

// DefaultConfig returns worker configuration with sensible defaults
func DefaultConfig(taskQueue string) *Config {
	return &Config{
		TaskQueue:                    taskQueue,
		MaxConcurrentActivityExecutions: 100,
		MaxConcurrentWorkflowExecutions: 50,
		EnableLogging:                   true,
	}
}

// Worker wraps the Temporal worker with convenient methods
type Worker struct {
	underlying worker.Worker
	config     *Config
	client     *temporalClient.Client
}

// New creates a new worker with the given configuration
func New(client *temporalClient.Client, cfg *Config) (*Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("worker config cannot be nil")
	}

	if cfg.TaskQueue == "" {
		return nil, fmt.Errorf("task queue must be specified")
	}

	opts := worker.Options{}

	if cfg.MaxConcurrentActivityExecutions > 0 {
		opts.MaxConcurrentActivityExecutionSize = cfg.MaxConcurrentActivityExecutions
	}

	if cfg.MaxConcurrentWorkflowExecutions > 0 {
		opts.MaxConcurrentWorkflowTaskExecutionSize = cfg.MaxConcurrentWorkflowExecutions
	}

	w := worker.New(client.Underlying(), cfg.TaskQueue, opts)

	return &Worker{
		underlying: w,
		config:     cfg,
		client:     client,
	}, nil
}

// RegisterWorkflow registers a workflow function with the worker
func (w *Worker) RegisterWorkflow(workflowFunc interface{}) {
	w.underlying.RegisterWorkflow(workflowFunc)
}

// RegisterActivity registers an activity function with the worker
func (w *Worker) RegisterActivity(activityFunc interface{}) {
	w.underlying.RegisterActivity(activityFunc)
}

// Start starts the worker
func (w *Worker) Start() error {
	if w.config.EnableLogging {
		log.Printf("Starting worker on task queue: %s", w.config.TaskQueue)
	}

	err := w.underlying.Start()
	if err != nil {
		return fmt.Errorf("failed to start worker: %w", err)
	}

	return nil
}

// Stop stops the worker gracefully
func (w *Worker) Stop() {
	if w.config.EnableLogging {
		log.Printf("Stopping worker on task queue: %s", w.config.TaskQueue)
	}
	w.underlying.Stop()
}

// Run starts the worker and blocks until interrupted
func (w *Worker) Run(ctx context.Context) error {
	if err := w.Start(); err != nil {
		return err
	}

	// Wait for interrupt signal or context cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigCh:
		if w.config.EnableLogging {
			log.Println("Received interrupt signal, shutting down worker...")
		}
	case <-ctx.Done():
		if w.config.EnableLogging {
			log.Println("Context cancelled, shutting down worker...")
		}
	}

	w.Stop()
	return nil
}

// Builder provides a fluent interface for building workers
type Builder struct {
	client     *temporalClient.Client
	config     *Config
	workflows  []interface{}
	activities []interface{}
}

// NewBuilder creates a new worker builder
func NewBuilder(client *temporalClient.Client, taskQueue string) *Builder {
	return &Builder{
		client:     client,
		config:     DefaultConfig(taskQueue),
		workflows:  make([]interface{}, 0),
		activities: make([]interface{}, 0),
	}
}

// WithMaxConcurrentActivities sets the max concurrent activity executions
func (b *Builder) WithMaxConcurrentActivities(max int) *Builder {
	b.config.MaxConcurrentActivityExecutions = max
	return b
}

// WithMaxConcurrentWorkflows sets the max concurrent workflow executions
func (b *Builder) WithMaxConcurrentWorkflows(max int) *Builder {
	b.config.MaxConcurrentWorkflowExecutions = max
	return b
}

// WithLogging enables or disables logging
func (b *Builder) WithLogging(enabled bool) *Builder {
	b.config.EnableLogging = enabled
	return b
}

// RegisterWorkflow adds a workflow to be registered
func (b *Builder) RegisterWorkflow(workflowFunc interface{}) *Builder {
	b.workflows = append(b.workflows, workflowFunc)
	return b
}

// RegisterActivity adds an activity to be registered
func (b *Builder) RegisterActivity(activityFunc interface{}) *Builder {
	b.activities = append(b.activities, activityFunc)
	return b
}

// Build creates the worker with all registered workflows and activities
func (b *Builder) Build() (*Worker, error) {
	w, err := New(b.client, b.config)
	if err != nil {
		return nil, err
	}

	// Register all workflows
	for _, wf := range b.workflows {
		w.RegisterWorkflow(wf)
	}

	// Register all activities
	for _, act := range b.activities {
		w.RegisterActivity(act)
	}

	return w, nil
}

// BuildAndRun creates the worker and runs it (blocking until interrupted)
func (b *Builder) BuildAndRun(ctx context.Context) error {
	w, err := b.Build()
	if err != nil {
		return err
	}

	return w.Run(ctx)
}
