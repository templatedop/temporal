package client

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// WorkflowOptions holds options for workflow execution
type WorkflowOptions struct {
	ID                    string
	TaskQueue             string
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	RetryPolicy              *RetryPolicy
	CronSchedule          string
	Memo                  map[string]interface{}
	SearchAttributes      map[string]interface{}
}

// RetryPolicy defines retry behavior for workflows
type RetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
}

// ExecuteWorkflow starts a workflow execution with the given options
func (c *Client) ExecuteWorkflow(ctx context.Context, opts *WorkflowOptions, workflow interface{}, args ...interface{}) (WorkflowRun, error) {
	if opts == nil {
		return nil, fmt.Errorf("workflow options cannot be nil")
	}

	if opts.TaskQueue == "" {
		return nil, fmt.Errorf("task queue must be specified")
	}

	startOpts := client.StartWorkflowOptions{
		ID:        opts.ID,
		TaskQueue: opts.TaskQueue,
	}

	if opts.WorkflowExecutionTimeout > 0 {
		startOpts.WorkflowExecutionTimeout = opts.WorkflowExecutionTimeout
	}

	if opts.WorkflowRunTimeout > 0 {
		startOpts.WorkflowRunTimeout = opts.WorkflowRunTimeout
	}

	if opts.WorkflowTaskTimeout > 0 {
		startOpts.WorkflowTaskTimeout = opts.WorkflowTaskTimeout
	}

	if opts.RetryPolicy != nil {
		startOpts.RetryPolicy = &temporal.RetryPolicy{
			InitialInterval:    opts.RetryPolicy.InitialInterval,
			BackoffCoefficient: opts.RetryPolicy.BackoffCoefficient,
			MaximumInterval:    opts.RetryPolicy.MaximumInterval,
			MaximumAttempts:    opts.RetryPolicy.MaximumAttempts,
		}
	}

	if opts.CronSchedule != "" {
		startOpts.CronSchedule = opts.CronSchedule
	}

	if opts.Memo != nil {
		startOpts.Memo = opts.Memo
	}

	if opts.SearchAttributes != nil {
		startOpts.SearchAttributes = opts.SearchAttributes
	}

	run, err := c.underlying.ExecuteWorkflow(ctx, startOpts, workflow, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute workflow: %w", err)
	}

	return &workflowRun{underlying: run}, nil
}

// WorkflowRun represents a running workflow
type WorkflowRun interface {
	GetID() string
	GetRunID() string
	Get(ctx context.Context, valuePtr interface{}) error
}

type workflowRun struct {
	underlying client.WorkflowRun
}

func (w *workflowRun) GetID() string {
	return w.underlying.GetID()
}

func (w *workflowRun) GetRunID() string {
	return w.underlying.GetRunID()
}

func (w *workflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return w.underlying.Get(ctx, valuePtr)
}

// GetWorkflow retrieves an existing workflow execution
func (c *Client) GetWorkflow(ctx context.Context, workflowID string, runID string) WorkflowRun {
	run := c.underlying.GetWorkflow(ctx, workflowID, runID)
	return &workflowRun{underlying: run}
}

// QueryWorkflow queries a running workflow
func (c *Client) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (interface{}, error) {
	resp, err := c.underlying.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow: %w", err)
	}

	var result interface{}
	if err := resp.Get(&result); err != nil {
		return nil, fmt.Errorf("failed to get query result: %w", err)
	}

	return result, nil
}

// SignalWorkflow sends a signal to a running workflow
func (c *Client) SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error {
	err := c.underlying.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
	if err != nil {
		return fmt.Errorf("failed to signal workflow: %w", err)
	}
	return nil
}

// CancelWorkflow cancels a running workflow
func (c *Client) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	err := c.underlying.CancelWorkflow(ctx, workflowID, runID)
	if err != nil {
		return fmt.Errorf("failed to cancel workflow: %w", err)
	}
	return nil
}

// TerminateWorkflow terminates a running workflow
func (c *Client) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	err := c.underlying.TerminateWorkflow(ctx, workflowID, runID, reason, details...)
	if err != nil {
		return fmt.Errorf("failed to terminate workflow: %w", err)
	}
	return nil
}
