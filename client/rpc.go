package client

import (
	"context"
	"fmt"
)

// ExecuteTypedWorkflow executes a workflow with type-safe input and output
// This provides an RPC-like interface for workflow execution
func ExecuteTypedWorkflow[TInput any, TOutput any](
	ctx context.Context,
	c *Client,
	opts *WorkflowOptions,
	workflowFunc interface{},
	input TInput,
) (TOutput, error) {
	var result TOutput

	run, err := c.ExecuteWorkflow(ctx, opts, workflowFunc, input)
	if err != nil {
		return result, fmt.Errorf("failed to start workflow: %w", err)
	}

	err = run.Get(ctx, &result)
	if err != nil {
		return result, fmt.Errorf("workflow execution failed: %w", err)
	}

	return result, nil
}

// ExecuteTypedWorkflowAsync starts a workflow and returns a handle without waiting
func ExecuteTypedWorkflowAsync[TInput any, TOutput any](
	ctx context.Context,
	c *Client,
	opts *WorkflowOptions,
	workflowFunc interface{},
	input TInput,
) (*TypedWorkflowRun[TOutput], error) {
	run, err := c.ExecuteWorkflow(ctx, opts, workflowFunc, input)
	if err != nil {
		return nil, fmt.Errorf("failed to start workflow: %w", err)
	}

	return &TypedWorkflowRun[TOutput]{
		run: run,
	}, nil
}

// TypedWorkflowRun represents a running workflow with type-safe result retrieval
type TypedWorkflowRun[TOutput any] struct {
	run WorkflowRun
}

// GetID returns the workflow ID
func (r *TypedWorkflowRun[TOutput]) GetID() string {
	return r.run.GetID()
}

// GetRunID returns the run ID
func (r *TypedWorkflowRun[TOutput]) GetRunID() string {
	return r.run.GetRunID()
}

// Get waits for the workflow to complete and returns the typed result
func (r *TypedWorkflowRun[TOutput]) Get(ctx context.Context) (TOutput, error) {
	var result TOutput
	err := r.run.Get(ctx, &result)
	return result, err
}

// QueryTypedWorkflow queries a workflow with type-safe parameters and result
func QueryTypedWorkflow[TInput any, TOutput any](
	ctx context.Context,
	c *Client,
	workflowID string,
	runID string,
	queryType string,
	input TInput,
) (TOutput, error) {
	var result TOutput

	queryResult, err := c.QueryWorkflow(ctx, workflowID, runID, queryType, input)
	if err != nil {
		return result, fmt.Errorf("query failed: %w", err)
	}

	// Type assertion to extract the result
	if queryResult != nil {
		if typed, ok := queryResult.(TOutput); ok {
			return typed, nil
		}
		return result, fmt.Errorf("query result type mismatch")
	}

	return result, nil
}

// SignalTypedWorkflow sends a typed signal to a workflow
func SignalTypedWorkflow[TInput any](
	ctx context.Context,
	c *Client,
	workflowID string,
	runID string,
	signalName string,
	input TInput,
) error {
	return c.SignalWorkflow(ctx, workflowID, runID, signalName, input)
}

// WorkflowStub provides a type-safe interface for interacting with a workflow
type WorkflowStub[TInput any, TOutput any] struct {
	client       *Client
	workflowFunc interface{}
	opts         *WorkflowOptions
}

// NewWorkflowStub creates a new typed workflow stub
func NewWorkflowStub[TInput any, TOutput any](
	c *Client,
	workflowFunc interface{},
	opts *WorkflowOptions,
) *WorkflowStub[TInput, TOutput] {
	return &WorkflowStub[TInput, TOutput]{
		client:       c,
		workflowFunc: workflowFunc,
		opts:         opts,
	}
}

// Execute runs the workflow synchronously
func (s *WorkflowStub[TInput, TOutput]) Execute(ctx context.Context, input TInput) (TOutput, error) {
	return ExecuteTypedWorkflow[TInput, TOutput](ctx, s.client, s.opts, s.workflowFunc, input)
}

// ExecuteAsync runs the workflow asynchronously
func (s *WorkflowStub[TInput, TOutput]) ExecuteAsync(ctx context.Context, input TInput) (*TypedWorkflowRun[TOutput], error) {
	return ExecuteTypedWorkflowAsync[TInput, TOutput](ctx, s.client, s.opts, s.workflowFunc, input)
}

// Get retrieves an existing workflow execution
func (s *WorkflowStub[TInput, TOutput]) Get(ctx context.Context, workflowID, runID string) (*TypedWorkflowRun[TOutput], error) {
	run := s.client.GetWorkflow(ctx, workflowID, runID)
	return &TypedWorkflowRun[TOutput]{
		run: run,
	}, nil
}

// WorkflowClient provides a higher-level client with RPC-like methods
type WorkflowClient struct {
	underlying *Client
}

// NewWorkflowClient creates a new workflow client
func NewWorkflowClient(c *Client) *WorkflowClient {
	return &WorkflowClient{
		underlying: c,
	}
}

// GetUnderlying returns the underlying client
func (wc *WorkflowClient) GetUnderlying() *Client {
	return wc.underlying
}

// Close closes the underlying client
func (wc *WorkflowClient) Close() {
	wc.underlying.Close()
}

// Convenience function to create workflow client with defaults
func NewWorkflowClientWithDefaults(ctx context.Context) (*WorkflowClient, error) {
	c, err := NewWithDefaults(ctx)
	if err != nil {
		return nil, err
	}
	return NewWorkflowClient(c), nil
}

// Helper functions for WorkflowClient (use standalone functions instead of methods)

// CallWorkflow executes a workflow with type-safe parameters (RPC-style)
func CallWorkflow[TInput any, TOutput any](
	ctx context.Context,
	wc *WorkflowClient,
	opts *WorkflowOptions,
	workflowFunc interface{},
	input TInput,
) (TOutput, error) {
	return ExecuteTypedWorkflow[TInput, TOutput](ctx, wc.underlying, opts, workflowFunc, input)
}

// CallWorkflowAsync executes a workflow asynchronously
func CallWorkflowAsync[TInput any, TOutput any](
	ctx context.Context,
	wc *WorkflowClient,
	opts *WorkflowOptions,
	workflowFunc interface{},
	input TInput,
) (*TypedWorkflowRun[TOutput], error) {
	return ExecuteTypedWorkflowAsync[TInput, TOutput](ctx, wc.underlying, opts, workflowFunc, input)
}

// QueryWorkflow queries a workflow with type-safe parameters
func QueryWorkflow[TInput any, TOutput any](
	ctx context.Context,
	wc *WorkflowClient,
	workflowID string,
	runID string,
	queryType string,
	input TInput,
) (TOutput, error) {
	return QueryTypedWorkflow[TInput, TOutput](ctx, wc.underlying, workflowID, runID, queryType, input)
}

// SignalWorkflow sends a typed signal to a workflow
func SignalWorkflow[TInput any](
	ctx context.Context,
	wc *WorkflowClient,
	workflowID string,
	runID string,
	signalName string,
	input TInput,
) error {
	return SignalTypedWorkflow[TInput](ctx, wc.underlying, workflowID, runID, signalName, input)
}
