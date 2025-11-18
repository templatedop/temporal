package batch

import (
	"context"
	"fmt"

	"go.temporal.io/api/workflowservice/v1"

	temporalclient "github.com/templatedop/temporal/client"
)

// BatchOperation represents a batch operation to be performed on workflows
type BatchOperation string

const (
	// BatchOperationTerminate terminates all workflows matching the query
	BatchOperationTerminate BatchOperation = "terminate"
	// BatchOperationCancel cancels all workflows matching the query
	BatchOperationCancel BatchOperation = "cancel"
	// BatchOperationSignal sends a signal to all workflows matching the query
	BatchOperationSignal BatchOperation = "signal"
)

// BatchBuilder provides a fluent interface for building batch operations
type BatchBuilder struct {
	client    *temporalclient.Client
	query     string
	operation BatchOperation
	reason    string
	signalName string
	signalArgs []interface{}
	namespace string
}

// NewBatchBuilder creates a new batch operation builder
func NewBatchBuilder(c *temporalclient.Client) *BatchBuilder {
	return &BatchBuilder{
		client:    c,
		namespace: c.Namespace(),
	}
}

// Query sets the workflow query to select which workflows to operate on
// Examples:
//   - "WorkflowType='OrderWorkflow' AND ExecutionStatus='Running'"
//   - "WorkflowType='ClaimWorkflow' AND ClaimStatus='pending'"
func (bb *BatchBuilder) Query(query string) *BatchBuilder {
	bb.query = query
	return bb
}

// WorkflowType is a convenience method to query by workflow type
func (bb *BatchBuilder) WorkflowType(workflowType string) *BatchBuilder {
	if bb.query == "" {
		bb.query = fmt.Sprintf("WorkflowType='%s'", workflowType)
	} else {
		bb.query = fmt.Sprintf("%s AND WorkflowType='%s'", bb.query, workflowType)
	}
	return bb
}

// ExecutionStatus is a convenience method to filter by execution status
// Valid statuses: Running, Completed, Failed, Canceled, Terminated, TimedOut
func (bb *BatchBuilder) ExecutionStatus(status string) *BatchBuilder {
	if bb.query == "" {
		bb.query = fmt.Sprintf("ExecutionStatus='%s'", status)
	} else {
		bb.query = fmt.Sprintf("%s AND ExecutionStatus='%s'", bb.query, status)
	}
	return bb
}

// SearchAttribute adds a search attribute filter to the query
func (bb *BatchBuilder) SearchAttribute(key, value string) *BatchBuilder {
	condition := fmt.Sprintf("%s='%s'", key, value)
	if bb.query == "" {
		bb.query = condition
	} else {
		bb.query = fmt.Sprintf("%s AND %s", bb.query, condition)
	}
	return bb
}

// Namespace sets a custom namespace (defaults to client's namespace)
func (bb *BatchBuilder) Namespace(namespace string) *BatchBuilder {
	bb.namespace = namespace
	return bb
}

// Terminate configures a batch terminate operation
func (bb *BatchBuilder) Terminate(reason string) *BatchBuilder {
	bb.operation = BatchOperationTerminate
	bb.reason = reason
	return bb
}

// Cancel configures a batch cancel operation
func (bb *BatchBuilder) Cancel(reason string) *BatchBuilder {
	bb.operation = BatchOperationCancel
	bb.reason = reason
	return bb
}

// Signal configures a batch signal operation
func (bb *BatchBuilder) Signal(signalName string, args ...interface{}) *BatchBuilder {
	bb.operation = BatchOperationSignal
	bb.signalName = signalName
	bb.signalArgs = args
	return bb
}

// Execute executes the batch operation
func (bb *BatchBuilder) Execute(ctx context.Context) error {
	if bb.query == "" {
		return fmt.Errorf("query is required for batch operations")
	}

	if bb.operation == "" {
		return fmt.Errorf("operation (Terminate, Cancel, or Signal) is required")
	}

	switch bb.operation {
	case BatchOperationTerminate:
		return bb.executeTerminate(ctx)
	case BatchOperationCancel:
		return bb.executeCancel(ctx)
	case BatchOperationSignal:
		return bb.executeSignal(ctx)
	default:
		return fmt.Errorf("unknown batch operation: %s", bb.operation)
	}
}

func (bb *BatchBuilder) executeTerminate(ctx context.Context) error {
	// Use list + iterate pattern for batch operations
	// Temporal doesn't have a native batch terminate API yet
	resp, err := bb.client.Underlying().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: bb.namespace,
		Query:     bb.query,
	})
	if err != nil {
		return fmt.Errorf("failed to list workflows: %w", err)
	}

	executions := resp.Executions

	count := 0
	for _, exec := range executions {
		err = bb.client.Underlying().TerminateWorkflow(
			ctx,
			exec.Execution.WorkflowId,
			exec.Execution.RunId,
			bb.reason,
		)
		if err != nil {
			return fmt.Errorf("failed to terminate workflow %s: %w", exec.Execution.WorkflowId, err)
		}
		count++
	}

	fmt.Printf("Terminated %d workflows\n", count)
	return nil
}

func (bb *BatchBuilder) executeCancel(ctx context.Context) error {
	resp, err := bb.client.Underlying().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: bb.namespace,
		Query:     bb.query,
	})
	if err != nil {
		return fmt.Errorf("failed to list workflows: %w", err)
	}

	count := 0
	for _, exec := range resp.Executions {
		err = bb.client.Underlying().CancelWorkflow(
			ctx,
			exec.Execution.WorkflowId,
			exec.Execution.RunId,
		)
		if err != nil {
			return fmt.Errorf("failed to cancel workflow %s: %w", exec.Execution.WorkflowId, err)
		}
		count++
	}

	fmt.Printf("Cancelled %d workflows\n", count)
	return nil
}

func (bb *BatchBuilder) executeSignal(ctx context.Context) error {
	if bb.signalName == "" {
		return fmt.Errorf("signal name is required for batch signal operations")
	}

	resp, err := bb.client.Underlying().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: bb.namespace,
		Query:     bb.query,
	})
	if err != nil {
		return fmt.Errorf("failed to list workflows: %w", err)
	}

	count := 0
	for _, exec := range resp.Executions {
		// SignalWorkflow doesn't take variadic args in the SDK
		var arg interface{}
		if len(bb.signalArgs) > 0 {
			arg = bb.signalArgs[0]
		}

		err = bb.client.Underlying().SignalWorkflow(
			ctx,
			exec.Execution.WorkflowId,
			exec.Execution.RunId,
			bb.signalName,
			arg,
		)
		if err != nil {
			return fmt.Errorf("failed to signal workflow %s: %w", exec.Execution.WorkflowId, err)
		}
		count++
	}

	fmt.Printf("Signaled %d workflows\n", count)
	return nil
}

// ExecuteWithProgress executes the batch operation with progress reporting
func (bb *BatchBuilder) ExecuteWithProgress(ctx context.Context, progressFn func(processed, total int)) error {
	if bb.query == "" {
		return fmt.Errorf("query is required for batch operations")
	}

	// First, count total workflows
	resp, err := bb.client.Underlying().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: bb.namespace,
		Query:     bb.query,
	})
	if err != nil {
		return fmt.Errorf("failed to list workflows: %w", err)
	}

	var workflows []struct {
		workflowID string
		runID      string
	}

	for _, exec := range resp.Executions {
		workflows = append(workflows, struct {
			workflowID string
			runID      string
		}{exec.Execution.WorkflowId, exec.Execution.RunId})
	}

	total := len(workflows)
	if progressFn != nil {
		progressFn(0, total)
	}

	// Execute operation on each workflow
	for i, wf := range workflows {
		var opErr error
		switch bb.operation {
		case BatchOperationTerminate:
			opErr = bb.client.Underlying().TerminateWorkflow(ctx, wf.workflowID, wf.runID, bb.reason)
		case BatchOperationCancel:
			opErr = bb.client.Underlying().CancelWorkflow(ctx, wf.workflowID, wf.runID)
		case BatchOperationSignal:
			var arg interface{}
			if len(bb.signalArgs) > 0 {
				arg = bb.signalArgs[0]
			}
			opErr = bb.client.Underlying().SignalWorkflow(ctx, wf.workflowID, wf.runID, bb.signalName, arg)
		}

		if opErr != nil {
			return fmt.Errorf("failed to execute operation on workflow %s: %w", wf.workflowID, opErr)
		}

		if progressFn != nil {
			progressFn(i+1, total)
		}
	}

	return nil
}

// DryRun lists workflows that would be affected without performing the operation
func (bb *BatchBuilder) DryRun(ctx context.Context) ([]string, error) {
	if bb.query == "" {
		return nil, fmt.Errorf("query is required")
	}

	resp, err := bb.client.Underlying().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: bb.namespace,
		Query:     bb.query,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	var workflowIDs []string
	for _, exec := range resp.Executions {
		workflowIDs = append(workflowIDs, exec.Execution.WorkflowId)
	}

	return workflowIDs, nil
}

// Common batch operation helpers

// TerminateAll terminates all workflows of a given type
func TerminateAll(ctx context.Context, c *temporalclient.Client, workflowType, reason string) error {
	return NewBatchBuilder(c).
		WorkflowType(workflowType).
		ExecutionStatus("Running").
		Terminate(reason).
		Execute(ctx)
}

// CancelAll cancels all workflows of a given type
func CancelAll(ctx context.Context, c *temporalclient.Client, workflowType, reason string) error {
	return NewBatchBuilder(c).
		WorkflowType(workflowType).
		ExecutionStatus("Running").
		Cancel(reason).
		Execute(ctx)
}

// SignalAll sends a signal to all workflows of a given type
func SignalAll(ctx context.Context, c *temporalclient.Client, workflowType, signalName string, args ...interface{}) error {
	return NewBatchBuilder(c).
		WorkflowType(workflowType).
		ExecutionStatus("Running").
		Signal(signalName, args...).
		Execute(ctx)
}

// TerminateBySearchAttribute terminates workflows matching a search attribute
func TerminateBySearchAttribute(ctx context.Context, c *temporalclient.Client, key, value, reason string) error {
	return NewBatchBuilder(c).
		ExecutionStatus("Running").
		SearchAttribute(key, value).
		Terminate(reason).
		Execute(ctx)
}
