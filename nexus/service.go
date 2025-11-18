package nexus

import (
	"context"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/workflow"
)

// ServiceBuilder provides a fluent interface for building Nexus services
type ServiceBuilder struct {
	name       string
	operations []nexus.RegisterableOperation
}

// NewServiceBuilder creates a new Nexus service builder
func NewServiceBuilder(name string) *ServiceBuilder {
	return &ServiceBuilder{
		name:       name,
		operations: make([]nexus.RegisterableOperation, 0),
	}
}

// RegisterSyncOperation registers a synchronous operation that completes immediately
func (sb *ServiceBuilder) RegisterSyncOperation(
	name string,
	handler func(ctx context.Context, input any) (any, error),
) *ServiceBuilder {
	op := nexus.NewSyncOperation(name, func(ctx context.Context, input any, options nexus.StartOperationOptions) (any, error) {
		return handler(ctx, input)
	})
	sb.operations = append(sb.operations, op)
	return sb
}

// RegisterWorkflowOperation registers an operation that executes a Temporal workflow
func (sb *ServiceBuilder) RegisterWorkflowOperation(
	name string,
	workflowFunc interface{},
) *ServiceBuilder {
	// This would be implemented with a proper workflow operation handler
	// For now, we'll provide a placeholder that shows the pattern
	op := nexus.NewSyncOperation(name, func(ctx context.Context, input any, options nexus.StartOperationOptions) (any, error) {
		return nil, fmt.Errorf("workflow operations require full Temporal integration - see examples")
	})
	sb.operations = append(sb.operations, op)
	return sb
}

// Build creates the Nexus service
func (sb *ServiceBuilder) Build() *nexus.Service {
	return nexus.NewService(sb.name)
}

// BuildWithOperations creates the Nexus service with all registered operations
func (sb *ServiceBuilder) BuildWithOperations() (*nexus.Service, []nexus.RegisterableOperation) {
	return nexus.NewService(sb.name), sb.operations
}

// NexusClient provides a convenient wrapper around workflow.NexusClient
type NexusClient struct {
	client workflow.NexusClient
}

// NewClient creates a new Nexus client for calling operations from workflows
func NewClient(endpoint, service string) *NexusClient {
	return &NexusClient{
		client: workflow.NewNexusClient(endpoint, service),
	}
}

// ExecuteOperation executes a Nexus operation from a workflow
func (nc *NexusClient) ExecuteOperation(
	ctx workflow.Context,
	operationName string,
	input any,
	opts ...NexusOperationOption,
) workflow.NexusOperationFuture {
	options := workflow.NexusOperationOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	return nc.client.ExecuteOperation(ctx, operationName, input, options)
}

// ExecuteOperationSync executes a Nexus operation and waits for the result
func (nc *NexusClient) ExecuteOperationSync(
	ctx workflow.Context,
	operationName string,
	input any,
	result any,
	opts ...NexusOperationOption,
) error {
	future := nc.ExecuteOperation(ctx, operationName, input, opts...)
	return future.Get(ctx, result)
}

// NexusOperationOption is a functional option for configuring Nexus operations
type NexusOperationOption func(*workflow.NexusOperationOptions)

// WithScheduleToCloseTimeout sets the end-to-end timeout for the operation
func WithScheduleToCloseTimeout(timeout time.Duration) NexusOperationOption {
	return func(opts *workflow.NexusOperationOptions) {
		opts.ScheduleToCloseTimeout = timeout
	}
}

// WithOperationSummary sets a summary for the operation (appears in UI/CLI)
func WithOperationSummary(summary string) NexusOperationOption {
	return func(opts *workflow.NexusOperationOptions) {
		opts.Summary = summary
	}
}

// OperationBuilder provides a fluent interface for building Nexus operations
type OperationBuilder[I any, O any] struct {
	name    string
	handler func(context.Context, I) (O, error)
}

// NewOperation creates a new operation builder
func NewOperation[I any, O any](name string) *OperationBuilder[I, O] {
	return &OperationBuilder[I, O]{
		name: name,
	}
}

// WithHandler sets the operation handler
func (ob *OperationBuilder[I, O]) WithHandler(handler func(context.Context, I) (O, error)) *OperationBuilder[I, O] {
	ob.handler = handler
	return ob
}

// BuildSync builds a synchronous operation
func (ob *OperationBuilder[I, O]) BuildSync() nexus.Operation[I, O] {
	return nexus.NewSyncOperation(ob.name, func(ctx context.Context, input I, options nexus.StartOperationOptions) (O, error) {
		return ob.handler(ctx, input)
	})
}

// Common operation patterns

// CallServiceOperation is a helper for calling a Nexus operation from a workflow
func CallServiceOperation[I any, O any](
	ctx workflow.Context,
	endpoint string,
	service string,
	operation string,
	input I,
	opts ...NexusOperationOption,
) (O, error) {
	client := NewClient(endpoint, service)
	var result O
	err := client.ExecuteOperationSync(ctx, operation, input, &result, opts...)
	return result, err
}
