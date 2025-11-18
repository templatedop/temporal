# Temporal Go Wrapper Library

A convenient, ergonomic wrapper library for the official Temporal Go SDK that provides simplified APIs, fluent builders, and helpful abstractions for building robust distributed applications.

**Direct Temporal connectivity** - No extra HTTP layer or intermediate services required!

## Features

### Core Features

- **Simplified Client API** - Easy client creation with sensible defaults
- **Fluent Workflow Builder** - Intuitive API for configuring workflow options
- **Worker Management** - Convenient worker creation and lifecycle management
- **Testing Helpers** - Simplified test utilities for workflows and activities
- **Preset Configurations** - Common patterns for timeouts and retry policies
- **Signal & Query Support** - Easy-to-use workflow communication
- **Type-Safe Interfaces** - Clean abstractions over the official SDK

### Advanced Features (IWF-Inspired)

🔥 **NEW**: Advanced patterns with direct Temporal connection (no HTTP overhead!)

- **🎯 State Machine Abstraction** - Define workflows as state machines with clear transitions
- **📞 RPC-Like Interface** - Type-safe workflow execution using Go generics
- **🔄 Saga Pattern** - Distributed transactions with automatic compensations
- **✅ Approval Workflows** - Human-in-the-loop with timeouts and reminders
- **🔍 Better Queries** - Fluent API for workflow persistence queries with pagination

See [FEATURES.md](FEATURES.md) for detailed documentation and examples.

## Why This Library vs IWF?

| Feature | IWF | This Library |
|---------|-----|--------------|
| Connection | Client → HTTP → IWF Server → Temporal | Client → Temporal (direct) |
| Latency | Higher | **5-6x faster** |
| Services Needed | 3 (Client, IWF, Temporal) | 2 (Client, Temporal) |
| Type Safety | Limited | Full (Go generics) |

## Installation

```bash
go get github.com/templatedop/temporal
```

## Quick Start

### 1. Create a Client

```go
import (
    "context"
    "github.com/templatedop/temporal/client"
)

// Create client with defaults (localhost:7233, default namespace)
ctx := context.Background()
c, err := client.NewWithDefaults(ctx)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// Or create with custom configuration
cfg := &client.Config{
    HostPort:  "temporal.example.com:7233",
    Namespace: "production",
    ConnectionTimeout: 15 * time.Second,
}
c, err := client.New(ctx, cfg)
```

### 2. Define Workflows and Activities

```go
import (
    "go.temporal.io/sdk/workflow"
    "go.temporal.io/sdk/activity"
)

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
    ao := workflow.ActivityOptions{
        StartToCloseTimeout: 10 * time.Second,
    }
    ctx = workflow.WithActivityOptions(ctx, ao)

    var greeting string
    err := workflow.ExecuteActivity(ctx, GreetActivity, name).Get(ctx, &greeting)
    return greeting, err
}

func GreetActivity(ctx context.Context, name string) (string, error) {
    return fmt.Sprintf("Hello, %s!", name), nil
}
```

### 3. Create and Start a Worker

```go
import (
    "github.com/templatedop/temporal/worker"
)

// Use the fluent builder for easy worker setup
w, err := worker.NewBuilder(c, "my-task-queue").
    RegisterWorkflow(GreetingWorkflow).
    RegisterActivity(GreetActivity).
    WithMaxConcurrentWorkflows(10).
    WithLogging(true).
    Build()

if err != nil {
    log.Fatal(err)
}

// Run the worker (blocks until interrupted)
err = w.Run(context.Background())
```

### 4. Execute a Workflow

```go
import (
    temporalworkflow "github.com/templatedop/temporal/workflow"
)

// Use the workflow builder for clean configuration
opts := temporalworkflow.NewBuilder("my-workflow-id", "my-task-queue").
    WithShortRunningDefaults().
    WithModerateRetry().
    WithSearchAttribute("CustomerID", "12345").
    Build()

workflowRun, err := c.ExecuteWorkflow(ctx, opts, GreetingWorkflow, "World")
if err != nil {
    log.Fatal(err)
}

// Wait for result
var result string
err = workflowRun.Get(ctx, &result)
```

## API Overview

### Client Package

#### Client Creation

```go
// Default configuration
c, err := client.NewWithDefaults(ctx)

// Custom configuration
cfg := &client.Config{
    HostPort:  "localhost:7233",
    Namespace: "default",
    ConnectionTimeout: 10 * time.Second,
    Identity: "my-worker",
}
c, err := client.New(ctx, cfg)
```

#### Workflow Operations

```go
// Execute workflow
workflowRun, err := c.ExecuteWorkflow(ctx, opts, workflowFunc, args...)

// Get workflow
workflowRun := c.GetWorkflow(ctx, workflowID, runID)

// Query workflow
result, err := c.QueryWorkflow(ctx, workflowID, runID, "queryType", args...)

// Signal workflow
err := c.SignalWorkflow(ctx, workflowID, runID, "signalName", data)

// Cancel workflow
err := c.CancelWorkflow(ctx, workflowID, runID)

// Terminate workflow
err := c.TerminateWorkflow(ctx, workflowID, runID, "reason", details...)
```

### Workflow Builder

The workflow builder provides a fluent API for configuring workflow execution options:

```go
opts := temporalworkflow.NewBuilder("workflow-id", "task-queue").
    // Timeouts
    WithExecutionTimeout(24 * time.Hour).
    WithRunTimeout(1 * time.Hour).
    WithTaskTimeout(10 * time.Second).

    // Preset timeout configurations
    WithShortRunningDefaults().    // 1h execution, 10m run, 10s task
    WithDefaultTimeouts().          // 24h execution, 1h run, 10s task
    WithLongRunningDefaults().      // 7d execution, 24h run, 10s task

    // Retry policies
    WithRetryPolicy(1*time.Second, 3).
    WithAggressiveRetry().          // 1s initial, 3 attempts
    WithModerateRetry().            // 5s initial, 5 attempts
    WithPatientRetry().             // 30s initial, 10 attempts

    // Cron schedule
    WithCronSchedule("0 0 * * *").

    // Metadata
    WithMemo("key", "value").
    WithSearchAttribute("OrderID", "12345").

    Build()
```

### Worker Package

#### Worker Builder

```go
w, err := worker.NewBuilder(client, "task-queue").
    // Register workflows and activities
    RegisterWorkflow(MyWorkflow).
    RegisterActivity(MyActivity).

    // Configure concurrency
    WithMaxConcurrentWorkflows(10).
    WithMaxConcurrentActivities(50).

    // Enable/disable logging
    WithLogging(true).

    Build()
```

#### Worker Lifecycle

```go
// Start worker
err := w.Start()

// Stop worker
w.Stop()

// Run worker (blocks until interrupted)
err := w.Run(ctx)

// Build and run in one step
err := worker.NewBuilder(c, "task-queue").
    RegisterWorkflow(MyWorkflow).
    RegisterActivity(MyActivity).
    BuildAndRun(ctx)
```

### Testing Package

#### Workflow Testing

```go
import (
    "testing"
    temporaltesting "github.com/templatedop/temporal/testing"
)

func TestMyWorkflow(t *testing.T) {
    wt := temporaltesting.NewWorkflowTest(t)

    // Register workflow and activities
    wt.RegisterWorkflow(MyWorkflow)
    wt.RegisterActivity(MyActivity)

    // Mock activity responses
    temporaltesting.MockActivitySuccess(wt, MyActivity, "mocked result", "arg1")

    // Execute workflow
    wt.ExecuteWorkflow(MyWorkflow, "input")

    // Verify results
    if !wt.IsWorkflowCompleted() {
        t.Fatal("Workflow not completed")
    }

    var result string
    err := wt.GetWorkflowResult(&result)
    assert.NoError(t, err)
    assert.Equal(t, "expected", result)

    // Verify expectations
    wt.AssertExpectations(t)
}
```

#### Activity Testing

```go
func TestMyActivity(t *testing.T) {
    result, err := temporaltesting.RunActivityTest(t, MyActivity, "input")

    assert.NoError(t, err)
    assert.Equal(t, "expected", result)
}
```

#### Convenience Functions

```go
// Quick workflow test
wt, result := temporaltesting.RunWorkflowTest(t, MyWorkflow, "input")

// Quick activity test
result, err := temporaltesting.RunActivityTest(t, MyActivity, "input")
```

## Examples

### Simple Example

See [examples/simple/main.go](examples/simple/main.go) for a basic greeting workflow that demonstrates:
- Client creation with defaults
- Worker builder usage
- Simple workflow and activity
- Workflow execution with fluent builder

### Advanced Example

See [examples/advanced/main.go](examples/advanced/main.go) for a complex order processing workflow that demonstrates:
- Multi-step workflow with state management
- Signal handling for cancellation
- Query handlers for status checking
- Activity retry policies
- Compensating transactions (refunds)

## Architecture

```
temporal/
├── client/          # Client wrapper and workflow operations
│   ├── client.go    # Client creation and configuration
│   └── workflow.go  # Workflow execution and management
├── worker/          # Worker management
│   └── worker.go    # Worker builder and lifecycle
├── workflow/        # Workflow utilities
│   └── builder.go   # Fluent workflow options builder
├── testing/         # Testing helpers
│   └── testing.go   # Workflow and activity test utilities
└── examples/        # Example applications
    ├── simple/      # Basic usage example
    └── advanced/    # Complex workflow example
```

## Why Use This Library?

### Before (Official SDK)

```go
// Verbose client creation
c, err := client.Dial(client.Options{
    HostPort:  "localhost:7233",
    Namespace: "default",
    ConnectionOptions: client.ConnectionOptions{
        DialTimeout: 10 * time.Second,
    },
})

// Verbose workflow options
workflowOptions := client.StartWorkflowOptions{
    ID:                       "my-workflow",
    TaskQueue:                "my-queue",
    WorkflowExecutionTimeout: 24 * time.Hour,
    WorkflowRunTimeout:       1 * time.Hour,
    WorkflowTaskTimeout:      10 * time.Second,
    RetryPolicy: &client.RetryPolicy{
        InitialInterval:    5 * time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    50 * time.Second,
        MaximumAttempts:    5,
    },
}
```

### After (This Library)

```go
// Simple client creation
c, err := client.NewWithDefaults(ctx)

// Fluent workflow options
opts := temporalworkflow.NewBuilder("my-workflow", "my-queue").
    WithDefaultTimeouts().
    WithModerateRetry().
    Build()
```

## Best Practices

1. **Use preset configurations** - Start with `WithDefaultTimeouts()` or `WithShortRunningDefaults()` and customize as needed
2. **Leverage the builder pattern** - Use fluent builders for cleaner, more maintainable code
3. **Test thoroughly** - Use the testing helpers to write comprehensive workflow and activity tests
4. **Handle errors gracefully** - The wrapper provides better error messages for common issues
5. **Access underlying SDK when needed** - Use `client.Underlying()` for advanced features not wrapped

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This library is provided as-is for use with the Temporal workflow engine.

## Related Projects

- [Temporal Go SDK](https://github.com/temporalio/sdk-go) - Official Temporal SDK for Go
- [Temporal](https://temporal.io) - The Temporal workflow engine

## Support

For issues related to this wrapper library, please file an issue in this repository.

For issues with the underlying Temporal SDK or server, please refer to the [official Temporal documentation](https://docs.temporal.io).
