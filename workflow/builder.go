package workflow

import (
	"time"

	"github.com/templatedop/temporal/client"
)

// Builder provides a fluent interface for building workflow options
type Builder struct {
	opts *client.WorkflowOptions
}

// NewBuilder creates a new workflow builder with required parameters
func NewBuilder(workflowID, taskQueue string) *Builder {
	return &Builder{
		opts: &client.WorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		},
	}
}

// WithExecutionTimeout sets the workflow execution timeout
func (b *Builder) WithExecutionTimeout(timeout time.Duration) *Builder {
	b.opts.WorkflowExecutionTimeout = timeout
	return b
}

// WithRunTimeout sets the workflow run timeout
func (b *Builder) WithRunTimeout(timeout time.Duration) *Builder {
	b.opts.WorkflowRunTimeout = timeout
	return b
}

// WithTaskTimeout sets the workflow task timeout
func (b *Builder) WithTaskTimeout(timeout time.Duration) *Builder {
	b.opts.WorkflowTaskTimeout = timeout
	return b
}

// WithRetryPolicy sets a retry policy for the workflow
func (b *Builder) WithRetryPolicy(initialInterval time.Duration, maxAttempts int32) *Builder {
	b.opts.RetryPolicy = &client.RetryPolicy{
		InitialInterval:    initialInterval,
		BackoffCoefficient: 2.0,
		MaximumInterval:    initialInterval * 10,
		MaximumAttempts:    maxAttempts,
	}
	return b
}

// WithCustomRetryPolicy sets a custom retry policy with all parameters
func (b *Builder) WithCustomRetryPolicy(policy *client.RetryPolicy) *Builder {
	b.opts.RetryPolicy = policy
	return b
}

// WithCronSchedule sets a cron schedule for the workflow
func (b *Builder) WithCronSchedule(cronSchedule string) *Builder {
	b.opts.CronSchedule = cronSchedule
	return b
}

// WithMemo adds memo data to the workflow
func (b *Builder) WithMemo(key string, value interface{}) *Builder {
	if b.opts.Memo == nil {
		b.opts.Memo = make(map[string]interface{})
	}
	b.opts.Memo[key] = value
	return b
}

// WithSearchAttribute adds a search attribute to the workflow
func (b *Builder) WithSearchAttribute(key string, value interface{}) *Builder {
	if b.opts.SearchAttributes == nil {
		b.opts.SearchAttributes = make(map[string]interface{})
	}
	b.opts.SearchAttributes[key] = value
	return b
}

// Build returns the constructed workflow options
func (b *Builder) Build() *client.WorkflowOptions {
	return b.opts
}

// Common preset configurations

// WithDefaultTimeouts sets common timeout values (execution: 24h, run: 1h, task: 10s)
func (b *Builder) WithDefaultTimeouts() *Builder {
	b.opts.WorkflowExecutionTimeout = 24 * time.Hour
	b.opts.WorkflowRunTimeout = 1 * time.Hour
	b.opts.WorkflowTaskTimeout = 10 * time.Second
	return b
}

// WithShortRunningDefaults sets timeouts for short-running workflows (execution: 1h, run: 10m, task: 10s)
func (b *Builder) WithShortRunningDefaults() *Builder {
	b.opts.WorkflowExecutionTimeout = 1 * time.Hour
	b.opts.WorkflowRunTimeout = 10 * time.Minute
	b.opts.WorkflowTaskTimeout = 10 * time.Second
	return b
}

// WithLongRunningDefaults sets timeouts for long-running workflows (execution: 7d, run: 24h, task: 10s)
func (b *Builder) WithLongRunningDefaults() *Builder {
	b.opts.WorkflowExecutionTimeout = 7 * 24 * time.Hour
	b.opts.WorkflowRunTimeout = 24 * time.Hour
	b.opts.WorkflowTaskTimeout = 10 * time.Second
	return b
}

// WithAggressiveRetry sets an aggressive retry policy (initial: 1s, max: 3 attempts)
func (b *Builder) WithAggressiveRetry() *Builder {
	return b.WithRetryPolicy(1*time.Second, 3)
}

// WithModerateRetry sets a moderate retry policy (initial: 5s, max: 5 attempts)
func (b *Builder) WithModerateRetry() *Builder {
	return b.WithRetryPolicy(5*time.Second, 5)
}

// WithPatientRetry sets a patient retry policy (initial: 30s, max: 10 attempts)
func (b *Builder) WithPatientRetry() *Builder {
	return b.WithRetryPolicy(30*time.Second, 10)
}
