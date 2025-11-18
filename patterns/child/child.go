package child

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// FanOutFanIn executes multiple child workflows in parallel and collects results
type FanOutFanIn[TInput any, TOutput any] struct {
	concurrency      int
	childWorkflow    interface{}
	errorHandling    ErrorHandling
	timeoutPerChild  int
}

// ErrorHandling defines how to handle child workflow errors
type ErrorHandling int

const (
	// FailOnFirstError stops on first child error
	FailOnFirstError ErrorHandling = iota
	// ContinueOnError continues even if some children fail
	ContinueOnError
	// RequireAll requires all children to succeed
	RequireAll
)

// NewFanOutFanIn creates a new fan-out/fan-in pattern
func NewFanOutFanIn[TInput any, TOutput any](childWorkflow interface{}) *FanOutFanIn[TInput, TOutput] {
	return &FanOutFanIn[TInput, TOutput]{
		concurrency:     10,
		childWorkflow:   childWorkflow,
		errorHandling:   FailOnFirstError,
		timeoutPerChild: 3600, // 1 hour default
	}
}

// WithConcurrency sets max concurrent child workflows
func (f *FanOutFanIn[TInput, TOutput]) WithConcurrency(max int) *FanOutFanIn[TInput, TOutput] {
	f.concurrency = max
	return f
}

// WithErrorHandling sets error handling strategy
func (f *FanOutFanIn[TInput, TOutput]) WithErrorHandling(strategy ErrorHandling) *FanOutFanIn[TInput, TOutput] {
	f.errorHandling = strategy
	return f
}

// WithTimeoutPerChild sets timeout for each child (in seconds)
func (f *FanOutFanIn[TInput, TOutput]) WithTimeoutPerChild(seconds int) *FanOutFanIn[TInput, TOutput] {
	f.timeoutPerChild = seconds
	return f
}

// FanOutResult contains results from fan-out execution
type FanOutResult[TOutput any] struct {
	Results    []TOutput
	Errors     []error
	Successful int
	Failed     int
}

// Execute runs the fan-out/fan-in pattern
func (f *FanOutFanIn[TInput, TOutput]) Execute(ctx workflow.Context, inputs []TInput) (*FanOutResult[TOutput], error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting fan-out/fan-in", "items", len(inputs), "concurrency", f.concurrency)

	result := &FanOutResult[TOutput]{
		Results: make([]TOutput, len(inputs)),
		Errors:  make([]error, len(inputs)),
	}

	// Create semaphore for concurrency control
	sem := make(chan struct{}, f.concurrency)

	// Create futures for all child workflows
	futures := make([]workflow.Future, len(inputs))

	for i, input := range inputs {
		// Acquire semaphore
		sem <- struct{}{}

		idx := i
		inp := input

		childOpts := workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: time.Duration(f.timeoutPerChild) * time.Second,
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)

		futures[idx] = workflow.ExecuteChildWorkflow(childCtx, f.childWorkflow, inp)

		// Release semaphore when done
		workflow.Go(ctx, func(ctx workflow.Context) {
			futures[idx].Get(ctx, nil)
			<-sem
		})
	}

	// Wait for all futures and collect results
	for i, future := range futures {
		var output TOutput
		err := future.Get(ctx, &output)

		if err != nil {
			result.Errors[i] = err
			result.Failed++
			logger.Warn("Child workflow failed", "index", i, "error", err)

			if f.errorHandling == FailOnFirstError {
				return result, fmt.Errorf("child workflow %d failed: %w", i, err)
			}
		} else {
			result.Results[i] = output
			result.Successful++
		}
	}

	logger.Info("Fan-out/fan-in completed", "successful", result.Successful, "failed", result.Failed)

	if f.errorHandling == RequireAll && result.Failed > 0 {
		return result, fmt.Errorf("some child workflows failed: %d/%d", result.Failed, len(inputs))
	}

	return result, nil
}

// ParentChildCoordinator coordinates parent and multiple child workflows
type ParentChildCoordinator[TInput any, TOutput any] struct {
	parentWorkflow interface{}
	childWorkflows []interface{}
	errorHandling  ErrorHandling
}

// NewParentChildCoordinator creates a new parent-child coordinator
func NewParentChildCoordinator[TInput any, TOutput any]() *ParentChildCoordinator[TInput, TOutput] {
	return &ParentChildCoordinator[TInput, TOutput]{
		childWorkflows: make([]interface{}, 0),
		errorHandling:  FailOnFirstError,
	}
}

// Parent sets the parent workflow
func (p *ParentChildCoordinator[TInput, TOutput]) Parent(parentWorkflow interface{}) *ParentChildCoordinator[TInput, TOutput] {
	p.parentWorkflow = parentWorkflow
	return p
}

// AddChild adds a child workflow
func (p *ParentChildCoordinator[TInput, TOutput]) AddChild(childWorkflow interface{}) *ParentChildCoordinator[TInput, TOutput] {
	p.childWorkflows = append(p.childWorkflows, childWorkflow)
	return p
}

// WithErrorHandling sets error handling strategy
func (p *ParentChildCoordinator[TInput, TOutput]) WithErrorHandling(strategy ErrorHandling) *ParentChildCoordinator[TInput, TOutput] {
	p.errorHandling = strategy
	return p
}

// CoordinationResult contains results from parent-child coordination
type CoordinationResult[TOutput any] struct {
	ParentResult interface{}
	ChildResults []TOutput
	Errors       []error
}

// Execute runs the parent-child coordination
func (p *ParentChildCoordinator[TInput, TOutput]) Execute(ctx workflow.Context, input TInput) (*CoordinationResult[TOutput], error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting parent-child coordination", "children", len(p.childWorkflows))

	result := &CoordinationResult[TOutput]{
		ChildResults: make([]TOutput, len(p.childWorkflows)),
		Errors:       make([]error, len(p.childWorkflows)),
	}

	// Execute parent workflow first if specified
	if p.parentWorkflow != nil {
		childOpts := workflow.ChildWorkflowOptions{}
		parentCtx := workflow.WithChildOptions(ctx, childOpts)

		var parentResult interface{}
		err := workflow.ExecuteChildWorkflow(parentCtx, p.parentWorkflow, input).Get(parentCtx, &parentResult)
		if err != nil {
			return result, fmt.Errorf("parent workflow failed: %w", err)
		}
		result.ParentResult = parentResult
	}

	// Execute all child workflows
	futures := make([]workflow.Future, len(p.childWorkflows))

	for i, childWorkflow := range p.childWorkflows {
		childOpts := workflow.ChildWorkflowOptions{}
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		futures[i] = workflow.ExecuteChildWorkflow(childCtx, childWorkflow, input)
	}

	// Collect results
	for i, future := range futures {
		var output TOutput
		err := future.Get(ctx, &output)

		if err != nil {
			result.Errors[i] = err
			logger.Warn("Child workflow failed", "index", i, "error", err)

			if p.errorHandling == FailOnFirstError {
				return result, fmt.Errorf("child workflow %d failed: %w", i, err)
			}
		} else {
			result.ChildResults[i] = output
		}
	}

	logger.Info("Parent-child coordination completed")
	return result, nil
}

// Sequential executes child workflows one after another
type Sequential[TInput any, TOutput any] struct {
	workflows []interface{}
	pipeline  bool // if true, output of one becomes input of next
}

// NewSequential creates a new sequential execution pattern
func NewSequential[TInput any, TOutput any]() *Sequential[TInput, TOutput] {
	return &Sequential[TInput, TOutput]{
		workflows: make([]interface{}, 0),
		pipeline:  false,
	}
}

// AddWorkflow adds a workflow to the sequence
func (s *Sequential[TInput, TOutput]) AddWorkflow(wf interface{}) *Sequential[TInput, TOutput] {
	s.workflows = append(s.workflows, wf)
	return s
}

// AsPipeline sets whether to pipeline outputs to next workflow
func (s *Sequential[TInput, TOutput]) AsPipeline(pipeline bool) *Sequential[TInput, TOutput] {
	s.pipeline = pipeline
	return s
}

// SequentialResult contains results from sequential execution
type SequentialResult[TOutput any] struct {
	Results []TOutput
	Final   TOutput
}

// Execute runs workflows sequentially
func (s *Sequential[TInput, TOutput]) Execute(ctx workflow.Context, input TInput) (*SequentialResult[TOutput], error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting sequential execution", "workflows", len(s.workflows))

	result := &SequentialResult[TOutput]{
		Results: make([]TOutput, len(s.workflows)),
	}

	currentInput := interface{}(input)

	for i, wf := range s.workflows {
		logger.Info("Executing workflow", "index", i)

		childOpts := workflow.ChildWorkflowOptions{}
		childCtx := workflow.WithChildOptions(ctx, childOpts)

		var output TOutput
		err := workflow.ExecuteChildWorkflow(childCtx, wf, currentInput).Get(childCtx, &output)
		if err != nil {
			return result, fmt.Errorf("workflow %d failed: %w", i, err)
		}

		result.Results[i] = output

		if s.pipeline {
			currentInput = output
		}
	}

	if len(result.Results) > 0 {
		result.Final = result.Results[len(result.Results)-1]
	}

	logger.Info("Sequential execution completed")
	return result, nil
}
