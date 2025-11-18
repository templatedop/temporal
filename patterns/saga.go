package patterns

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// SagaStep represents a single step in a saga
type SagaStep struct {
	Name             string
	Action           interface{} // Activity to execute
	Compensation     interface{} // Activity to run if saga fails
	ActionTimeout    time.Duration
	CompensateTimeout time.Duration
}

// SagaResult contains the result of a saga execution
type SagaResult struct {
	Success         bool
	CompletedSteps  int
	FailedStep      string
	Error           error
	Results         []interface{}
}

// Saga implements the Saga pattern for distributed transactions
type Saga struct {
	steps []SagaStep
}

// NewSaga creates a new saga
func NewSaga() *Saga {
	return &Saga{
		steps: make([]SagaStep, 0),
	}
}

// AddStep adds a step to the saga
func (s *Saga) AddStep(name string, action, compensation interface{}, actionTimeout, compensateTimeout time.Duration) *Saga {
	s.steps = append(s.steps, SagaStep{
		Name:             name,
		Action:           action,
		Compensation:     compensation,
		ActionTimeout:    actionTimeout,
		CompensateTimeout: compensateTimeout,
	})
	return s
}

// Execute runs the saga workflow
func (s *Saga) Execute(ctx workflow.Context, input interface{}) (*SagaResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting saga execution", "steps", len(s.steps))

	result := &SagaResult{
		Success: true,
		Results: make([]interface{}, 0),
	}

	completedSteps := make([]int, 0)
	currentInput := input

	// Execute forward steps
	for i, step := range s.steps {
		logger.Info("Executing saga step", "step", step.Name, "index", i)

		ao := workflow.ActivityOptions{
			StartToCloseTimeout: step.ActionTimeout,
		}
		stepCtx := workflow.WithActivityOptions(ctx, ao)

		var stepResult interface{}
		err := workflow.ExecuteActivity(stepCtx, step.Action, currentInput).Get(stepCtx, &stepResult)

		if err != nil {
			logger.Error("Saga step failed", "step", step.Name, "error", err)
			result.Success = false
			result.FailedStep = step.Name
			result.Error = fmt.Errorf("step %s failed: %w", step.Name, err)
			result.CompletedSteps = i

			// Compensate completed steps in reverse order
			logger.Info("Starting compensation", "completedSteps", len(completedSteps))
			s.compensate(ctx, completedSteps, logger)

			return result, result.Error
		}

		completedSteps = append(completedSteps, i)
		result.Results = append(result.Results, stepResult)
		currentInput = stepResult // Use output as input for next step

		logger.Info("Saga step completed", "step", step.Name)
	}

	result.CompletedSteps = len(s.steps)
	logger.Info("Saga completed successfully")

	return result, nil
}

// compensate runs compensation activities in reverse order
func (s *Saga) compensate(ctx workflow.Context, completedSteps []int, logger log.Logger) {
	// Run compensations in reverse order
	for i := len(completedSteps) - 1; i >= 0; i-- {
		stepIndex := completedSteps[i]
		step := s.steps[stepIndex]

		if step.Compensation == nil {
			logger.Warn("No compensation defined for step", "step", step.Name)
			continue
		}

		logger.Info("Compensating step", "step", step.Name)

		ao := workflow.ActivityOptions{
			StartToCloseTimeout: step.CompensateTimeout,
		}
		compCtx := workflow.WithActivityOptions(ctx, ao)

		err := workflow.ExecuteActivity(compCtx, step.Compensation, nil).Get(compCtx, nil)
		if err != nil {
			logger.Error("Compensation failed", "step", step.Name, "error", err)
			// Continue with other compensations even if one fails
		} else {
			logger.Info("Compensation completed", "step", step.Name)
		}
	}
}

// SagaBuilder provides a fluent interface for building sagas
type SagaBuilder struct {
	saga *Saga
}

// NewSagaBuilder creates a new saga builder
func NewSagaBuilder() *SagaBuilder {
	return &SagaBuilder{
		saga: NewSaga(),
	}
}

// Step adds a step with action and compensation
func (b *SagaBuilder) Step(name string, action, compensation interface{}) *SagaBuilder {
	b.saga.AddStep(name, action, compensation, 30*time.Second, 30*time.Second)
	return b
}

// StepWithTimeout adds a step with custom timeouts
func (b *SagaBuilder) StepWithTimeout(name string, action, compensation interface{}, actionTimeout, compensateTimeout time.Duration) *SagaBuilder {
	b.saga.AddStep(name, action, compensation, actionTimeout, compensateTimeout)
	return b
}

// Build returns the constructed saga
func (b *SagaBuilder) Build() *Saga {
	return b.saga
}

// ExecuteSaga is a convenience function to execute a saga
func ExecuteSaga(ctx workflow.Context, saga *Saga, input interface{}) (*SagaResult, error) {
	return saga.Execute(ctx, input)
}
