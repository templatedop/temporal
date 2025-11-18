package dsl

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// WorkflowDefinition represents a workflow defined in config/DSL
type WorkflowDefinition struct {
	Name        string                 `yaml:"name" json:"name"`
	Description string                 `yaml:"description" json:"description"`
	Version     string                 `yaml:"version" json:"version"`
	Timeout     string                 `yaml:"timeout" json:"timeout"` // e.g., "1h", "30m"
	Input       map[string]interface{} `yaml:"input" json:"input"`
	Steps       []Step                 `yaml:"steps" json:"steps"`
	OnError     *ErrorHandler          `yaml:"onError" json:"onError"`
}

// Step represents a single step in the workflow
type Step struct {
	Name        string                 `yaml:"name" json:"name"`
	Type        StepType               `yaml:"type" json:"type"`
	Activity    string                 `yaml:"activity,omitempty" json:"activity,omitempty"`
	Workflow    string                 `yaml:"workflow,omitempty" json:"workflow,omitempty"`
	Input       map[string]interface{} `yaml:"input,omitempty" json:"input,omitempty"`
	Timeout     string                 `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	RetryPolicy *RetryPolicy           `yaml:"retryPolicy,omitempty" json:"retryPolicy,omitempty"`
	Condition   string                 `yaml:"condition,omitempty" json:"condition,omitempty"` // For conditional execution
	Parallel    []Step                 `yaml:"parallel,omitempty" json:"parallel,omitempty"`   // For parallel execution
	Sequential  []Step                 `yaml:"sequential,omitempty" json:"sequential,omitempty"` // For sequential sub-steps
	Loop        *LoopConfig            `yaml:"loop,omitempty" json:"loop,omitempty"`
	Switch      *SwitchConfig          `yaml:"switch,omitempty" json:"switch,omitempty"`
	Wait        string                 `yaml:"wait,omitempty" json:"wait,omitempty"` // Duration to wait
	Signal      *SignalConfig          `yaml:"signal,omitempty" json:"signal,omitempty"`
	OutputVar   string                 `yaml:"outputVar,omitempty" json:"outputVar,omitempty"` // Store result in this variable
}

// StepType defines the type of workflow step
type StepType string

const (
	StepTypeActivity   StepType = "activity"
	StepTypeWorkflow   StepType = "workflow"
	StepTypeParallel   StepType = "parallel"
	StepTypeSequential StepType = "sequential"
	StepTypeCondition  StepType = "condition"
	StepTypeLoop       StepType = "loop"
	StepTypeSwitch     StepType = "switch"
	StepTypeWait       StepType = "wait"
	StepTypeSignal     StepType = "signal"
)

// RetryPolicy defines retry configuration for a step
type RetryPolicy struct {
	MaxAttempts     int    `yaml:"maxAttempts" json:"maxAttempts"`
	InitialInterval string `yaml:"initialInterval" json:"initialInterval"`
	MaxInterval     string `yaml:"maxInterval" json:"maxInterval"`
	BackoffFactor   float64 `yaml:"backoffFactor" json:"backoffFactor"`
}

// LoopConfig defines loop configuration
type LoopConfig struct {
	Items    string `yaml:"items" json:"items"`       // Variable containing items to iterate
	ItemVar  string `yaml:"itemVar" json:"itemVar"`   // Variable name for current item
	IndexVar string `yaml:"indexVar" json:"indexVar"` // Variable name for index
	MaxIterations int `yaml:"maxIterations" json:"maxIterations"`
}

// SwitchConfig defines switch/case configuration
type SwitchConfig struct {
	Value   string              `yaml:"value" json:"value"` // Variable to switch on
	Cases   map[string][]Step   `yaml:"cases" json:"cases"`
	Default []Step              `yaml:"default" json:"default"`
}

// SignalConfig defines signal configuration
type SignalConfig struct {
	Name    string `yaml:"name" json:"name"`
	Timeout string `yaml:"timeout" json:"timeout"`
}

// ErrorHandler defines error handling configuration
type ErrorHandler struct {
	Steps []Step `yaml:"steps" json:"steps"`
}

// ActivityRegistry manages registered activities
type ActivityRegistry struct {
	activities map[string]interface{}
}

// NewActivityRegistry creates a new activity registry
func NewActivityRegistry() *ActivityRegistry {
	return &ActivityRegistry{
		activities: make(map[string]interface{}),
	}
}

// Register registers an activity by name
func (ar *ActivityRegistry) Register(name string, activity interface{}) {
	ar.activities[name] = activity
}

// Get retrieves an activity by name
func (ar *ActivityRegistry) Get(name string) (interface{}, error) {
	activity, ok := ar.activities[name]
	if !ok {
		return nil, fmt.Errorf("activity not found: %s", name)
	}
	return activity, nil
}

// WorkflowContext holds execution context
type WorkflowContext struct {
	Variables map[string]interface{}
	Registry  *ActivityRegistry
}

// NewWorkflowContext creates a new workflow context
func NewWorkflowContext(registry *ActivityRegistry, input map[string]interface{}) *WorkflowContext {
	ctx := &WorkflowContext{
		Variables: make(map[string]interface{}),
		Registry:  registry,
	}

	// Initialize with input variables
	for k, v := range input {
		ctx.Variables[k] = v
	}

	return ctx
}

// SetVariable sets a variable in the context
func (wc *WorkflowContext) SetVariable(name string, value interface{}) {
	wc.Variables[name] = value
}

// GetVariable gets a variable from the context
func (wc *WorkflowContext) GetVariable(name string) (interface{}, bool) {
	val, ok := wc.Variables[name]
	return val, ok
}

// ExecuteDefinedWorkflow executes a workflow from its definition
func ExecuteDefinedWorkflow(ctx workflow.Context, def *WorkflowDefinition, registry *ActivityRegistry, input map[string]interface{}) (map[string]interface{}, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Executing defined workflow", "name", def.Name, "version", def.Version)

	// Create workflow context
	wfCtx := NewWorkflowContext(registry, input)

	// Set workflow timeout if specified
	if def.Timeout != "" {
		timeout, err := time.ParseDuration(def.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %w", err)
		}
		var cancel workflow.CancelFunc
		ctx, cancel = workflow.WithCancel(ctx)
		defer cancel()

		// Use a timer to enforce timeout
		workflow.Go(ctx, func(ctx workflow.Context) {
			_ = workflow.Sleep(ctx, timeout)
			cancel()
		})
	}

	// Execute steps
	for _, step := range def.Steps {
		err := executeStep(ctx, &step, wfCtx)
		if err != nil {
			logger.Error("Step failed", "step", step.Name, "error", err)

			// Execute error handler if defined
			if def.OnError != nil {
				logger.Info("Executing error handler")
				for _, errorStep := range def.OnError.Steps {
					_ = executeStep(ctx, &errorStep, wfCtx)
				}
			}

			return wfCtx.Variables, err
		}
	}

	logger.Info("Workflow completed successfully", "name", def.Name)
	return wfCtx.Variables, nil
}

// executeStep executes a single workflow step
func executeStep(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Executing step", "name", step.Name, "type", step.Type)

	// Check condition if specified
	if step.Condition != "" {
		shouldExecute, err := evaluateCondition(step.Condition, wfCtx)
		if err != nil {
			return fmt.Errorf("condition evaluation failed: %w", err)
		}
		if !shouldExecute {
			logger.Info("Step skipped due to condition", "step", step.Name)
			return nil
		}
	}

	// Execute based on step type
	switch step.Type {
	case StepTypeActivity:
		return executeActivity(ctx, step, wfCtx)
	case StepTypeWorkflow:
		return executeChildWorkflow(ctx, step, wfCtx)
	case StepTypeParallel:
		return executeParallel(ctx, step, wfCtx)
	case StepTypeSequential:
		return executeSequential(ctx, step, wfCtx)
	case StepTypeLoop:
		return executeLoop(ctx, step, wfCtx)
	case StepTypeSwitch:
		return executeSwitch(ctx, step, wfCtx)
	case StepTypeWait:
		return executeWait(ctx, step)
	case StepTypeSignal:
		return executeSignal(ctx, step, wfCtx)
	default:
		return fmt.Errorf("unknown step type: %s", step.Type)
	}
}

// executeActivity executes an activity step
func executeActivity(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	if step.Activity == "" {
		return fmt.Errorf("activity name is required")
	}

	// Get activity from registry
	activity, err := wfCtx.Registry.Get(step.Activity)
	if err != nil {
		return err
	}

	// Prepare activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second, // Default
	}

	if step.Timeout != "" {
		timeout, err := time.ParseDuration(step.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout: %w", err)
		}
		ao.StartToCloseTimeout = timeout
	}

	if step.RetryPolicy != nil {
		ao.RetryPolicy = &temporal.RetryPolicy{
			MaximumAttempts: int32(step.RetryPolicy.MaxAttempts),
		}
		if step.RetryPolicy.InitialInterval != "" {
			interval, _ := time.ParseDuration(step.RetryPolicy.InitialInterval)
			ao.RetryPolicy.InitialInterval = interval
		}
		if step.RetryPolicy.MaxInterval != "" {
			maxInterval, _ := time.ParseDuration(step.RetryPolicy.MaxInterval)
			ao.RetryPolicy.MaximumInterval = maxInterval
		}
		if step.RetryPolicy.BackoffFactor > 0 {
			ao.RetryPolicy.BackoffCoefficient = step.RetryPolicy.BackoffFactor
		}
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	// Prepare input
	input := resolveVariables(step.Input, wfCtx)

	// Execute activity
	var result interface{}
	err = workflow.ExecuteActivity(ctx, activity, input).Get(ctx, &result)
	if err != nil {
		return err
	}

	// Store result if output variable specified
	if step.OutputVar != "" {
		wfCtx.SetVariable(step.OutputVar, result)
	}

	return nil
}

// executeChildWorkflow executes a child workflow step
func executeChildWorkflow(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	// This is a placeholder - would need actual child workflow execution
	return fmt.Errorf("child workflow execution not implemented yet")
}

// executeParallel executes steps in parallel
func executeParallel(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	if len(step.Parallel) == 0 {
		return nil
	}

	// Create a selector for parallel execution
	selector := workflow.NewSelector(ctx)
	futures := make([]workflow.Future, len(step.Parallel))
	errors := make([]error, len(step.Parallel))

	// Start all parallel steps
	for i, parallelStep := range step.Parallel {
		i := i
		parallelStep := parallelStep
		futures[i] = workflow.ExecuteLocalActivity(ctx, func() error {
			return executeStep(ctx, &parallelStep, wfCtx)
		})

		selector.AddFuture(futures[i], func(f workflow.Future) {
			errors[i] = f.Get(ctx, nil)
		})
	}

	// Wait for all to complete
	for range step.Parallel {
		selector.Select(ctx)
	}

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

// executeSequential executes steps sequentially
func executeSequential(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	for _, seqStep := range step.Sequential {
		err := executeStep(ctx, &seqStep, wfCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

// executeLoop executes a loop
func executeLoop(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	if step.Loop == nil {
		return fmt.Errorf("loop configuration is required")
	}

	// Get items to iterate over
	itemsVar, ok := wfCtx.GetVariable(step.Loop.Items)
	if !ok {
		return fmt.Errorf("loop items variable not found: %s", step.Loop.Items)
	}

	items, ok := itemsVar.([]interface{})
	if !ok {
		return fmt.Errorf("loop items must be an array")
	}

	// Execute loop
	maxIter := len(items)
	if step.Loop.MaxIterations > 0 && step.Loop.MaxIterations < maxIter {
		maxIter = step.Loop.MaxIterations
	}

	for i := 0; i < maxIter; i++ {
		// Set loop variables
		if step.Loop.ItemVar != "" {
			wfCtx.SetVariable(step.Loop.ItemVar, items[i])
		}
		if step.Loop.IndexVar != "" {
			wfCtx.SetVariable(step.Loop.IndexVar, i)
		}

		// Execute loop body (sequential steps)
		for _, loopStep := range step.Sequential {
			err := executeStep(ctx, &loopStep, wfCtx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// executeSwitch executes a switch/case
func executeSwitch(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	if step.Switch == nil {
		return fmt.Errorf("switch configuration is required")
	}

	// Get switch value
	valueVar, ok := wfCtx.GetVariable(step.Switch.Value)
	if !ok {
		return fmt.Errorf("switch value variable not found: %s", step.Switch.Value)
	}

	valueStr := fmt.Sprintf("%v", valueVar)

	// Find matching case
	caseSteps, ok := step.Switch.Cases[valueStr]
	if !ok {
		// Use default case
		caseSteps = step.Switch.Default
	}

	// Execute case steps
	for _, caseStep := range caseSteps {
		err := executeStep(ctx, &caseStep, wfCtx)
		if err != nil {
			return err
		}
	}

	return nil
}

// executeWait waits for a duration
func executeWait(ctx workflow.Context, step *Step) error {
	if step.Wait == "" {
		return fmt.Errorf("wait duration is required")
	}

	duration, err := time.ParseDuration(step.Wait)
	if err != nil {
		return fmt.Errorf("invalid wait duration: %w", err)
	}

	return workflow.Sleep(ctx, duration)
}

// executeSignal waits for a signal
func executeSignal(ctx workflow.Context, step *Step, wfCtx *WorkflowContext) error {
	if step.Signal == nil {
		return fmt.Errorf("signal configuration is required")
	}

	signalChan := workflow.GetSignalChannel(ctx, step.Signal.Name)

	// Setup timeout if specified
	var timeoutCtx workflow.Context
	var cancel workflow.CancelFunc

	if step.Signal.Timeout != "" {
		timeout, err := time.ParseDuration(step.Signal.Timeout)
		if err != nil {
			return fmt.Errorf("invalid signal timeout: %w", err)
		}
		timeoutCtx, cancel = workflow.WithCancel(ctx)
		defer cancel()

		// Use selector to implement timeout
		selector := workflow.NewSelector(ctx)
		timerFuture := workflow.NewTimer(ctx, timeout)

		var signalData interface{}
		selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &signalData)
			if step.OutputVar != "" {
				wfCtx.SetVariable(step.OutputVar, signalData)
			}
			cancel()
		})
		selector.AddFuture(timerFuture, func(f workflow.Future) {
			cancel()
		})

		selector.Select(timeoutCtx)
		return nil
	} else {
		timeoutCtx = ctx
	}

	// Wait for signal
	var signalData interface{}
	signalChan.Receive(timeoutCtx, &signalData)

	// Store signal data if output variable specified
	if step.OutputVar != "" {
		wfCtx.SetVariable(step.OutputVar, signalData)
	}

	return nil
}

// evaluateCondition evaluates a simple condition
// This is a basic implementation - could be extended with expression parser
func evaluateCondition(condition string, wfCtx *WorkflowContext) (bool, error) {
	// For now, just check if variable exists and is truthy
	// Format: "variableName" or "!variableName"
	negate := false
	varName := condition

	if len(condition) > 0 && condition[0] == '!' {
		negate = true
		varName = condition[1:]
	}

	value, ok := wfCtx.GetVariable(varName)
	if !ok {
		return negate, nil // Variable doesn't exist
	}

	// Check truthiness
	result := isTruthy(value)
	if negate {
		result = !result
	}

	return result, nil
}

// isTruthy checks if a value is truthy
func isTruthy(value interface{}) bool {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case bool:
		return v
	case int, int32, int64:
		return v != 0
	case float32, float64:
		return v != 0
	case string:
		return v != ""
	default:
		return true
	}
}

// resolveVariables resolves variable references in input
func resolveVariables(input map[string]interface{}, wfCtx *WorkflowContext) map[string]interface{} {
	if input == nil {
		return nil
	}

	result := make(map[string]interface{})
	for k, v := range input {
		// If value is a string starting with $, treat as variable reference
		if str, ok := v.(string); ok && len(str) > 0 && str[0] == '$' {
			varName := str[1:]
			if val, exists := wfCtx.GetVariable(varName); exists {
				result[k] = val
			} else {
				result[k] = v // Keep original if variable not found
			}
		} else {
			result[k] = v
		}
	}
	return result
}
