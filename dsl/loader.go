package dsl

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Loader loads workflow definitions from files
type Loader struct {
	definitions map[string]*WorkflowDefinition
}

// NewLoader creates a new workflow definition loader
func NewLoader() *Loader {
	return &Loader{
		definitions: make(map[string]*WorkflowDefinition),
	}
}

// LoadFromYAML loads a workflow definition from a YAML file
func (l *Loader) LoadFromYAML(filepath string) (*WorkflowDefinition, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var def WorkflowDefinition
	err = yaml.Unmarshal(data, &def)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Validate definition
	if err := l.validate(&def); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}

	// Register definition
	l.definitions[def.Name] = &def

	return &def, nil
}

// LoadFromJSON loads a workflow definition from a JSON file
func (l *Loader) LoadFromJSON(filepath string) (*WorkflowDefinition, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var def WorkflowDefinition
	err = json.Unmarshal(data, &def)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Validate definition
	if err := l.validate(&def); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}

	// Register definition
	l.definitions[def.Name] = &def

	return &def, nil
}

// ParseYAML parses a workflow definition from YAML bytes
func (l *Loader) ParseYAML(data []byte) (*WorkflowDefinition, error) {
	var def WorkflowDefinition
	err := yaml.Unmarshal(data, &def)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	if err := l.validate(&def); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}

	l.definitions[def.Name] = &def
	return &def, nil
}

// ParseJSON parses a workflow definition from JSON bytes
func (l *Loader) ParseJSON(data []byte) (*WorkflowDefinition, error) {
	var def WorkflowDefinition
	err := json.Unmarshal(data, &def)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if err := l.validate(&def); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}

	l.definitions[def.Name] = &def
	return &def, nil
}

// Get retrieves a workflow definition by name
func (l *Loader) Get(name string) (*WorkflowDefinition, error) {
	def, ok := l.definitions[name]
	if !ok {
		return nil, fmt.Errorf("workflow definition not found: %s", name)
	}
	return def, nil
}

// List returns all loaded workflow definitions
func (l *Loader) List() []*WorkflowDefinition {
	defs := make([]*WorkflowDefinition, 0, len(l.definitions))
	for _, def := range l.definitions {
		defs = append(defs, def)
	}
	return defs
}

// validate validates a workflow definition
func (l *Loader) validate(def *WorkflowDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("workflow name is required")
	}

	if len(def.Steps) == 0 {
		return fmt.Errorf("workflow must have at least one step")
	}

	// Validate each step
	for i, step := range def.Steps {
		if err := l.validateStep(&step, i); err != nil {
			return err
		}
	}

	return nil
}

// validateStep validates a single step
func (l *Loader) validateStep(step *Step, index int) error {
	if step.Name == "" {
		return fmt.Errorf("step %d: name is required", index)
	}

	if step.Type == "" {
		return fmt.Errorf("step %s: type is required", step.Name)
	}

	// Validate based on step type
	switch step.Type {
	case StepTypeActivity:
		if step.Activity == "" {
			return fmt.Errorf("step %s: activity name is required", step.Name)
		}
	case StepTypeWorkflow:
		if step.Workflow == "" {
			return fmt.Errorf("step %s: workflow name is required", step.Name)
		}
	case StepTypeParallel:
		if len(step.Parallel) == 0 {
			return fmt.Errorf("step %s: parallel steps are required", step.Name)
		}
	case StepTypeSequential:
		if len(step.Sequential) == 0 {
			return fmt.Errorf("step %s: sequential steps are required", step.Name)
		}
	case StepTypeLoop:
		if step.Loop == nil {
			return fmt.Errorf("step %s: loop configuration is required", step.Name)
		}
		if step.Loop.Items == "" {
			return fmt.Errorf("step %s: loop items variable is required", step.Name)
		}
	case StepTypeSwitch:
		if step.Switch == nil {
			return fmt.Errorf("step %s: switch configuration is required", step.Name)
		}
		if step.Switch.Value == "" {
			return fmt.Errorf("step %s: switch value is required", step.Name)
		}
	case StepTypeWait:
		if step.Wait == "" {
			return fmt.Errorf("step %s: wait duration is required", step.Name)
		}
	case StepTypeSignal:
		if step.Signal == nil {
			return fmt.Errorf("step %s: signal configuration is required", step.Name)
		}
		if step.Signal.Name == "" {
			return fmt.Errorf("step %s: signal name is required", step.Name)
		}
	}

	return nil
}

// Builder provides a fluent API for building workflow definitions programmatically
type Builder struct {
	def *WorkflowDefinition
}

// NewBuilder creates a new workflow definition builder
func NewBuilder(name string) *Builder {
	return &Builder{
		def: &WorkflowDefinition{
			Name:  name,
			Steps: make([]Step, 0),
		},
	}
}

// WithDescription sets the workflow description
func (b *Builder) WithDescription(description string) *Builder {
	b.def.Description = description
	return b
}

// WithVersion sets the workflow version
func (b *Builder) WithVersion(version string) *Builder {
	b.def.Version = version
	return b
}

// WithTimeout sets the workflow timeout
func (b *Builder) WithTimeout(timeout string) *Builder {
	b.def.Timeout = timeout
	return b
}

// AddActivityStep adds an activity step
func (b *Builder) AddActivityStep(name, activityName string) *StepBuilder {
	step := Step{
		Name:     name,
		Type:     StepTypeActivity,
		Activity: activityName,
	}
	return &StepBuilder{
		workflowBuilder: b,
		step:            &step,
	}
}

// AddWorkflowStep adds a child workflow step
func (b *Builder) AddWorkflowStep(name, workflowName string) *StepBuilder {
	step := Step{
		Name:     name,
		Type:     StepTypeWorkflow,
		Workflow: workflowName,
	}
	return &StepBuilder{
		workflowBuilder: b,
		step:            &step,
	}
}

// AddWaitStep adds a wait step
func (b *Builder) AddWaitStep(name, duration string) *Builder {
	step := Step{
		Name: name,
		Type: StepTypeWait,
		Wait: duration,
	}
	b.def.Steps = append(b.def.Steps, step)
	return b
}

// AddParallelSteps adds parallel steps
func (b *Builder) AddParallelSteps(name string, steps ...Step) *Builder {
	step := Step{
		Name:     name,
		Type:     StepTypeParallel,
		Parallel: steps,
	}
	b.def.Steps = append(b.def.Steps, step)
	return b
}

// Build builds the workflow definition
func (b *Builder) Build() *WorkflowDefinition {
	return b.def
}

// StepBuilder provides fluent API for building steps
type StepBuilder struct {
	workflowBuilder *Builder
	step            *Step
}

// WithInput sets step input
func (sb *StepBuilder) WithInput(input map[string]interface{}) *StepBuilder {
	sb.step.Input = input
	return sb
}

// WithTimeout sets step timeout
func (sb *StepBuilder) WithTimeout(timeout string) *StepBuilder {
	sb.step.Timeout = timeout
	return sb
}

// WithRetry sets retry policy
func (sb *StepBuilder) WithRetry(maxAttempts int, initialInterval string) *StepBuilder {
	sb.step.RetryPolicy = &RetryPolicy{
		MaxAttempts:     maxAttempts,
		InitialInterval: initialInterval,
		BackoffFactor:   2.0,
	}
	return sb
}

// WithCondition sets a condition for execution
func (sb *StepBuilder) WithCondition(condition string) *StepBuilder {
	sb.step.Condition = condition
	return sb
}

// WithOutputVar sets the output variable name
func (sb *StepBuilder) WithOutputVar(varName string) *StepBuilder {
	sb.step.OutputVar = varName
	return sb
}

// Done completes the step and returns to workflow builder
func (sb *StepBuilder) Done() *Builder {
	sb.workflowBuilder.def.Steps = append(sb.workflowBuilder.def.Steps, *sb.step)
	return sb.workflowBuilder
}
