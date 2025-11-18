package statemachine

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// State represents a state in the workflow state machine
type State interface {
	// Name returns the unique name of this state
	Name() string

	// Execute runs the state logic and returns the next state name
	Execute(ctx workflow.Context, input interface{}) (nextState string, output interface{}, err error)
}

// StateFunc is a function-based implementation of State
type StateFunc struct {
	StateName string
	Executor  func(ctx workflow.Context, input interface{}) (string, interface{}, error)
}

func (s *StateFunc) Name() string {
	return s.StateName
}

func (s *StateFunc) Execute(ctx workflow.Context, input interface{}) (string, interface{}, error) {
	return s.Executor(ctx, input)
}

// Transition defines a state transition
type Transition struct {
	From      string
	To        string
	Condition func(output interface{}) bool // Optional condition
}

// StateMachine manages state transitions
type StateMachine struct {
	states      map[string]State
	transitions []Transition
	initialState string
	finalStates  map[string]bool
}

// NewStateMachine creates a new state machine
func NewStateMachine(initialState string) *StateMachine {
	return &StateMachine{
		states:       make(map[string]State),
		transitions:  make([]Transition, 0),
		initialState: initialState,
		finalStates:  make(map[string]bool),
	}
}

// AddState adds a state to the state machine
func (sm *StateMachine) AddState(state State) *StateMachine {
	sm.states[state.Name()] = state
	return sm
}

// AddTransition adds a transition between states
func (sm *StateMachine) AddTransition(from, to string) *StateMachine {
	sm.transitions = append(sm.transitions, Transition{
		From: from,
		To:   to,
	})
	return sm
}

// AddConditionalTransition adds a conditional transition
func (sm *StateMachine) AddConditionalTransition(from, to string, condition func(interface{}) bool) *StateMachine {
	sm.transitions = append(sm.transitions, Transition{
		From:      from,
		To:        to,
		Condition: condition,
	})
	return sm
}

// SetFinalState marks a state as final (terminal)
func (sm *StateMachine) SetFinalState(stateName string) *StateMachine {
	sm.finalStates[stateName] = true
	return sm
}

// Execute runs the state machine workflow
func (sm *StateMachine) Execute(ctx workflow.Context, input interface{}) (interface{}, error) {
	logger := workflow.GetLogger(ctx)

	currentState := sm.initialState
	currentInput := input
	var lastOutput interface{}

	// Track state history for debugging
	stateHistory := []string{currentState}

	for {
		logger.Info("Executing state", "state", currentState)

		// Check if we've reached a final state
		if sm.finalStates[currentState] {
			logger.Info("Reached final state", "state", currentState)
			return lastOutput, nil
		}

		// Get the current state
		state, exists := sm.states[currentState]
		if !exists {
			return nil, fmt.Errorf("state not found: %s", currentState)
		}

		// Execute the state
		nextState, output, err := state.Execute(ctx, currentInput)
		if err != nil {
			logger.Error("State execution failed", "state", currentState, "error", err)
			return nil, fmt.Errorf("state %s failed: %w", currentState, err)
		}

		lastOutput = output
		currentInput = output // Pass output as input to next state

		// Validate transition
		if !sm.isValidTransition(currentState, nextState, output) {
			return nil, fmt.Errorf("invalid transition from %s to %s", currentState, nextState)
		}

		logger.Info("State transition", "from", currentState, "to", nextState)
		stateHistory = append(stateHistory, nextState)

		// Prevent infinite loops
		if len(stateHistory) > 1000 {
			return nil, fmt.Errorf("too many state transitions, possible infinite loop")
		}

		currentState = nextState
	}
}

// isValidTransition checks if a transition is valid
func (sm *StateMachine) isValidTransition(from, to string, output interface{}) bool {
	for _, transition := range sm.transitions {
		if transition.From == from && transition.To == to {
			if transition.Condition != nil {
				return transition.Condition(output)
			}
			return true
		}
	}
	return false
}

// Builder provides a fluent interface for building state machines
type Builder struct {
	sm *StateMachine
}

// NewBuilder creates a new state machine builder
func NewBuilder(initialState string) *Builder {
	return &Builder{
		sm: NewStateMachine(initialState),
	}
}

// State adds a state with a function
func (b *Builder) State(name string, executor func(workflow.Context, interface{}) (string, interface{}, error)) *Builder {
	b.sm.AddState(&StateFunc{
		StateName: name,
		Executor:  executor,
	})
	return b
}

// StateWithActivity adds a state that executes an activity
func (b *Builder) StateWithActivity(name string, activityFunc interface{}, nextState string, timeout time.Duration) *Builder {
	executor := func(ctx workflow.Context, input interface{}) (string, interface{}, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: timeout,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result interface{}
		err := workflow.ExecuteActivity(ctx, activityFunc, input).Get(ctx, &result)
		if err != nil {
			return "", nil, err
		}

		return nextState, result, nil
	}

	b.sm.AddState(&StateFunc{
		StateName: name,
		Executor:  executor,
	})
	return b
}

// Transition adds a transition between states
func (b *Builder) Transition(from, to string) *Builder {
	b.sm.AddTransition(from, to)
	return b
}

// ConditionalTransition adds a conditional transition
func (b *Builder) ConditionalTransition(from, to string, condition func(interface{}) bool) *Builder {
	b.sm.AddConditionalTransition(from, to, condition)
	return b
}

// FinalState marks a state as final
func (b *Builder) FinalState(name string) *Builder {
	b.sm.SetFinalState(name)
	return b
}

// Build returns the constructed state machine
func (b *Builder) Build() *StateMachine {
	return b.sm
}

// Common state machine patterns

// LinearStateMachine creates a simple linear state machine
func LinearStateMachine(states ...State) *StateMachine {
	if len(states) == 0 {
		return nil
	}

	sm := NewStateMachine(states[0].Name())

	for i, state := range states {
		sm.AddState(state)

		// Add transitions
		if i < len(states)-1 {
			sm.AddTransition(state.Name(), states[i+1].Name())
		} else {
			// Last state is final
			sm.SetFinalState(state.Name())
		}
	}

	return sm
}
