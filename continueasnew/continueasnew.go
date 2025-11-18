package continueasnew

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// ContinueAsNewManager manages continue-as-new logic for long-running workflows
type ContinueAsNewManager[TState any] struct {
	maxIterations   int
	maxHistorySize  int
	maxExecutionTime int // in seconds
	iterationCount  int
	workflowFunc    interface{}
}

// NewContinueAsNewManager creates a new continue-as-new manager
func NewContinueAsNewManager[TState any](workflowFunc interface{}) *ContinueAsNewManager[TState] {
	return &ContinueAsNewManager[TState]{
		maxIterations:    1000,
		maxHistorySize:   10000,
		maxExecutionTime: 86400, // 24 hours
		workflowFunc:     workflowFunc,
	}
}

// WithMaxIterations sets max iterations before continue-as-new
func (m *ContinueAsNewManager[TState]) WithMaxIterations(max int) *ContinueAsNewManager[TState] {
	m.maxIterations = max
	return m
}

// WithMaxHistorySize sets max history events before continue-as-new
func (m *ContinueAsNewManager[TState]) WithMaxHistorySize(max int) *ContinueAsNewManager[TState] {
	m.maxHistorySize = max
	return m
}

// WithMaxExecutionTime sets max execution time in seconds
func (m *ContinueAsNewManager[TState]) WithMaxExecutionTime(seconds int) *ContinueAsNewManager[TState] {
	m.maxExecutionTime = seconds
	return m
}

// ShouldContinue checks if workflow should continue as new
func (m *ContinueAsNewManager[TState]) ShouldContinue(ctx workflow.Context, iterationCount int) bool {
	// Check iteration count
	if iterationCount >= m.maxIterations {
		workflow.GetLogger(ctx).Info("Max iterations reached", "count", iterationCount)
		return true
	}

	// Check history size
	info := workflow.GetInfo(ctx)
	if info.GetCurrentHistoryLength() >= m.maxHistorySize {
		workflow.GetLogger(ctx).Info("Max history size reached", "size", info.GetCurrentHistoryLength())
		return true
	}

	return false
}

// Continue continues the workflow as new with the given state
func (m *ContinueAsNewManager[TState]) Continue(ctx workflow.Context, state TState) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Continuing workflow as new")

	return workflow.NewContinueAsNewError(ctx, m.workflowFunc, state)
}

// PaginatedProcessor processes items in batches with automatic continue-as-new
type PaginatedProcessor[TItem any, TState any] struct {
	batchSize        int
	maxBatchesPerRun int
	processor        func(workflow.Context, []TItem, TState) (TState, error)
	workflowFunc     interface{}
}

// NewPaginatedProcessor creates a new paginated processor
func NewPaginatedProcessor[TItem any, TState any](
	workflowFunc interface{},
	processor func(workflow.Context, []TItem, TState) (TState, error),
) *PaginatedProcessor[TItem, TState] {
	return &PaginatedProcessor[TItem, TState]{
		batchSize:        100,
		maxBatchesPerRun: 10,
		processor:        processor,
		workflowFunc:     workflowFunc,
	}
}

// WithBatchSize sets the batch size
func (p *PaginatedProcessor[TItem, TState]) WithBatchSize(size int) *PaginatedProcessor[TItem, TState] {
	p.batchSize = size
	return p
}

// WithMaxBatchesPerRun sets max batches to process before continue-as-new
func (p *PaginatedProcessor[TItem, TState]) WithMaxBatchesPerRun(max int) *PaginatedProcessor[TItem, TState] {
	p.maxBatchesPerRun = max
	return p
}

// ProcessState contains the state for paginated processing
type ProcessState[TState any] struct {
	State         TState
	Offset        int
	TotalProcessed int
}

// Process processes all items with automatic pagination and continue-as-new
func (p *PaginatedProcessor[TItem, TState]) Process(
	ctx workflow.Context,
	items []TItem,
	initialState ProcessState[TState],
) (ProcessState[TState], error) {
	logger := workflow.GetLogger(ctx)
	state := initialState

	batchesProcessed := 0

	for state.Offset < len(items) {
		// Check if we should continue as new
		if batchesProcessed >= p.maxBatchesPerRun {
			logger.Info("Max batches per run reached, continuing as new",
				"processed", state.TotalProcessed,
				"offset", state.Offset)
			return state, workflow.NewContinueAsNewError(ctx, p.workflowFunc, items, state)
		}

		// Get next batch
		end := state.Offset + p.batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[state.Offset:end]
		logger.Info("Processing batch",
			"offset", state.Offset,
			"size", len(batch),
			"total", len(items))

		// Process batch
		newState, err := p.processor(ctx, batch, state.State)
		if err != nil {
			return state, err
		}

		state.State = newState
		state.Offset = end
		state.TotalProcessed += len(batch)
		batchesProcessed++
	}

	logger.Info("Processing completed", "totalProcessed", state.TotalProcessed)
	return state, nil
}

// PeriodicWorkflow manages workflows that run periodically with continue-as-new
type PeriodicWorkflow[TState any] struct {
	interval     int // in seconds
	maxRuns      int
	workflowFunc interface{}
}

// NewPeriodicWorkflow creates a new periodic workflow manager
func NewPeriodicWorkflow[TState any](workflowFunc interface{}, intervalSeconds int) *PeriodicWorkflow[TState] {
	return &PeriodicWorkflow[TState]{
		interval:     intervalSeconds,
		maxRuns:      100, // default
		workflowFunc: workflowFunc,
	}
}

// WithMaxRuns sets max runs before needing manual intervention
func (p *PeriodicWorkflow[TState]) WithMaxRuns(max int) *PeriodicWorkflow[TState] {
	p.maxRuns = max
	return p
}

// PeriodicState tracks periodic workflow state
type PeriodicState[TState any] struct {
	State    TState
	RunCount int
}

// RunPeriodic executes a task periodically with continue-as-new
func (p *PeriodicWorkflow[TState]) RunPeriodic(
	ctx workflow.Context,
	state PeriodicState[TState],
	task func(workflow.Context, TState) (TState, error),
) (PeriodicState[TState], error) {
	logger := workflow.GetLogger(ctx)

	logger.Info("Starting periodic run", "runCount", state.RunCount)

	// Execute the task
	newState, err := task(ctx, state.State)
	if err != nil {
		return state, err
	}

	state.State = newState
	state.RunCount++

	// Check if we've hit max runs
	if state.RunCount >= p.maxRuns {
		logger.Warn("Max runs reached, workflow will complete", "runCount", state.RunCount)
		return state, nil
	}

	// Sleep until next iteration
	logger.Info("Sleeping until next run", "interval", p.interval)
	err = workflow.Sleep(ctx, time.Duration(p.interval)*time.Second)
	if err != nil {
		return state, err
	}

	// Continue as new for next iteration
	logger.Info("Continuing as new for next iteration")
	return state, workflow.NewContinueAsNewError(ctx, p.workflowFunc, state)
}

// LongRunningIterator helps iterate over long-running processes
type LongRunningIterator[TState any] struct {
	manager      *ContinueAsNewManager[TState]
	workflowFunc interface{}
}

// NewLongRunningIterator creates a new long-running iterator
func NewLongRunningIterator[TState any](workflowFunc interface{}) *LongRunningIterator[TState] {
	return &LongRunningIterator[TState]{
		manager:      NewContinueAsNewManager[TState](workflowFunc),
		workflowFunc: workflowFunc,
	}
}

// WithMaxIterations sets max iterations per run
func (l *LongRunningIterator[TState]) WithMaxIterations(max int) *LongRunningIterator[TState] {
	l.manager.WithMaxIterations(max)
	return l
}

// IterateState tracks iteration state
type IterateState[TState any] struct {
	State          TState
	IterationCount int
	Completed      bool
}

// Iterate runs iterations with automatic continue-as-new
func (l *LongRunningIterator[TState]) Iterate(
	ctx workflow.Context,
	state IterateState[TState],
	iterator func(workflow.Context, TState, int) (TState, bool, error),
) (IterateState[TState], error) {
	logger := workflow.GetLogger(ctx)

	for {
		// Check if we should continue as new
		if l.manager.ShouldContinue(ctx, state.IterationCount) {
			logger.Info("Continuing as new", "iteration", state.IterationCount)
			return state, workflow.NewContinueAsNewError(ctx, l.workflowFunc, state)
		}

		// Execute iteration
		newState, completed, err := iterator(ctx, state.State, state.IterationCount)
		if err != nil {
			return state, err
		}

		state.State = newState
		state.IterationCount++

		if completed {
			state.Completed = true
			logger.Info("Iteration completed", "totalIterations", state.IterationCount)
			return state, nil
		}
	}
}
