package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	"github.com/templatedop/temporal/continueasnew"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// PerformanceReviewState represents the state of an ongoing review process
type PerformanceReviewState struct {
	EmployeeID       string
	ReviewCycle      string
	Completed        bool
	ReviewsCollected int
	TotalReviews     int
	AverageScore     float64
	Status           string
	Version          int
}

// ReviewData represents a single review
type ReviewData struct {
	ReviewerID string
	EmployeeID string
	Score      float64
	Comments   string
	Timestamp  time.Time
}

// Annual Performance Review Workflow with continue-as-new
// This workflow runs for a full year, collecting reviews periodically
func AnnualPerformanceReviewWorkflow(
	ctx workflow.Context,
	state continueasnew.PeriodicState[PerformanceReviewState],
) (continueasnew.PeriodicState[PerformanceReviewState], error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Annual performance review iteration",
		"employeeID", state.State.EmployeeID,
		"runCount", state.RunCount)

	// Use versioning for safe evolution of the workflow
	version := workflow.GetVersion(ctx, "review-process-v2", workflow.DefaultVersion, 2)
	state.State.Version = int(version)

	// Create periodic workflow manager
	periodic := continueasnew.NewPeriodicWorkflow[PerformanceReviewState](
		AnnualPerformanceReviewWorkflow,
		7*24*3600, // Weekly iterations (7 days)
	).WithMaxRuns(52) // Run for 52 weeks (1 year)

	// Task to execute each period
	task := func(ctx workflow.Context, reviewState PerformanceReviewState) (PerformanceReviewState, error) {
		logger.Info("Collecting reviews for week", "week", state.RunCount+1)

		if version == 2 {
			// New version with enhanced review collection
			reviewState = collectReviewsV2(ctx, reviewState)
		} else {
			// Old version
			reviewState = collectReviewsV1(ctx, reviewState)
		}

		// Check if review cycle is complete
		if reviewState.ReviewsCollected >= reviewState.TotalReviews {
			reviewState.Completed = true
			reviewState.Status = "completed"
			logger.Info("Review cycle completed", "totalReviews", reviewState.ReviewsCollected)
		}

		return reviewState, nil
	}

	// Run periodic workflow with continue-as-new
	return periodic.RunPeriodic(ctx, state, task)
}

// collectReviewsV1 - Original review collection (legacy)
func collectReviewsV1(ctx workflow.Context, state PerformanceReviewState) PerformanceReviewState {
	logger := workflow.GetLogger(ctx)
	logger.Info("Using V1 review collection")

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var reviews []ReviewData
	err := workflow.ExecuteActivity(ctx, CollectReviewsActivityV1, state.EmployeeID).Get(ctx, &reviews)
	if err != nil {
		logger.Error("Failed to collect reviews", "error", err)
		return state
	}

	state.ReviewsCollected += len(reviews)

	// Calculate average
	total := state.AverageScore * float64(state.ReviewsCollected-len(reviews))
	for _, review := range reviews {
		total += review.Score
	}
	state.AverageScore = total / float64(state.ReviewsCollected)

	return state
}

// collectReviewsV2 - Enhanced review collection (new)
func collectReviewsV2(ctx workflow.Context, state PerformanceReviewState) PerformanceReviewState {
	logger := workflow.GetLogger(ctx)
	logger.Info("Using V2 enhanced review collection")

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var reviews []ReviewData
	err := workflow.ExecuteActivity(ctx, CollectReviewsActivityV2, state.EmployeeID).Get(ctx, &reviews)
	if err != nil {
		logger.Error("Failed to collect reviews", "error", err)
		return state
	}

	state.ReviewsCollected += len(reviews)

	// Enhanced averaging with weighted scores
	total := state.AverageScore * float64(state.ReviewsCollected-len(reviews))
	for _, review := range reviews {
		// V2: Apply recency weighting
		weight := 1.0 + (float64(state.ReviewsCollected) * 0.1)
		total += review.Score * weight
	}
	state.AverageScore = total / float64(state.ReviewsCollected)

	return state
}

// OnboardingInput combines tasks and state for onboarding workflow
type OnboardingInput struct {
	Tasks []string
	State continueasnew.ProcessState[map[string]bool]
}

// Employee Onboarding Workflow with pagination
// This workflow processes large batches of onboarding tasks
func EmployeeOnboardingWorkflow(
	ctx workflow.Context,
	input OnboardingInput,
) (continueasnew.ProcessState[map[string]bool], error) {
	tasks := input.Tasks
	state := input.State
	logger := workflow.GetLogger(ctx)
	logger.Info("Employee onboarding workflow",
		"totalTasks", len(tasks),
		"offset", state.Offset,
		"processed", state.TotalProcessed)

	if state.State == nil {
		state.State = make(map[string]bool)
	}

	// Process tasks in batches with manual continue-as-new logic
	batchSize := 10
	maxBatchesPerRun := 5
	batchesProcessed := 0

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	for state.Offset < len(tasks) {
		// Check if we should continue as new
		if batchesProcessed >= maxBatchesPerRun {
			logger.Info("Max batches per run reached, continuing as new",
				"processed", state.TotalProcessed,
				"offset", state.Offset)
			return state, workflow.NewContinueAsNewError(ctx, EmployeeOnboardingWorkflow, OnboardingInput{
				Tasks: tasks,
				State: state,
			})
		}

		// Get next batch
		end := state.Offset + batchSize
		if end > len(tasks) {
			end = len(tasks)
		}

		batch := tasks[state.Offset:end]
		logger.Info("Processing batch",
			"offset", state.Offset,
			"size", len(batch),
			"total", len(tasks))

		// Process each task in batch
		for _, task := range batch {
			var completed bool
			err := workflow.ExecuteActivity(ctx, ProcessOnboardingTaskActivity, task).Get(ctx, &completed)
			if err != nil {
				logger.Error("Task failed", "task", task, "error", err)
				state.State[task] = false
			} else {
				state.State[task] = completed
			}
		}

		state.Offset = end
		state.TotalProcessed += len(batch)
		batchesProcessed++
	}

	logger.Info("Processing completed", "totalProcessed", state.TotalProcessed)
	return state, nil
}

// Activities

func CollectReviewsActivityV1(ctx context.Context, employeeID string) ([]ReviewData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Collecting reviews (V1)", "employeeID", employeeID)

	// Simulate collecting reviews
	time.Sleep(100 * time.Millisecond)

	reviews := []ReviewData{
		{ReviewerID: "MGR-001", EmployeeID: employeeID, Score: 4.5, Comments: "Good work", Timestamp: time.Now()},
		{ReviewerID: "PEER-001", EmployeeID: employeeID, Score: 4.2, Comments: "Team player", Timestamp: time.Now()},
	}

	return reviews, nil
}

func CollectReviewsActivityV2(ctx context.Context, employeeID string) ([]ReviewData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Collecting reviews (V2 - Enhanced)", "employeeID", employeeID)

	// Simulate collecting reviews with enhanced data
	time.Sleep(100 * time.Millisecond)

	reviews := []ReviewData{
		{ReviewerID: "MGR-001", EmployeeID: employeeID, Score: 4.7, Comments: "Excellent progress", Timestamp: time.Now()},
		{ReviewerID: "PEER-001", EmployeeID: employeeID, Score: 4.5, Comments: "Great collaboration", Timestamp: time.Now()},
		{ReviewerID: "PEER-002", EmployeeID: employeeID, Score: 4.3, Comments: "Helpful and supportive", Timestamp: time.Now()},
	}

	return reviews, nil
}

func ProcessOnboardingTaskActivity(ctx context.Context, task string) (bool, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing onboarding task", "task", task)

	// Simulate task processing
	time.Sleep(50 * time.Millisecond)

	return true, nil
}

func main() {
	ctx := context.Background()

	// Create client
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	taskQueue := "hr-queue"

	// Start worker
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(AnnualPerformanceReviewWorkflow).
		RegisterWorkflow(EmployeeOnboardingWorkflow).
		RegisterActivity(CollectReviewsActivityV1).
		RegisterActivity(CollectReviewsActivityV2).
		RegisterActivity(ProcessOnboardingTaskActivity).
		Build()

	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	go func() {
		if err := w.Run(ctx); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// Example 1: Performance Review (demonstrates versioning and periodic continue-as-new)
	log.Println("\n=== Starting Annual Performance Review ===")

	reviewState := continueasnew.PeriodicState[PerformanceReviewState]{
		State: PerformanceReviewState{
			EmployeeID:   "EMP-12345",
			ReviewCycle:  "2024",
			TotalReviews: 10,
			Status:       "in_progress",
		},
		RunCount: 0,
	}

	opts1 := temporalworkflow.NewBuilder("perf-review-EMP-12345-2024", taskQueue).
		WithLongRunningDefaults().
		WithSearchAttribute("EmployeeID", "EMP-12345").
		Build()

	run1, err := client.ExecuteTypedWorkflowAsync[
		continueasnew.PeriodicState[PerformanceReviewState],
		continueasnew.PeriodicState[PerformanceReviewState],
	](ctx, c, opts1, AnnualPerformanceReviewWorkflow, reviewState)

	if err != nil {
		log.Fatalf("Failed to start review workflow: %v", err)
	}

	log.Printf("Performance review started: WorkflowID=%s", run1.GetID())

	// Example 2: Employee Onboarding (demonstrates pagination with continue-as-new)
	log.Println("\n=== Starting Employee Onboarding ===")

	// Large list of onboarding tasks
	tasks := make([]string, 100)
	for i := 0; i < 100; i++ {
		tasks[i] = fmt.Sprintf("TASK-%03d", i+1)
	}

	onboardingState := continueasnew.ProcessState[map[string]bool]{
		State:          make(map[string]bool),
		Offset:         0,
		TotalProcessed: 0,
	}

	opts2 := temporalworkflow.NewBuilder("onboarding-EMP-99999", taskQueue).
		WithDefaultTimeouts().
		WithSearchAttribute("EmployeeID", "EMP-99999").
		Build()

	run2, err := client.ExecuteTypedWorkflowAsync[
		OnboardingInput,
		continueasnew.ProcessState[map[string]bool],
	](ctx, c, opts2, EmployeeOnboardingWorkflow, OnboardingInput{
		Tasks: tasks,
		State: onboardingState,
	})

	if err != nil {
		log.Fatalf("Failed to start onboarding workflow: %v", err)
	}

	log.Printf("Onboarding started: WorkflowID=%s", run2.GetID())

	// Wait for onboarding to complete (it's shorter)
	log.Println("\n=== Waiting for onboarding completion ===")
	result2, err := run2.Get(ctx)
	if err != nil {
		log.Printf("Onboarding failed: %v", err)
	} else {
		log.Printf("Onboarding completed: TotalProcessed=%d, Offset=%d",
			result2.TotalProcessed, result2.Offset)
		log.Printf("Tasks completed: %d/%d", len(result2.State), len(tasks))
	}

	log.Println("\nNote: Performance review workflow will continue running weekly.")
	log.Println("It uses continue-as-new to avoid history size limits.")

	w.Stop()
}
