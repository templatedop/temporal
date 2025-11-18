package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// UserRegistration represents input for user registration workflow
type UserRegistration struct {
	Email    string
	Username string
	FullName string
}

// RegistrationResult represents the output
type RegistrationResult struct {
	UserID    string
	Success   bool
	Message   string
	Timestamp time.Time
}

// UserRegistrationWorkflow is a simple workflow with typed input/output
func UserRegistrationWorkflow(ctx workflow.Context, input UserRegistration) (RegistrationResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("User registration started", "email", input.Email)

	// Simulate registration logic
	result := RegistrationResult{
		UserID:    fmt.Sprintf("user-%d", workflow.Now(ctx).Unix()),
		Success:   true,
		Message:   fmt.Sprintf("Welcome, %s!", input.FullName),
		Timestamp: workflow.Now(ctx),
	}

	logger.Info("User registration completed", "userID", result.UserID)
	return result, nil
}

// DemoRPCStyle demonstrates RPC-like workflow execution
func DemoRPCStyle() {
	ctx := context.Background()

	// Create client
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	taskQueue := "rpc-demo-queue"

	// Start worker
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(UserRegistrationWorkflow).
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

	log.Println("=== Demo: RPC-Style Workflow Execution ===")

	// Method 1: Direct typed execution (synchronous)
	log.Println("Method 1: ExecuteTypedWorkflow (synchronous)")
	opts1 := temporalworkflow.NewBuilder("rpc-demo-1", taskQueue).
		WithShortRunningDefaults().
		Build()

	input := UserRegistration{
		Email:    "alice@example.com",
		Username: "alice",
		FullName: "Alice Smith",
	}

	// This feels like calling a function!
	result, err := client.ExecuteTypedWorkflow[UserRegistration, RegistrationResult](
		ctx, c, opts1, UserRegistrationWorkflow, input,
	)

	if err != nil {
		log.Printf("Error: %v\n", err)
	} else {
		log.Printf("Result: UserID=%s, Message=%s\n\n", result.UserID, result.Message)
	}

	// Method 2: Async execution with typed handle
	log.Println("Method 2: ExecuteTypedWorkflowAsync (asynchronous)")
	opts2 := temporalworkflow.NewBuilder("rpc-demo-2", taskQueue).
		WithShortRunningDefaults().
		Build()

	input2 := UserRegistration{
		Email:    "bob@example.com",
		Username: "bob",
		FullName: "Bob Johnson",
	}

	run, err := client.ExecuteTypedWorkflowAsync[UserRegistration, RegistrationResult](
		ctx, c, opts2, UserRegistrationWorkflow, input2,
	)

	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	log.Printf("Workflow started: ID=%s, RunID=%s\n", run.GetID(), run.GetRunID())

	// Get result later
	result2, err := run.Get(ctx)
	if err != nil {
		log.Printf("Error: %v\n", err)
	} else {
		log.Printf("Result: UserID=%s, Message=%s\n\n", result2.UserID, result2.Message)
	}

	// Method 3: Using WorkflowClient with standalone function (highest-level abstraction)
	log.Println("Method 3: CallWorkflow (RPC-style)")
	wc := client.NewWorkflowClient(c)

	opts3 := temporalworkflow.NewBuilder("rpc-demo-3", taskQueue).
		WithShortRunningDefaults().
		Build()

	input3 := UserRegistration{
		Email:    "charlie@example.com",
		Username: "charlie",
		FullName: "Charlie Brown",
	}

	// Most concise - feels like a remote procedure call!
	result3, err := client.CallWorkflow[UserRegistration, RegistrationResult](
		ctx, wc, opts3, UserRegistrationWorkflow, input3,
	)

	if err != nil {
		log.Printf("Error: %v\n", err)
	} else {
		log.Printf("Result: UserID=%s, Message=%s\n\n", result3.UserID, result3.Message)
	}

	// Method 4: Using WorkflowStub (reusable)
	log.Println("Method 4: WorkflowStub (reusable)")
	opts4 := temporalworkflow.NewBuilder("rpc-demo-4", taskQueue).
		WithShortRunningDefaults().
		Build()

	stub := client.NewWorkflowStub[UserRegistration, RegistrationResult](
		c, UserRegistrationWorkflow, opts4,
	)

	input4 := UserRegistration{
		Email:    "diana@example.com",
		Username: "diana",
		FullName: "Diana Prince",
	}

	result4, err := stub.Execute(ctx, input4)
	if err != nil {
		log.Printf("Error: %v\n", err)
	} else {
		log.Printf("Result: UserID=%s, Message=%s\n\n", result4.UserID, result4.Message)
	}

	log.Println("All RPC-style executions completed!")

	w.Stop()
}

// This is a separate example - uncomment main to run
/*
func main() {
	DemoRPCStyle()
}
*/
