package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// GreetingWorkflow is a simple workflow that greets a person
func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("GreetingWorkflow started", "name", name)

	// Configure activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Execute the greeting activity
	var greeting string
	err := workflow.ExecuteActivity(ctx, GreetActivity, name).Get(ctx, &greeting)
	if err != nil {
		logger.Error("Activity failed", "Error", err)
		return "", err
	}

	logger.Info("GreetingWorkflow completed", "greeting", greeting)
	return greeting, nil
}

// GreetActivity is a simple activity that creates a greeting message
func GreetActivity(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("GreetActivity called", "name", name)

	greeting := fmt.Sprintf("Hello, %s! Welcome to Temporal.", name)
	return greeting, nil
}

func main() {
	// Create a Temporal client with default settings
	ctx := context.Background()
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Build and start a worker
	taskQueue := "greeting-queue"

	// Use the worker builder for convenient setup
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(GreetingWorkflow).
		RegisterActivity(GreetActivity).
		WithLogging(true).
		Build()

	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	// Start the worker in a goroutine
	go func() {
		if err := w.Run(ctx); err != nil {
			log.Fatalf("Worker failed: %v", err)
		}
	}()

	// Give the worker a moment to start
	time.Sleep(1 * time.Second)

	// Execute a workflow using the fluent builder
	workflowOpts := temporalworkflow.NewBuilder("greeting-workflow-"+time.Now().Format("20060102150405"), taskQueue).
		WithShortRunningDefaults().
		WithModerateRetry().
		Build()

	workflowRun, err := c.ExecuteWorkflow(ctx, workflowOpts, GreetingWorkflow, "World")
	if err != nil {
		log.Fatalf("Failed to execute workflow: %v", err)
	}

	log.Printf("Started workflow with ID: %s, RunID: %s", workflowRun.GetID(), workflowRun.GetRunID())

	// Wait for the workflow to complete
	var result string
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Fatalf("Failed to get workflow result: %v", err)
	}

	log.Printf("Workflow result: %s", result)

	// Stop the worker
	w.Stop()
}
