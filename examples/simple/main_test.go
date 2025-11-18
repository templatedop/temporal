package main

import (
	"testing"

	"github.com/stretchr/testify/mock"

	temporaltesting "github.com/templatedop/temporal/testing"
)

func TestGreetingWorkflow(t *testing.T) {
	wt := temporaltesting.NewWorkflowTest(t)

	// Register workflow and activity
	wt.RegisterWorkflow(GreetingWorkflow)
	wt.RegisterActivity(GreetActivity)

	// Execute the workflow
	wt.ExecuteWorkflow(GreetingWorkflow, "TestUser")

	// Verify workflow completed
	if !wt.IsWorkflowCompleted() {
		t.Fatal("Workflow did not complete")
	}

	// Verify no errors
	if err := wt.GetWorkflowError(); err != nil {
		t.Fatalf("Workflow failed: %v", err)
	}

	// Get and verify result
	var result string
	err := wt.GetWorkflowResult(&result)
	if err != nil {
		t.Fatalf("Failed to get result: %v", err)
	}

	expected := "Hello, TestUser! Welcome to Temporal."
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestGreetActivity(t *testing.T) {
	at := temporaltesting.NewActivityTest(t)

	// Use the convenience method to execute and decode in one step
	var result string
	err := at.ExecuteActivityAndDecode(&result, GreetActivity, "TestUser")
	if err != nil {
		t.Fatalf("Activity failed: %v", err)
	}

	expected := "Hello, TestUser! Welcome to Temporal."
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestGreetingWorkflowWithMock(t *testing.T) {
	wt := temporaltesting.NewWorkflowTest(t)

	// Register workflow
	wt.RegisterWorkflow(GreetingWorkflow)

	// Mock the activity to return a custom message
	// Note: activities receive a context as the first parameter
	wt.OnActivity(GreetActivity, mock.Anything, "TestUser").Return("Mocked greeting", nil)

	// Execute the workflow
	wt.ExecuteWorkflow(GreetingWorkflow, "TestUser")

	// Verify result
	var result string
	err := wt.GetWorkflowResult(&result)
	if err != nil {
		t.Fatalf("Failed to get result: %v", err)
	}

	if result != "Mocked greeting" {
		t.Errorf("Expected mocked greeting, got %q", result)
	}

	// Verify mock expectations
	wt.AssertExpectations(t)
}
