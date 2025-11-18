package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	"github.com/templatedop/temporal/patterns/child"
	"github.com/templatedop/temporal/updates"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// ClaimData represents an insurance claim
type ClaimData struct {
	ClaimID      string
	PolicyID     string
	ClaimantName string
	Amount       float64
	Status       string
	Description  string
	Documents    []string
}

// ClaimProcessingWorkflow orchestrates claim processing with child workflows
func ClaimProcessingWorkflow(ctx workflow.Context, claim ClaimData) (ClaimData, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting claim processing", "claimID", claim.ClaimID)

	// Set initial status
	claim.Status = "received"

	// Register update handlers for real-time claim updates
	err := updates.Register[updates.ClaimUpdate, string](ctx, "updateClaimStatus",
		func(ctx workflow.Context, update updates.ClaimUpdate) (string, error) {
			logger.Info("Received claim update", "claimID", update.ClaimID, "newStatus", update.Status)
			claim.Status = update.Status
			claim.Amount = update.Amount
			return fmt.Sprintf("Claim %s updated to %s", update.ClaimID, update.Status), nil
		})

	if err != nil {
		return claim, err
	}

	// Use child workflows for parallel processing
	fanout := child.NewFanOutFanIn[string, string](VerifyDocumentWorkflow).
		WithConcurrency(3).
		WithErrorHandling(child.ContinueOnError)

	logger.Info("Verifying documents", "count", len(claim.Documents))
	docResults, err := fanout.Execute(ctx, claim.Documents)
	if err != nil {
		logger.Error("Document verification failed", "error", err)
		claim.Status = "rejected"
		return claim, err
	}

	logger.Info("Documents verified", "successful", docResults.Successful, "failed", docResults.Failed)

	// Update status
	claim.Status = "documents_verified"

	// Run sequential child workflows for claim assessment
	sequential := child.NewSequential[ClaimData, ClaimData]().
		AddWorkflow(AssessDamageWorkflow).
		AddWorkflow(CalculatePayoutWorkflow).
		AsPipeline(true)

	logger.Info("Running claim assessment pipeline")
	assessmentResult, err := sequential.Execute(ctx, claim)
	if err != nil {
		logger.Error("Assessment failed", "error", err)
		claim.Status = "assessment_failed"
		return claim, err
	}

	claim = assessmentResult.Final
	claim.Status = "assessed"

	logger.Info("Claim processing completed", "finalAmount", claim.Amount, "status", claim.Status)
	return claim, nil
}

// VerifyDocumentWorkflow verifies a single document
func VerifyDocumentWorkflow(ctx workflow.Context, documentID string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Verifying document", "documentID", documentID)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, VerifyDocumentActivity, documentID).Get(ctx, &result)
	return result, err
}

// AssessDamageWorkflow assesses claim damage
func AssessDamageWorkflow(ctx workflow.Context, claim ClaimData) (ClaimData, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Assessing damage", "claimID", claim.ClaimID)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var assessedClaim ClaimData
	err := workflow.ExecuteActivity(ctx, AssessDamageActivity, claim).Get(ctx, &assessedClaim)
	return assessedClaim, err
}

// CalculatePayoutWorkflow calculates final payout
func CalculatePayoutWorkflow(ctx workflow.Context, claim ClaimData) (ClaimData, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Calculating payout", "claimID", claim.ClaimID)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var finalClaim ClaimData
	err := workflow.ExecuteActivity(ctx, CalculatePayoutActivity, claim).Get(ctx, &finalClaim)
	return finalClaim, err
}

// Activities

func VerifyDocumentActivity(ctx context.Context, documentID string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Verifying document", "documentID", documentID)

	// Simulate document verification
	time.Sleep(100 * time.Millisecond)

	return fmt.Sprintf("Document %s verified", documentID), nil
}

func AssessDamageActivity(ctx context.Context, claim ClaimData) (ClaimData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Assessing damage for claim", "claimID", claim.ClaimID)

	// Simulate damage assessment
	time.Sleep(200 * time.Millisecond)

	// Update claim with assessment
	claim.Amount = claim.Amount * 0.9 // 10% reduction for assessment
	return claim, nil
}

func CalculatePayoutActivity(ctx context.Context, claim ClaimData) (ClaimData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Calculating payout", "claimID", claim.ClaimID)

	// Simulate payout calculation
	time.Sleep(150 * time.Millisecond)

	// Final payout calculation
	claim.Amount = claim.Amount * 0.95 // 5% processing fee
	return claim, nil
}

func main() {
	ctx := context.Background()

	// Create client
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	taskQueue := "insurance-claims-queue"

	// Start worker
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(ClaimProcessingWorkflow).
		RegisterWorkflow(VerifyDocumentWorkflow).
		RegisterWorkflow(AssessDamageWorkflow).
		RegisterWorkflow(CalculatePayoutWorkflow).
		RegisterActivity(VerifyDocumentActivity).
		RegisterActivity(AssessDamageActivity).
		RegisterActivity(CalculatePayoutActivity).
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

	// Execute claim processing workflow
	claim := ClaimData{
		ClaimID:      "CLM-2024-001",
		PolicyID:     "POL-123456",
		ClaimantName: "John Doe",
		Amount:       50000.00,
		Description:  "Water damage from burst pipe",
		Documents:    []string{"DOC-001", "DOC-002", "DOC-003"},
	}

	opts := temporalworkflow.NewBuilder("claim-"+claim.ClaimID, taskQueue).
		WithDefaultTimeouts().
		WithModerateRetry().
		WithSearchAttribute("ClaimID", claim.ClaimID).
		WithSearchAttribute("PolicyID", claim.PolicyID).
		Build()

	run, err := client.ExecuteTypedWorkflowAsync[ClaimData, ClaimData](
		ctx, c, opts, ClaimProcessingWorkflow, claim,
	)

	if err != nil {
		log.Fatalf("Failed to start workflow: %v", err)
	}

	log.Printf("Claim processing started: WorkflowID=%s, RunID=%s", run.GetID(), run.GetRunID())

	// Simulate real-time update
	time.Sleep(2 * time.Second)
	log.Println("\n=== Sending claim status update ===")

	update := updates.ClaimUpdate{
		ClaimID:   claim.ClaimID,
		Status:    "under_review",
		Amount:    50000.00,
		Notes:     "Additional review required",
		UpdatedBy: "adjuster@insurance.com",
	}

	updateResult, err := updates.ExecuteUpdate[updates.ClaimUpdate, string](
		ctx, c, run.GetID(), run.GetRunID(), "updateClaimStatus", update,
	)

	if err != nil {
		log.Printf("Update failed: %v", err)
	} else {
		log.Printf("Update result: %s", updateResult)
	}

	// Wait for completion
	result, err := run.Get(ctx)
	if err != nil {
		log.Fatalf("Workflow failed: %v", err)
	}

	log.Printf("\n=== Claim Processing Completed ===")
	log.Printf("Claim ID: %s", result.ClaimID)
	log.Printf("Final Status: %s", result.Status)
	log.Printf("Final Amount: $%.2f", result.Amount)

	w.Stop()
}
