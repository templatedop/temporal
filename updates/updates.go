package updates

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	temporalclient "github.com/templatedop/temporal/client"
)

// UpdateHandler handles a workflow update
type UpdateHandler[TInput any, TOutput any] func(ctx workflow.Context, input TInput) (TOutput, error)

// Validator validates update input before execution
type Validator[TInput any] func(input TInput) error

// UpdateRegistry manages workflow update handlers
type UpdateRegistry struct {
	handlers map[string]interface{}
}

// NewUpdateRegistry creates a new update registry
func NewUpdateRegistry() *UpdateRegistry {
	return &UpdateRegistry{
		handlers: make(map[string]interface{}),
	}
}

// Register registers an update handler
func Register[TInput any, TOutput any](
	ctx workflow.Context,
	updateName string,
	handler UpdateHandler[TInput, TOutput],
) error {
	return workflow.SetUpdateHandler(ctx, updateName, func(ctx workflow.Context, input TInput) (TOutput, error) {
		logger := workflow.GetLogger(ctx)
		logger.Info("Executing update", "updateName", updateName)

		result, err := handler(ctx, input)
		if err != nil {
			logger.Error("Update failed", "updateName", updateName, "error", err)
			return result, err
		}

		logger.Info("Update completed", "updateName", updateName)
		return result, nil
	})
}

// RegisterWithValidator registers an update handler with validation
func RegisterWithValidator[TInput any, TOutput any](
	ctx workflow.Context,
	updateName string,
	validator Validator[TInput],
	handler UpdateHandler[TInput, TOutput],
) error {
	return workflow.SetUpdateHandlerWithOptions(
		ctx,
		updateName,
		func(ctx workflow.Context, input TInput) (TOutput, error) {
			logger := workflow.GetLogger(ctx)
			logger.Info("Executing update with validation", "updateName", updateName)

			result, err := handler(ctx, input)
			if err != nil {
				logger.Error("Update failed", "updateName", updateName, "error", err)
				return result, err
			}

			logger.Info("Update completed", "updateName", updateName)
			return result, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, input TInput) error {
				return validator(input)
			},
		},
	)
}

// Client-side update execution

// ExecuteUpdate sends an update to a workflow with type-safe parameters
func ExecuteUpdate[TInput any, TOutput any](
	ctx context.Context,
	c *temporalclient.Client,
	workflowID string,
	runID string,
	updateName string,
	input TInput,
) (TOutput, error) {
	var result TOutput

	updateHandle, err := c.Underlying().UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        runID,
		UpdateName:   updateName,
		Args:         []interface{}{input},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})

	if err != nil {
		return result, fmt.Errorf("failed to execute update: %w", err)
	}

	err = updateHandle.Get(ctx, &result)
	if err != nil {
		return result, fmt.Errorf("failed to get update result: %w", err)
	}

	return result, nil
}

// ExecuteUpdateAsync sends an update without waiting for completion
func ExecuteUpdateAsync[TInput any](
	ctx context.Context,
	c *temporalclient.Client,
	workflowID string,
	runID string,
	updateName string,
	input TInput,
) (string, error) {
	updateHandle, err := c.Underlying().UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        runID,
		UpdateName:   updateName,
		Args:         []interface{}{input},
		WaitForStage: client.WorkflowUpdateStageAccepted,
	})

	if err != nil {
		return "", fmt.Errorf("failed to execute async update: %w", err)
	}

	return updateHandle.UpdateID(), nil
}

// UpdateBuilder provides a fluent interface for building updates
type UpdateBuilder[TInput any, TOutput any] struct {
	workflowID string
	runID      string
	updateName string
	input      TInput
	waitForCompletion bool
}

// NewUpdateBuilder creates a new update builder
func NewUpdateBuilder[TInput any, TOutput any](workflowID, updateName string) *UpdateBuilder[TInput, TOutput] {
	return &UpdateBuilder[TInput, TOutput]{
		workflowID:        workflowID,
		updateName:        updateName,
		waitForCompletion: true,
	}
}

// WithRunID sets the run ID
func (b *UpdateBuilder[TInput, TOutput]) WithRunID(runID string) *UpdateBuilder[TInput, TOutput] {
	b.runID = runID
	return b
}

// WithInput sets the input
func (b *UpdateBuilder[TInput, TOutput]) WithInput(input TInput) *UpdateBuilder[TInput, TOutput] {
	b.input = input
	return b
}

// Async sets whether to wait for completion
func (b *UpdateBuilder[TInput, TOutput]) Async() *UpdateBuilder[TInput, TOutput] {
	b.waitForCompletion = false
	return b
}

// Execute executes the update
func (b *UpdateBuilder[TInput, TOutput]) Execute(ctx context.Context, c *temporalclient.Client) (TOutput, error) {
	if b.waitForCompletion {
		return ExecuteUpdate[TInput, TOutput](ctx, c, b.workflowID, b.runID, b.updateName, b.input)
	}

	var result TOutput
	_, err := ExecuteUpdateAsync[TInput](ctx, c, b.workflowID, b.runID, b.updateName, b.input)
	return result, err
}

// Common update patterns for insurance

// ClaimUpdate represents an update to an insurance claim
type ClaimUpdate struct {
	ClaimID     string
	Status      string
	Amount      float64
	Notes       string
	UpdatedBy   string
}

// PolicyUpdate represents an update to an insurance policy
type PolicyUpdate struct {
	PolicyID    string
	Coverage    float64
	Premium     float64
	Beneficiary string
	UpdatedBy   string
}

// UpdateClaimStatus is a common pattern for updating claim status
func UpdateClaimStatus(ctx workflow.Context, update ClaimUpdate) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Updating claim status", "claimID", update.ClaimID, "newStatus", update.Status)

	// Workflow would update its state here
	// This is just a helper pattern

	return fmt.Sprintf("Claim %s updated to %s", update.ClaimID, update.Status), nil
}

// UpdatePolicyDetails is a common pattern for updating policy details
func UpdatePolicyDetails(ctx workflow.Context, update PolicyUpdate) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Updating policy details", "policyID", update.PolicyID)

	// Workflow would update its state here

	return fmt.Sprintf("Policy %s updated", update.PolicyID), nil
}

// Validators

// ValidateClaimUpdate validates claim update input
func ValidateClaimUpdate(update ClaimUpdate) error {
	if update.ClaimID == "" {
		return fmt.Errorf("claim ID is required")
	}
	if update.Status == "" {
		return fmt.Errorf("status is required")
	}
	if update.Amount < 0 {
		return fmt.Errorf("amount cannot be negative")
	}
	return nil
}

// ValidatePolicyUpdate validates policy update input
func ValidatePolicyUpdate(update PolicyUpdate) error {
	if update.PolicyID == "" {
		return fmt.Errorf("policy ID is required")
	}
	if update.Coverage < 0 {
		return fmt.Errorf("coverage cannot be negative")
	}
	if update.Premium < 0 {
		return fmt.Errorf("premium cannot be negative")
	}
	return nil
}
