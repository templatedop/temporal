package patterns

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// ApprovalRequest represents a request for approval
type ApprovalRequest struct {
	RequestID   string
	Requestor   string
	Description string
	Data        interface{}
}

// ApprovalResponse represents the approval decision
type ApprovalResponse struct {
	Approved  bool
	Approver  string
	Comment   string
	Timestamp time.Time
}

// ApprovalConfig configures the approval workflow
type ApprovalConfig struct {
	Timeout           time.Duration
	ReminderInterval  time.Duration
	EscalationTimeout time.Duration
	DefaultApprover   string
}

// DefaultApprovalConfig returns default approval configuration
func DefaultApprovalConfig() *ApprovalConfig {
	return &ApprovalConfig{
		Timeout:           24 * time.Hour,
		ReminderInterval:  4 * time.Hour,
		EscalationTimeout: 24 * time.Hour,
	}
}

// ApprovalWorkflow implements a human-in-the-loop approval pattern
func ApprovalWorkflow(ctx workflow.Context, request ApprovalRequest, config *ApprovalConfig) (*ApprovalResponse, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting approval workflow", "requestID", request.RequestID)

	if config == nil {
		config = DefaultApprovalConfig()
	}

	// Set up signal channel for approval
	approvalChannel := workflow.GetSignalChannel(ctx, "approval")

	var response ApprovalResponse
	approved := false

	// Send notification activity
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	notifyCtx := workflow.WithActivityOptions(ctx, ao)

	// Notify approver (this would typically send an email, slack message, etc.)
	err := workflow.ExecuteActivity(notifyCtx, "NotifyApprover", request).Get(notifyCtx, nil)
	if err != nil {
		logger.Warn("Failed to send notification", "error", err)
		// Continue anyway - don't fail the workflow
	}

	// Wait for approval with timeout and reminders
	startTime := workflow.Now(ctx)

	for !approved {
		selector := workflow.NewSelector(ctx)

		// Case 1: Approval received
		selector.AddReceive(approvalChannel, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &response)
			approved = true
			logger.Info("Approval received", "approved", response.Approved, "approver", response.Approver)
		})

		// Case 2: Reminder timer
		reminderTimer := workflow.NewTimer(ctx, config.ReminderInterval)
		selector.AddFuture(reminderTimer, func(f workflow.Future) {
			logger.Info("Sending reminder")
			// Send reminder notification
			err := workflow.ExecuteActivity(notifyCtx, "SendReminder", request).Get(notifyCtx, nil)
			if err != nil {
				logger.Warn("Failed to send reminder", "error", err)
			}
		})

		// Case 3: Timeout
		if workflow.Now(ctx).Sub(startTime) > config.Timeout {
			logger.Warn("Approval timeout reached")

			if config.DefaultApprover != "" {
				// Auto-approve or escalate
				response = ApprovalResponse{
					Approved:  false,
					Approver:  config.DefaultApprover,
					Comment:   "Auto-rejected due to timeout",
					Timestamp: workflow.Now(ctx),
				}
				approved = true
			} else {
				return nil, fmt.Errorf("approval timeout after %v", config.Timeout)
			}
		}

		if !approved {
			selector.Select(ctx)
		}
	}

	response.Timestamp = workflow.Now(ctx)
	return &response, nil
}

// MultiStageApprovalWorkflow implements multi-stage approval
func MultiStageApprovalWorkflow(ctx workflow.Context, request ApprovalRequest, approvers []string) ([]ApprovalResponse, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting multi-stage approval", "stages", len(approvers))

	responses := make([]ApprovalResponse, 0, len(approvers))

	for i, approver := range approvers {
		logger.Info("Requesting approval from stage", "stage", i+1, "approver", approver)

		// Create a stage-specific request
		stageRequest := request
		stageRequest.RequestID = fmt.Sprintf("%s-stage-%d", request.RequestID, i+1)

		config := DefaultApprovalConfig()
		config.DefaultApprover = approver

		response, err := ApprovalWorkflow(ctx, stageRequest, config)
		if err != nil {
			return responses, fmt.Errorf("stage %d approval failed: %w", i+1, err)
		}

		responses = append(responses, *response)

		if !response.Approved {
			logger.Info("Approval rejected at stage", "stage", i+1)
			return responses, fmt.Errorf("approval rejected by %s: %s", response.Approver, response.Comment)
		}

		logger.Info("Stage approved", "stage", i+1)
	}

	logger.Info("All stages approved")
	return responses, nil
}

// ParallelApprovalWorkflow requires approval from multiple approvers in parallel
func ParallelApprovalWorkflow(ctx workflow.Context, request ApprovalRequest, approvers []string, requiredApprovals int) ([]ApprovalResponse, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting parallel approval", "approvers", len(approvers), "required", requiredApprovals)

	if requiredApprovals > len(approvers) {
		return nil, fmt.Errorf("required approvals (%d) cannot exceed number of approvers (%d)", requiredApprovals, len(approvers))
	}

	// Start approval workflows in parallel
	futures := make([]workflow.Future, len(approvers))
	for i, approver := range approvers {
		stageRequest := request
		stageRequest.RequestID = fmt.Sprintf("%s-approver-%d", request.RequestID, i)

		config := DefaultApprovalConfig()
		config.DefaultApprover = approver

		futures[i] = workflow.ExecuteChildWorkflow(ctx, ApprovalWorkflow, stageRequest, config)
	}

	// Collect responses
	responses := make([]ApprovalResponse, 0)
	approvedCount := 0
	rejectedCount := 0

	for i, future := range futures {
		var response ApprovalResponse
		err := future.Get(ctx, &response)

		if err != nil {
			logger.Warn("Approval request failed", "approver", approvers[i], "error", err)
			rejectedCount++
		} else {
			responses = append(responses, response)
			if response.Approved {
				approvedCount++
			} else {
				rejectedCount++
			}
		}

		// Early termination if we can't reach required approvals
		remaining := len(approvers) - i - 1
		if approvedCount+remaining < requiredApprovals {
			return responses, fmt.Errorf("insufficient approvals: got %d, required %d", approvedCount, requiredApprovals)
		}
	}

	if approvedCount >= requiredApprovals {
		logger.Info("Parallel approval succeeded", "approved", approvedCount, "required", requiredApprovals)
		return responses, nil
	}

	return responses, fmt.Errorf("insufficient approvals: got %d, required %d", approvedCount, requiredApprovals)
}
