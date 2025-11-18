package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// OrderStatus represents the state of an order
type OrderStatus struct {
	OrderID   string
	Status    string
	Total     float64
	UpdatedAt time.Time
}

// ProcessOrderWorkflow demonstrates a more complex workflow with signals and queries
func ProcessOrderWorkflow(ctx workflow.Context, orderID string) (*OrderStatus, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ProcessOrderWorkflow started", "orderID", orderID)

	status := &OrderStatus{
		OrderID:   orderID,
		Status:    "created",
		Total:     0,
		UpdatedAt: workflow.Now(ctx),
	}

	// Set up query handler
	err := workflow.SetQueryHandler(ctx, "status", func() (*OrderStatus, error) {
		return status, nil
	})
	if err != nil {
		return nil, err
	}

	// Set up signal handler for cancellation
	cancelRequested := false
	cancelChannel := workflow.GetSignalChannel(ctx, "cancel")

	workflow.Go(ctx, func(ctx workflow.Context) {
		cancelChannel.Receive(ctx, nil)
		cancelRequested = true
		logger.Info("Cancel signal received")
	})

	// Activity options with retry
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Step 1: Validate order
	status.Status = "validating"
	status.UpdatedAt = workflow.Now(ctx)

	var valid bool
	err = workflow.ExecuteActivity(ctx, ValidateOrderActivity, orderID).Get(ctx, &valid)
	if err != nil {
		status.Status = "validation_failed"
		return status, err
	}

	if !valid {
		status.Status = "invalid"
		return status, errors.New("order validation failed")
	}

	if cancelRequested {
		status.Status = "cancelled"
		return status, nil
	}

	// Step 2: Calculate total
	status.Status = "calculating"
	status.UpdatedAt = workflow.Now(ctx)

	var total float64
	err = workflow.ExecuteActivity(ctx, CalculateTotalActivity, orderID).Get(ctx, &total)
	if err != nil {
		status.Status = "calculation_failed"
		return status, err
	}

	status.Total = total

	if cancelRequested {
		status.Status = "cancelled"
		return status, nil
	}

	// Step 3: Process payment
	status.Status = "processing_payment"
	status.UpdatedAt = workflow.Now(ctx)

	var paymentID string
	err = workflow.ExecuteActivity(ctx, ProcessPaymentActivity, orderID, total).Get(ctx, &paymentID)
	if err != nil {
		status.Status = "payment_failed"
		return status, err
	}

	if cancelRequested {
		// Refund if cancelled after payment
		workflow.ExecuteActivity(ctx, RefundPaymentActivity, paymentID).Get(ctx, nil)
		status.Status = "cancelled"
		return status, nil
	}

	// Step 4: Fulfill order
	status.Status = "fulfilling"
	status.UpdatedAt = workflow.Now(ctx)

	err = workflow.ExecuteActivity(ctx, FulfillOrderActivity, orderID).Get(ctx, nil)
	if err != nil {
		status.Status = "fulfillment_failed"
		return status, err
	}

	status.Status = "completed"
	status.UpdatedAt = workflow.Now(ctx)

	logger.Info("ProcessOrderWorkflow completed", "orderID", orderID, "total", total)
	return status, nil
}

// Activities

func ValidateOrderActivity(ctx context.Context, orderID string) (bool, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Validating order", "orderID", orderID)

	// Simulate validation
	time.Sleep(500 * time.Millisecond)
	return true, nil
}

func CalculateTotalActivity(ctx context.Context, orderID string) (float64, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Calculating total", "orderID", orderID)

	// Simulate calculation
	time.Sleep(300 * time.Millisecond)
	return 99.99, nil
}

func ProcessPaymentActivity(ctx context.Context, orderID string, amount float64) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Processing payment", "orderID", orderID, "amount", amount)

	// Simulate payment processing
	time.Sleep(1 * time.Second)
	paymentID := fmt.Sprintf("pay_%s_%d", orderID, time.Now().Unix())
	return paymentID, nil
}

func RefundPaymentActivity(ctx context.Context, paymentID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Refunding payment", "paymentID", paymentID)

	// Simulate refund
	time.Sleep(500 * time.Millisecond)
	return nil
}

func FulfillOrderActivity(ctx context.Context, orderID string) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Fulfilling order", "orderID", orderID)

	// Simulate fulfillment
	time.Sleep(800 * time.Millisecond)
	return nil
}

func main() {
	ctx := context.Background()

	// Create client
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	taskQueue := "order-processing-queue"

	// Start worker
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(ProcessOrderWorkflow).
		RegisterActivity(ValidateOrderActivity).
		RegisterActivity(CalculateTotalActivity).
		RegisterActivity(ProcessPaymentActivity).
		RegisterActivity(RefundPaymentActivity).
		RegisterActivity(FulfillOrderActivity).
		WithMaxConcurrentWorkflows(10).
		WithMaxConcurrentActivities(50).
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

	// Execute workflow
	orderID := fmt.Sprintf("order_%d", time.Now().Unix())
	workflowID := fmt.Sprintf("process-order-%s", orderID)

	workflowOpts := temporalworkflow.NewBuilder(workflowID, taskQueue).
		WithDefaultTimeouts().
		WithPatientRetry().
		WithSearchAttribute("OrderID", orderID).
		Build()

	workflowRun, err := c.ExecuteWorkflow(ctx, workflowOpts, ProcessOrderWorkflow, orderID)
	if err != nil {
		log.Fatalf("Failed to execute workflow: %v", err)
	}

	log.Printf("Started workflow - ID: %s, RunID: %s", workflowRun.GetID(), workflowRun.GetRunID())

	// Demonstrate querying the workflow
	time.Sleep(2 * time.Second)
	queryResult, err := c.QueryWorkflow(ctx, workflowID, "", "status")
	if err != nil {
		log.Printf("Failed to query workflow: %v", err)
	} else {
		log.Printf("Current status: %+v", queryResult)
	}

	// Wait for completion
	var result OrderStatus
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Fatalf("Workflow failed: %v", err)
	}

	log.Printf("Workflow completed successfully!")
	log.Printf("Final status: %+v", result)

	w.Stop()
}
