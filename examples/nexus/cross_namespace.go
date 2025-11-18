package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/workflow"

	temporalclient "github.com/templatedop/temporal/client"
	temporalnexus "github.com/templatedop/temporal/nexus"
	temporalworker "github.com/templatedop/temporal/worker"
)

// Example: Cross-namespace communication using Nexus
//
// This demonstrates:
// 1. Service A (Payment Service) in namespace "payments"
// 2. Service B (Order Service) in namespace "orders"
// 3. Order Service calls Payment Service via Nexus

// ============================================================================
// Payment Service (runs in "payments" namespace)
// ============================================================================

type ProcessPaymentInput struct {
	OrderID string  `json:"orderId"`
	Amount  float64 `json:"amount"`
	Currency string `json:"currency"`
}

type ProcessPaymentOutput struct {
	TransactionID string `json:"transactionId"`
	Status        string `json:"status"`
	ProcessedAt   string `json:"processedAt"`
}

// ProcessPayment is a synchronous Nexus operation
func ProcessPayment(ctx context.Context, input ProcessPaymentInput) (ProcessPaymentOutput, error) {
	// Simulate payment processing
	log.Printf("Processing payment for order %s: %.2f %s", input.OrderID, input.Amount, input.Currency)

	// In a real implementation, this would:
	// - Validate payment details
	// - Call payment gateway
	// - Store transaction record

	return ProcessPaymentOutput{
		TransactionID: fmt.Sprintf("TXN-%s-%d", input.OrderID, time.Now().Unix()),
		Status:        "completed",
		ProcessedAt:   time.Now().Format(time.RFC3339),
	}, nil
}

// CreatePaymentService creates the payment Nexus service
func CreatePaymentService() *nexus.Service {
	service := nexus.NewService("payment-service")

	// Register the ProcessPayment operation
	_ = nexus.NewSyncOperation(
		"process-payment",
		func(ctx context.Context, input ProcessPaymentInput, opts nexus.StartOperationOptions) (ProcessPaymentOutput, error) {
			return ProcessPayment(ctx, input)
		},
	)

	// In a real implementation, you would register the operation with the service
	// service.Register(processPaymentOp)

	return service
}

// ============================================================================
// Order Service (runs in "orders" namespace)
// ============================================================================

type OrderWorkflowInput struct {
	OrderID      string  `json:"orderId"`
	CustomerID   string  `json:"customerId"`
	TotalAmount  float64 `json:"totalAmount"`
	Currency     string  `json:"currency"`
}

type OrderWorkflowOutput struct {
	OrderID       string `json:"orderId"`
	Status        string `json:"status"`
	TransactionID string `json:"transactionId"`
}

// OrderWorkflow orchestrates an order including payment via Nexus
func OrderWorkflow(ctx workflow.Context, input OrderWorkflowInput) (OrderWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting order workflow", "orderID", input.OrderID)

	// Step 1: Validate order (local activity)
	err := validateOrder(ctx, input)
	if err != nil {
		return OrderWorkflowOutput{}, err
	}

	// Step 2: Call Payment Service via Nexus (cross-namespace)
	// The Payment Service runs in the "payments" namespace
	// We call it using the Nexus endpoint "payment-endpoint"

	paymentInput := ProcessPaymentInput{
		OrderID:  input.OrderID,
		Amount:   input.TotalAmount,
		Currency: input.Currency,
	}

	paymentResult, err := temporalnexus.CallServiceOperation[ProcessPaymentInput, ProcessPaymentOutput](
		ctx,
		"payment-endpoint",      // Nexus endpoint name
		"payment-service",       // Service name
		"process-payment",       // Operation name
		paymentInput,
		temporalnexus.WithOperationSummary(fmt.Sprintf("Payment for order %s", input.OrderID)),
	)

	if err != nil {
		logger.Error("Payment failed", "error", err)
		return OrderWorkflowOutput{
			OrderID: input.OrderID,
			Status:  "payment_failed",
		}, err
	}

	logger.Info("Payment completed", "transactionID", paymentResult.TransactionID)

	// Step 3: Fulfill order (local activity)
	err = fulfillOrder(ctx, input.OrderID)
	if err != nil {
		return OrderWorkflowOutput{}, err
	}

	return OrderWorkflowOutput{
		OrderID:       input.OrderID,
		Status:        "completed",
		TransactionID: paymentResult.TransactionID,
	}, nil
}

func validateOrder(ctx workflow.Context, input OrderWorkflowInput) error {
	// Simplified validation
	if input.TotalAmount <= 0 {
		return fmt.Errorf("invalid amount: %.2f", input.TotalAmount)
	}
	return nil
}

func fulfillOrder(ctx workflow.Context, orderID string) error {
	// Simplified fulfillment
	workflow.GetLogger(ctx).Info("Fulfilling order", "orderID", orderID)
	return nil
}

// ============================================================================
// Main: Setting up both services
// ============================================================================

// Commented out to avoid multiple main functions in the examples package
// func main() {
// 	// This example shows how to set up both services
// 	// In practice, they would run in separate deployments
//
// 	// Setup Payment Service Worker (in "payments" namespace)
// 	setupPaymentService()
//
// 	// Setup Order Service Worker (in "orders" namespace)
// 	setupOrderService()
// }

func setupPaymentService() {
	ctx := context.Background()

	// Create client for "payments" namespace
	cfg := &temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "payments",
	}
	c, err := temporalclient.New(ctx, cfg)
	if err != nil {
		log.Fatal("Failed to create payment service client:", err)
	}
	defer c.Close()

	// Create Nexus service
	_ = CreatePaymentService()

	// Register service with worker
	_, err = temporalworker.NewBuilder(c, "payment-task-queue").
		WithLogging(true).
		Build()

	if err != nil {
		log.Fatal("Failed to create payment worker:", err)
	}

	// Register the Nexus service
	// Note: In a real implementation, you would register the service:
	// w.RegisterNexusService(service)

	log.Println("Payment Service started in namespace: payments")

	// In production, you would call w.Run(ctx) here
}

func setupOrderService() {
	ctx := context.Background()

	// Create client for "orders" namespace
	cfg := &temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "orders",
	}
	c, err := temporalclient.New(ctx, cfg)
	if err != nil {
		log.Fatal("Failed to create order service client:", err)
	}
	defer c.Close()

	// Create worker with OrderWorkflow
	_, err = temporalworker.NewBuilder(c, "order-task-queue").
		RegisterWorkflow(OrderWorkflow).
		WithLogging(true).
		Build()

	if err != nil {
		log.Fatal("Failed to create order worker:", err)
	}

	log.Println("Order Service started in namespace: orders")

	// In production, you would call w.Run(ctx) here

	// Example: Execute an order workflow
	executeOrderWorkflow(ctx, c)
}

func executeOrderWorkflow(ctx context.Context, c *temporalclient.Client) {
	input := OrderWorkflowInput{
		OrderID:     "ORD-12345",
		CustomerID:  "CUST-789",
		TotalAmount: 99.99,
		Currency:    "USD",
	}

	workflowRun, err := c.ExecuteWorkflow(
		ctx,
		&temporalclient.WorkflowOptions{
			ID:        "order-workflow-" + input.OrderID,
			TaskQueue: "order-task-queue",
		},
		OrderWorkflow,
		input,
	)

	if err != nil {
		log.Fatal("Failed to start order workflow:", err)
	}

	var result OrderWorkflowOutput
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Fatal("Order workflow failed:", err)
	}

	log.Printf("Order completed: %+v", result)
}
