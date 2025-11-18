package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	"github.com/templatedop/temporal/statemachine"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// OrderInput represents the input to the order workflow
type OrderInput struct {
	OrderID      string
	CustomerName string
	Amount       float64
}

// Order processing states
const (
	StateValidate   = "validate"
	StateReserve    = "reserve"
	StateCharge     = "charge"
	StateFulfill    = "fulfill"
	StateComplete   = "complete"
	StateCancelled  = "cancelled"
)

// Activities
func ValidateOrder(ctx context.Context, input *OrderInput) (bool, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Validating order", "orderID", input.OrderID)

	// Simulate validation
	time.Sleep(100 * time.Millisecond)

	// Validate order (simple check)
	if input.Amount > 0 && input.CustomerName != "" {
		return true, nil
	}

	return false, fmt.Errorf("invalid order")
}

func ReserveInventory(ctx context.Context, input *OrderInput) (*OrderInput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Reserving inventory", "orderID", input.OrderID)

	time.Sleep(100 * time.Millisecond)
	return input, nil
}

func ChargePayment(ctx context.Context, input *OrderInput) (*OrderInput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Charging payment", "orderID", input.OrderID, "amount", input.Amount)

	time.Sleep(100 * time.Millisecond)
	return input, nil
}

func FulfillOrder(ctx context.Context, input *OrderInput) (*OrderInput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Fulfilling order", "orderID", input.OrderID)

	time.Sleep(100 * time.Millisecond)
	return input, nil
}

// OrderStateMachineWorkflow demonstrates state machine pattern
func OrderStateMachineWorkflow(ctx workflow.Context, input *OrderInput) (interface{}, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting order state machine workflow", "orderID", input.OrderID)

	// Build the state machine
	sm := statemachine.NewBuilder(StateValidate).
		// Validate state
		State(StateValidate, func(ctx workflow.Context, data interface{}) (string, interface{}, error) {
			ao := workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
			}
			ctx = workflow.WithActivityOptions(ctx, ao)

			var valid bool
			err := workflow.ExecuteActivity(ctx, ValidateOrder, input).Get(ctx, &valid)
			if err != nil || !valid {
				return StateCancelled, data, fmt.Errorf("validation failed")
			}

			return StateReserve, data, nil
		}).
		// Reserve inventory state
		StateWithActivity(StateReserve, ReserveInventory, StateCharge, 10*time.Second).
		// Charge payment state
		StateWithActivity(StateCharge, ChargePayment, StateFulfill, 10*time.Second).
		// Fulfill order state
		StateWithActivity(StateFulfill, FulfillOrder, StateComplete, 10*time.Second).
		// Final states
		FinalState(StateComplete).
		FinalState(StateCancelled).
		// Transitions
		Transition(StateValidate, StateReserve).
		Transition(StateValidate, StateCancelled).
		Transition(StateReserve, StateCharge).
		Transition(StateCharge, StateFulfill).
		Transition(StateFulfill, StateComplete).
		Build()

	// Execute the state machine
	result, err := sm.Execute(ctx, input)
	if err != nil {
		logger.Error("State machine execution failed", "error", err)
		return nil, err
	}

	logger.Info("State machine completed successfully")
	return result, nil
}

func main() {
	ctx := context.Background()

	// Create client
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	taskQueue := "statemachine-queue"

	// Start worker
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(OrderStateMachineWorkflow).
		RegisterActivity(ValidateOrder).
		RegisterActivity(ReserveInventory).
		RegisterActivity(ChargePayment).
		RegisterActivity(FulfillOrder).
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
	orderInput := &OrderInput{
		OrderID:      "order-12345",
		CustomerName: "John Doe",
		Amount:       99.99,
	}

	opts := temporalworkflow.NewBuilder("statemachine-workflow", taskQueue).
		WithShortRunningDefaults().
		Build()

	workflowRun, err := c.ExecuteWorkflow(ctx, opts, OrderStateMachineWorkflow, orderInput)
	if err != nil {
		log.Fatalf("Failed to execute workflow: %v", err)
	}

	log.Printf("Started workflow - ID: %s, RunID: %s", workflowRun.GetID(), workflowRun.GetRunID())

	// Wait for result
	var result interface{}
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Fatalf("Workflow failed: %v", err)
	}

	log.Printf("Workflow completed successfully: %+v", result)

	w.Stop()
}
