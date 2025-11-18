package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/client"
	"github.com/templatedop/temporal/patterns"
	temporalworkflow "github.com/templatedop/temporal/workflow"
	"github.com/templatedop/temporal/worker"
)

// BookingData holds trip booking information
type BookingData struct {
	TripID          string
	FlightBookingID string
	HotelBookingID  string
	CarBookingID    string
}

// Flight booking activities
func BookFlight(ctx context.Context, data *BookingData) (*BookingData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Booking flight", "tripID", data.TripID)

	time.Sleep(200 * time.Millisecond)
	data.FlightBookingID = "FLIGHT-" + data.TripID
	return data, nil
}

func CancelFlight(ctx context.Context, data interface{}) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Cancelling flight booking")

	time.Sleep(100 * time.Millisecond)
	return nil
}

// Hotel booking activities
func BookHotel(ctx context.Context, data *BookingData) (*BookingData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Booking hotel", "tripID", data.TripID)

	time.Sleep(200 * time.Millisecond)
	data.HotelBookingID = "HOTEL-" + data.TripID
	return data, nil
}

func CancelHotel(ctx context.Context, data interface{}) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Cancelling hotel booking")

	time.Sleep(100 * time.Millisecond)
	return nil
}

// Car rental activities
func BookCar(ctx context.Context, data *BookingData) (*BookingData, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Booking car rental", "tripID", data.TripID)

	// Simulate a failure for demonstration
	// return nil, fmt.Errorf("car rental service unavailable")

	time.Sleep(200 * time.Millisecond)
	data.CarBookingID = "CAR-" + data.TripID
	return data, nil
}

func CancelCar(ctx context.Context, data interface{}) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Cancelling car rental booking")

	time.Sleep(100 * time.Millisecond)
	return nil
}

// TripBookingSagaWorkflow demonstrates the Saga pattern for distributed transactions
func TripBookingSagaWorkflow(ctx workflow.Context, tripID string) (*patterns.SagaResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting trip booking saga", "tripID", tripID)

	// Build the saga
	saga := patterns.NewSagaBuilder().
		StepWithTimeout("BookFlight", BookFlight, CancelFlight, 30*time.Second, 10*time.Second).
		StepWithTimeout("BookHotel", BookHotel, CancelHotel, 30*time.Second, 10*time.Second).
		StepWithTimeout("BookCar", BookCar, CancelCar, 30*time.Second, 10*time.Second).
		Build()

	// Execute the saga
	bookingData := &BookingData{
		TripID: tripID,
	}

	result, err := saga.Execute(ctx, bookingData)
	if err != nil {
		logger.Error("Saga failed", "error", err)
		return result, err
	}

	logger.Info("Saga completed successfully", "completedSteps", result.CompletedSteps)
	return result, nil
}

// DemoSagaFailureWorkflow demonstrates saga with intentional failure
func DemoSagaFailureWorkflow(ctx workflow.Context, tripID string) (*patterns.SagaResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting trip booking saga (with failure)", "tripID", tripID)

	// Create a failing car booking activity
	FailingBookCar := func(ctx context.Context, data *BookingData) (*BookingData, error) {
		logger := activity.GetLogger(ctx)
		logger.Info("Attempting to book car rental (will fail)", "tripID", data.TripID)
		time.Sleep(100 * time.Millisecond)
		return nil, fmt.Errorf("car rental service unavailable")
	}

	// Build the saga with failing step
	saga := patterns.NewSagaBuilder().
		StepWithTimeout("BookFlight", BookFlight, CancelFlight, 30*time.Second, 10*time.Second).
		StepWithTimeout("BookHotel", BookHotel, CancelHotel, 30*time.Second, 10*time.Second).
		StepWithTimeout("BookCar", FailingBookCar, CancelCar, 30*time.Second, 10*time.Second).
		Build()

	bookingData := &BookingData{
		TripID: tripID,
	}

	result, err := saga.Execute(ctx, bookingData)

	// Even though it failed, the compensations should have run
	logger.Info("Saga result",
		"success", result.Success,
		"completedSteps", result.CompletedSteps,
		"failedStep", result.FailedStep)

	return result, err
}

func main() {
	ctx := context.Background()

	// Create client
	c, err := client.NewWithDefaults(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	taskQueue := "saga-queue"

	// Start worker
	w, err := worker.NewBuilder(c, taskQueue).
		RegisterWorkflow(TripBookingSagaWorkflow).
		RegisterWorkflow(DemoSagaFailureWorkflow).
		RegisterActivity(BookFlight).
		RegisterActivity(CancelFlight).
		RegisterActivity(BookHotel).
		RegisterActivity(CancelHotel).
		RegisterActivity(BookCar).
		RegisterActivity(CancelCar).
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

	// Example 1: Successful saga
	log.Println("\n=== Running successful saga ===")
	opts1 := temporalworkflow.NewBuilder("saga-success", taskQueue).
		WithShortRunningDefaults().
		Build()

	run1, err := c.ExecuteWorkflow(ctx, opts1, TripBookingSagaWorkflow, "TRIP-001")
	if err != nil {
		log.Fatalf("Failed to execute workflow: %v", err)
	}

	var result1 patterns.SagaResult
	err = run1.Get(ctx, &result1)
	if err != nil {
		log.Printf("Saga failed (expected for demo): %v", err)
	} else {
		log.Printf("Saga completed: Success=%v, CompletedSteps=%d", result1.Success, result1.CompletedSteps)
	}

	// Example 2: Saga with failure (demonstrates compensation)
	log.Println("\n=== Running saga with failure (demonstrates compensation) ===")
	opts2 := temporalworkflow.NewBuilder("saga-failure", taskQueue).
		WithShortRunningDefaults().
		Build()

	run2, err := c.ExecuteWorkflow(ctx, opts2, DemoSagaFailureWorkflow, "TRIP-002")
	if err != nil {
		log.Fatalf("Failed to execute workflow: %v", err)
	}

	var result2 patterns.SagaResult
	err = run2.Get(ctx, &result2)
	if err != nil {
		log.Printf("Saga failed (expected): %v", err)
		log.Printf("Compensation completed for %d steps", result2.CompletedSteps)
	}

	w.Stop()
}
