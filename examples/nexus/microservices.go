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

// Example: Microservices Orchestration using Nexus
//
// This demonstrates a food delivery system with independent microservices:
// 1. Restaurant Service - manages order preparation
// 2. Delivery Service - manages driver assignment and delivery
// 3. Notification Service - sends customer notifications
// 4. Orchestrator Service - coordinates the entire order fulfillment

// ============================================================================
// Restaurant Service
// ============================================================================

type PrepareOrderInput struct {
	OrderID      string   `json:"orderId"`
	RestaurantID string   `json:"restaurantId"`
	Items        []string `json:"items"`
}

type PrepareOrderOutput struct {
	OrderID        string `json:"orderId"`
	EstimatedTime  int    `json:"estimatedTimeMinutes"`
	PreparationID  string `json:"preparationId"`
	Status         string `json:"status"`
}

// PrepareOrder operation in Restaurant Service
func PrepareOrder(ctx context.Context, input PrepareOrderInput) (PrepareOrderOutput, error) {
	log.Printf("Restaurant %s preparing order %s with %d items",
		input.RestaurantID, input.OrderID, len(input.Items))

	// Simulate preparation time calculation
	estimatedTime := len(input.Items) * 5 // 5 minutes per item

	return PrepareOrderOutput{
		OrderID:        input.OrderID,
		EstimatedTime:  estimatedTime,
		PreparationID:  fmt.Sprintf("PREP-%s", input.OrderID),
		Status:         "preparing",
	}, nil
}

// ============================================================================
// Delivery Service
// ============================================================================

type AssignDriverInput struct {
	OrderID         string  `json:"orderId"`
	RestaurantID    string  `json:"restaurantId"`
	DeliveryAddress string  `json:"deliveryAddress"`
	EstimatedTime   int     `json:"estimatedTimeMinutes"`
}

type AssignDriverOutput struct {
	DriverID      string  `json:"driverId"`
	DriverName    string  `json:"driverName"`
	EstimatedETA  int     `json:"estimatedEtaMinutes"`
	TrackingURL   string  `json:"trackingUrl"`
}

// AssignDriver operation in Delivery Service
func AssignDriver(ctx context.Context, input AssignDriverInput) (AssignDriverOutput, error) {
	log.Printf("Assigning driver for order %s to %s", input.OrderID, input.DeliveryAddress)

	// Simulate driver assignment
	driverID := fmt.Sprintf("DRV-%d", time.Now().Unix()%1000)

	return AssignDriverOutput{
		DriverID:     driverID,
		DriverName:   "John Doe",
		EstimatedETA: input.EstimatedTime + 15, // prep time + travel time
		TrackingURL:  fmt.Sprintf("https://track.delivery.com/%s", input.OrderID),
	}, nil
}

// ============================================================================
// Notification Service
// ============================================================================

type SendNotificationInput struct {
	CustomerID string `json:"customerId"`
	OrderID    string `json:"orderId"`
	Message    string `json:"message"`
	Channel    string `json:"channel"` // sms, email, push
}

type SendNotificationOutput struct {
	NotificationID string `json:"notificationId"`
	Status         string `json:"status"`
	SentAt         string `json:"sentAt"`
}

// SendNotification operation in Notification Service
func SendNotification(ctx context.Context, input SendNotificationInput) (SendNotificationOutput, error) {
	log.Printf("Sending %s notification to customer %s: %s",
		input.Channel, input.CustomerID, input.Message)

	return SendNotificationOutput{
		NotificationID: fmt.Sprintf("NOTIF-%s", input.OrderID),
		Status:         "sent",
		SentAt:         time.Now().Format(time.RFC3339),
	}, nil
}

// ============================================================================
// Orchestrator Service - Food Delivery Workflow
// ============================================================================

type FoodDeliveryInput struct {
	OrderID         string   `json:"orderId"`
	CustomerID      string   `json:"customerId"`
	RestaurantID    string   `json:"restaurantId"`
	Items           []string `json:"items"`
	DeliveryAddress string   `json:"deliveryAddress"`
}

type FoodDeliveryOutput struct {
	OrderID       string `json:"orderId"`
	Status        string `json:"status"`
	EstimatedETA  int    `json:"estimatedEtaMinutes"`
	DriverName    string `json:"driverName"`
	TrackingURL   string `json:"trackingUrl"`
}

// FoodDeliveryWorkflow orchestrates the entire food delivery process
// by calling multiple microservices via Nexus
func FoodDeliveryWorkflow(ctx workflow.Context, input FoodDeliveryInput) (FoodDeliveryOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting food delivery workflow", "orderID", input.OrderID)

	output := FoodDeliveryOutput{
		OrderID: input.OrderID,
		Status:  "processing",
	}

	// Step 1: Call Restaurant Service to prepare order
	logger.Info("Requesting order preparation from Restaurant Service")
	prepareResult, err := temporalnexus.CallServiceOperation[PrepareOrderInput, PrepareOrderOutput](
		ctx,
		"restaurant-endpoint",
		"restaurant-service",
		"prepare-order",
		PrepareOrderInput{
			OrderID:      input.OrderID,
			RestaurantID: input.RestaurantID,
			Items:        input.Items,
		},
		temporalnexus.WithOperationSummary(fmt.Sprintf("Prepare order %s", input.OrderID)),
	)

	if err != nil {
		logger.Error("Failed to prepare order", "error", err)
		return output, err
	}

	logger.Info("Order preparation started",
		"preparationID", prepareResult.PreparationID,
		"estimatedTime", prepareResult.EstimatedTime)

	// Step 2: Call Delivery Service to assign driver (runs in parallel with preparation)
	logger.Info("Assigning driver from Delivery Service")
	driverResult, err := temporalnexus.CallServiceOperation[AssignDriverInput, AssignDriverOutput](
		ctx,
		"delivery-endpoint",
		"delivery-service",
		"assign-driver",
		AssignDriverInput{
			OrderID:         input.OrderID,
			RestaurantID:    input.RestaurantID,
			DeliveryAddress: input.DeliveryAddress,
			EstimatedTime:   prepareResult.EstimatedTime,
		},
		temporalnexus.WithOperationSummary(fmt.Sprintf("Assign driver for order %s", input.OrderID)),
	)

	if err != nil {
		logger.Error("Failed to assign driver", "error", err)
		return output, err
	}

	logger.Info("Driver assigned",
		"driverID", driverResult.DriverID,
		"estimatedETA", driverResult.EstimatedETA)

	// Step 3: Send confirmation notification to customer
	logger.Info("Sending confirmation notification")
	_, err = temporalnexus.CallServiceOperation[SendNotificationInput, SendNotificationOutput](
		ctx,
		"notification-endpoint",
		"notification-service",
		"send-notification",
		SendNotificationInput{
			CustomerID: input.CustomerID,
			OrderID:    input.OrderID,
			Message: fmt.Sprintf("Your order is being prepared! Driver %s will deliver in %d minutes.",
				driverResult.DriverName, driverResult.EstimatedETA),
			Channel: "push",
		},
	)

	if err != nil {
		logger.Error("Failed to send notification", "error", err)
		// Non-critical error, continue
	}

	// Step 4: Wait for preparation to complete (simulated)
	workflow.Sleep(ctx, time.Duration(prepareResult.EstimatedTime)*time.Second)

	// Step 5: Send "out for delivery" notification
	logger.Info("Order ready, sending delivery notification")
	_, err = temporalnexus.CallServiceOperation[SendNotificationInput, SendNotificationOutput](
		ctx,
		"notification-endpoint",
		"notification-service",
		"send-notification",
		SendNotificationInput{
			CustomerID: input.CustomerID,
			OrderID:    input.OrderID,
			Message:    fmt.Sprintf("Your order is out for delivery! Track it at: %s", driverResult.TrackingURL),
			Channel:    "push",
		},
	)

	output.Status = "out_for_delivery"
	output.EstimatedETA = driverResult.EstimatedETA
	output.DriverName = driverResult.DriverName
	output.TrackingURL = driverResult.TrackingURL

	return output, nil
}

// ============================================================================
// Service Setup Functions
// ============================================================================

func createRestaurantService() *nexus.Service {
	service := nexus.NewService("restaurant-service")

	// Register operations - in a real implementation, you'd register them properly
	// This is a simplified example

	return service
}

func createDeliveryService() *nexus.Service {
	service := nexus.NewService("delivery-service")
	return service
}

func createNotificationService() *nexus.Service {
	service := nexus.NewService("notification-service")
	return service
}

// ============================================================================
// Main
// ============================================================================

func main() {
	ctx := context.Background()

	// In a real system, each service would run in its own process/container
	// Here we show the setup for demonstration

	// Setup Restaurant Service Worker
	go setupRestaurantWorker(ctx)

	// Setup Delivery Service Worker
	go setupDeliveryWorker(ctx)

	// Setup Notification Service Worker
	go setupNotificationWorker(ctx)

	// Setup Orchestrator Worker
	time.Sleep(2 * time.Second) // Give services time to start
	executeOrchestration(ctx)
}

func setupRestaurantWorker(ctx context.Context) {
	cfg := &temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	}
	c, _ := temporalclient.New(ctx, cfg)
	defer c.Close()

	service := createRestaurantService()

	w, _ := temporalworker.NewBuilder(c, "restaurant-tasks").
		RegisterNexusService(service).
		WithLogging(true).
		Build()

	log.Println("Restaurant Service worker started")
	w.Run(ctx)
}

func setupDeliveryWorker(ctx context.Context) {
	cfg := &temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	}
	c, _ := temporalclient.New(ctx, cfg)
	defer c.Close()

	service := createDeliveryService()

	w, _ := temporalworker.NewBuilder(c, "delivery-tasks").
		RegisterNexusService(service).
		WithLogging(true).
		Build()

	log.Println("Delivery Service worker started")
	w.Run(ctx)
}

func setupNotificationWorker(ctx context.Context) {
	cfg := &temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	}
	c, _ := temporalclient.New(ctx, cfg)
	defer c.Close()

	service := createNotificationService()

	w, _ := temporalworker.NewBuilder(c, "notification-tasks").
		RegisterNexusService(service).
		WithLogging(true).
		Build()

	log.Println("Notification Service worker started")
	w.Run(ctx)
}

func executeOrchestration(ctx context.Context) {
	cfg := &temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	}
	c, err := temporalclient.New(ctx, cfg)
	if err != nil {
		log.Fatal("Failed to create client:", err)
	}
	defer c.Close()

	// Start orchestrator worker
	w, _ := temporalworker.NewBuilder(c, "orchestrator-tasks").
		RegisterWorkflow(FoodDeliveryWorkflow).
		WithLogging(true).
		Build()

	go w.Run(ctx)

	// Execute workflow
	input := FoodDeliveryInput{
		OrderID:         "ORDER-12345",
		CustomerID:      "CUST-789",
		RestaurantID:    "REST-456",
		Items:           []string{"Burger", "Fries", "Coke"},
		DeliveryAddress: "123 Main St, Apt 4B",
	}

	workflowRun, err := c.ExecuteWorkflow(
		ctx,
		&temporalclient.WorkflowOptions{
			ID:        "food-delivery-" + input.OrderID,
			TaskQueue: "orchestrator-tasks",
		},
		FoodDeliveryWorkflow,
		input,
	)

	if err != nil {
		log.Fatal("Failed to start workflow:", err)
	}

	var result FoodDeliveryOutput
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Fatal("Workflow failed:", err)
	}

	log.Printf("Order completed: %+v", result)
}
