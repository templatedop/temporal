package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/templatedop/temporal/dsl"
)

// Activity implementations for insurance claim workflow

// ClaimData represents claim information
type ClaimData struct {
	ClaimID     string  `json:"claimId"`
	ClaimType   string  `json:"claimType"`
	ClaimAmount float64 `json:"claimAmount"`
	CustomerID  string  `json:"customerId"`
}

// ValidationResult represents validation output
type ValidationResult struct {
	Valid  bool   `json:"valid"`
	Reason string `json:"reason,omitempty"`
}

// ValidateClaim validates a claim
func ValidateClaim(ctx context.Context, input map[string]interface{}) (*ValidationResult, error) {
	log.Printf("Validating claim: %v", input["claimId"])
	// Simulate validation logic
	time.Sleep(100 * time.Millisecond)
	return &ValidationResult{Valid: true}, nil
}

// AutoFraudCheck checks for fraud indicators
func AutoFraudCheck(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("Running fraud check for claim: %v", input["claimId"])
	time.Sleep(200 * time.Millisecond)
	return map[string]interface{}{
		"fraudScore":  0.15,
		"lowFraudRisk": true,
	}, nil
}

// AutoApprove auto-approves low-risk claims
func AutoApprove(ctx context.Context, input map[string]interface{}) (string, error) {
	log.Printf("Auto-approving claim: %v", input["claimId"])
	return "approved", nil
}

// CheckCoverage checks policy coverage
func CheckCoverage(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("Checking coverage for policy: %v", input["policyId"])
	time.Sleep(150 * time.Millisecond)
	return map[string]interface{}{
		"covered":    true,
		"percentage": 80,
	}, nil
}

// VerifyProvider verifies medical provider
func VerifyProvider(ctx context.Context, input map[string]interface{}) (bool, error) {
	log.Printf("Verifying provider: %v", input["providerId"])
	time.Sleep(100 * time.Millisecond)
	return true, nil
}

// CalculateBenefits calculates benefit amount
func CalculateBenefits(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("Calculating benefits")
	return map[string]interface{}{
		"benefitsAmount": 800.0,
		"deductible":     200.0,
	}, nil
}

// ScheduleInspection schedules home inspection
func ScheduleInspection(ctx context.Context, input map[string]interface{}) (string, error) {
	log.Printf("Scheduling inspection for address: %v", input["address"])
	return "inspection-scheduled-123", nil
}

// ReviewInspection reviews inspection results
func ReviewInspection(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("Reviewing inspection for claim: %v", input["claimId"])
	return map[string]interface{}{
		"approved": true,
		"damages":  "minor",
	}, nil
}

// ProcessPayment processes claim payment
func ProcessPayment(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	log.Printf("Processing payment for claim: %v", input["claimId"])
	time.Sleep(300 * time.Millisecond)
	return map[string]interface{}{
		"paymentId":     "PAY-12345",
		"status":        "completed",
		"transactionId": "TXN-67890",
	}, nil
}

// SendNotification sends notification to customer
func SendNotification(ctx context.Context, input map[string]interface{}) error {
	log.Printf("Sending notification to customer: %v", input["customerId"])
	log.Printf("Status: %v", input["status"])
	return nil
}

// LogError logs error details
func LogError(ctx context.Context, input map[string]interface{}) error {
	log.Printf("ERROR logged - Claim: %v, Error: %v", input["claimId"], input["error"])
	return nil
}

// NotifySupervisor notifies supervisor of error
func NotifySupervisor(ctx context.Context, input map[string]interface{}) error {
	log.Printf("Notifying supervisor - Claim: %v", input["claimId"])
	return nil
}

// QueueForManualReview queues claim for manual review
func QueueForManualReview(ctx context.Context, input map[string]interface{}) error {
	log.Printf("Queueing for manual review - Claim: %v, Reason: %v",
		input["claimId"], input["reason"])
	return nil
}

// DSLWorkflow is the workflow that executes DSL definitions
func DSLWorkflow(ctx workflow.Context, defPath string, input map[string]interface{}) (map[string]interface{}, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting DSL workflow", "definitionPath", defPath)

	// Create activity registry
	registry := dsl.NewActivityRegistry()
	registry.Register("ValidateClaim", ValidateClaim)
	registry.Register("AutoFraudCheck", AutoFraudCheck)
	registry.Register("AutoApprove", AutoApprove)
	registry.Register("CheckCoverage", CheckCoverage)
	registry.Register("VerifyProvider", VerifyProvider)
	registry.Register("CalculateBenefits", CalculateBenefits)
	registry.Register("ScheduleInspection", ScheduleInspection)
	registry.Register("ReviewInspection", ReviewInspection)
	registry.Register("ProcessPayment", ProcessPayment)
	registry.Register("SendNotification", SendNotification)
	registry.Register("LogError", LogError)
	registry.Register("NotifySupervisor", NotifySupervisor)
	registry.Register("QueueForManualReview", QueueForManualReview)

	// Load workflow definition
	loader := dsl.NewLoader()
	def, err := loader.LoadFromYAML(defPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load workflow definition: %w", err)
	}

	// Execute defined workflow
	return dsl.ExecuteDefinedWorkflow(ctx, def, registry, input)
}

func main() {
	// Create Temporal client
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	// Create worker
	w := worker.New(c, "dsl-task-queue", worker.Options{})

	// Register workflow
	w.RegisterWorkflow(DSLWorkflow)

	// Register all activities
	w.RegisterActivity(ValidateClaim)
	w.RegisterActivity(AutoFraudCheck)
	w.RegisterActivity(AutoApprove)
	w.RegisterActivity(CheckCoverage)
	w.RegisterActivity(VerifyProvider)
	w.RegisterActivity(CalculateBenefits)
	w.RegisterActivity(ScheduleInspection)
	w.RegisterActivity(ReviewInspection)
	w.RegisterActivity(ProcessPayment)
	w.RegisterActivity(SendNotification)
	w.RegisterActivity(LogError)
	w.RegisterActivity(NotifySupervisor)
	w.RegisterActivity(QueueForManualReview)

	// Start worker in background
	go func() {
		err = w.Run(worker.InterruptCh())
		if err != nil {
			log.Fatalln("Unable to start worker", err)
		}
	}()

	// Give worker time to start
	time.Sleep(2 * time.Second)

	// Execute workflow with different claim types
	ctx := context.Background()

	// Example 1: Auto claim (low amount - auto approve)
	fmt.Println("\n=== Example 1: Auto Claim (Low Amount) ===")
	autoClaimInput := map[string]interface{}{
		"claimId":     "AUTO-001",
		"claimType":   "auto",
		"claimAmount": "low",
		"customerId":  "CUST-123",
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        "dsl-workflow-auto-low",
		TaskQueue: "dsl-task-queue",
	}

	we, err := c.ExecuteWorkflow(ctx, workflowOptions, DSLWorkflow,
		"insurance_claim_routing.yaml", autoClaimInput)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	fmt.Printf("Started workflow - WorkflowID: %s, RunID: %s\n", we.GetID(), we.GetRunID())

	// Wait for result
	var result map[string]interface{}
	err = we.Get(ctx, &result)
	if err != nil {
		log.Fatalln("Unable to get workflow result", err)
	}

	fmt.Printf("Workflow completed successfully\n")
	fmt.Printf("Result: %+v\n", result)

	// Example 2: Health claim
	fmt.Println("\n=== Example 2: Health Claim ===")
	healthClaimInput := map[string]interface{}{
		"claimId":     "HEALTH-001",
		"claimType":   "health",
		"policyId":    "POL-456",
		"providerId":  "PROV-789",
		"claimAmount": 1000.0,
		"procedures":  []string{"consultation", "x-ray"},
		"customerId":  "CUST-456",
	}

	workflowOptions.ID = "dsl-workflow-health"
	we2, err := c.ExecuteWorkflow(ctx, workflowOptions, DSLWorkflow,
		"insurance_claim_routing.yaml", healthClaimInput)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	fmt.Printf("Started workflow - WorkflowID: %s, RunID: %s\n", we2.GetID(), we2.GetRunID())

	// Wait for result
	err = we2.Get(ctx, &result)
	if err != nil {
		log.Fatalln("Unable to get workflow result", err)
	}

	fmt.Printf("Workflow completed successfully\n")
	fmt.Printf("Result: %+v\n", result)

	// Example 3: Using programmatic builder
	fmt.Println("\n=== Example 3: Programmatic Workflow Definition ===")

	// Build workflow definition programmatically
	builder := dsl.NewBuilder("simple-claim-check")
	builder.WithDescription("Simple claim validation workflow").
		WithVersion("1.0").
		WithTimeout("5m")

	builder.AddActivityStep("validate", "ValidateClaim").
		WithInput(map[string]interface{}{
			"claimId":   "$claimId",
			"claimData": "$claimData",
		}).
		WithTimeout("30s").
		WithOutputVar("validationResult").
		Done()

	builder.AddActivityStep("process", "ProcessPayment").
		WithInput(map[string]interface{}{
			"claimId": "$claimId",
			"amount":  "$amount",
		}).
		WithTimeout("1m").
		WithOutputVar("paymentResult").
		Done()

	programmaticDef := builder.Build()

	// Execute programmatic workflow
	registry := dsl.NewActivityRegistry()
	registry.Register("ValidateClaim", ValidateClaim)
	registry.Register("ProcessPayment", ProcessPayment)

	simpleInput := map[string]interface{}{
		"claimId": "SIMPLE-001",
		"amount":  500.0,
	}

	workflowOptions.ID = "dsl-workflow-programmatic"
	we3, err := c.ExecuteWorkflow(ctx, workflowOptions, func(ctx workflow.Context) (map[string]interface{}, error) {
		return dsl.ExecuteDefinedWorkflow(ctx, programmaticDef, registry, simpleInput)
	})
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	fmt.Printf("Started workflow - WorkflowID: %s, RunID: %s\n", we3.GetID(), we3.GetRunID())

	err = we3.Get(ctx, &result)
	if err != nil {
		log.Fatalln("Unable to get workflow result", err)
	}

	fmt.Printf("Workflow completed successfully\n")
	fmt.Printf("Result: %+v\n", result)

	fmt.Println("\n=== All DSL workflow examples completed successfully! ===")
}
