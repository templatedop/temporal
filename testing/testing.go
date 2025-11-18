package testing

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// WorkflowTest provides a convenient wrapper around Temporal's workflow test suite
type WorkflowTest struct {
	suite *testsuite.WorkflowTestSuite
	env   *testsuite.TestWorkflowEnvironment
}

// NewWorkflowTest creates a new workflow test helper
func NewWorkflowTest(t *testing.T) *WorkflowTest {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	return &WorkflowTest{
		suite: suite,
		env:   env,
	}
}

// RegisterWorkflow registers a workflow for testing
func (wt *WorkflowTest) RegisterWorkflow(workflowFunc interface{}) {
	wt.env.RegisterWorkflow(workflowFunc)
}

// RegisterActivity registers an activity for testing
func (wt *WorkflowTest) RegisterActivity(activityFunc interface{}) {
	wt.env.RegisterActivity(activityFunc)
}

// OnActivity mocks an activity to return a specific value
func (wt *WorkflowTest) OnActivity(activityFunc interface{}, args ...interface{}) *testsuite.MockCallWrapper {
	return wt.env.OnActivity(activityFunc, args...)
}

// ExecuteWorkflow executes a workflow in the test environment
func (wt *WorkflowTest) ExecuteWorkflow(workflowFunc interface{}, args ...interface{}) {
	wt.env.ExecuteWorkflow(workflowFunc, args...)
}

// IsWorkflowCompleted checks if the workflow is completed
func (wt *WorkflowTest) IsWorkflowCompleted() bool {
	return wt.env.IsWorkflowCompleted()
}

// GetWorkflowError returns any workflow error
func (wt *WorkflowTest) GetWorkflowError() error {
	return wt.env.GetWorkflowError()
}

// GetWorkflowResult gets the workflow result
func (wt *WorkflowTest) GetWorkflowResult(valuePtr interface{}) error {
	return wt.env.GetWorkflowResult(valuePtr)
}

// AssertExpectations asserts that all mock expectations were met
func (wt *WorkflowTest) AssertExpectations(t *testing.T) bool {
	return wt.env.AssertExpectations(t)
}

// SetStartTime sets the workflow start time
func (wt *WorkflowTest) SetStartTime(startTime time.Time) {
	wt.env.SetStartTime(startTime)
}

// RegisterDelayedCallback registers a callback to be called after a delay
func (wt *WorkflowTest) RegisterDelayedCallback(callback func(), delay time.Duration) {
	wt.env.RegisterDelayedCallback(callback, delay)
}

// QueryWorkflow queries the workflow
func (wt *WorkflowTest) QueryWorkflow(queryType string, args ...interface{}) (interface{}, error) {
	val, err := wt.env.QueryWorkflow(queryType, args...)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// SignalWorkflow sends a signal to the workflow
func (wt *WorkflowTest) SignalWorkflow(signalName string, arg interface{}) {
	wt.env.SignalWorkflow(signalName, arg)
}

// Close cleans up the test environment
func (wt *WorkflowTest) Close() {
	// Environment cleanup is handled automatically
}

// ActivityTest provides a convenient wrapper for testing activities
type ActivityTest struct {
	suite *testsuite.WorkflowTestSuite
	env   *testsuite.TestActivityEnvironment
}

// NewActivityTest creates a new activity test helper
func NewActivityTest(t *testing.T) *ActivityTest {
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestActivityEnvironment()

	return &ActivityTest{
		suite: suite,
		env:   env,
	}
}

// RegisterActivity registers an activity for testing
func (at *ActivityTest) RegisterActivity(activityFunc interface{}) {
	at.env.RegisterActivity(activityFunc)
}

// ExecuteActivity executes an activity in the test environment
// Returns an encoded value that needs to be decoded
func (at *ActivityTest) ExecuteActivity(activityFunc interface{}, args ...interface{}) (interface{}, error) {
	return at.env.ExecuteActivity(activityFunc, args...)
}

// ExecuteActivityAndDecode executes an activity and decodes the result into valuePtr
func (at *ActivityTest) ExecuteActivityAndDecode(valuePtr interface{}, activityFunc interface{}, args ...interface{}) error {
	// Register the activity before executing it
	at.env.RegisterActivity(activityFunc)

	val, err := at.env.ExecuteActivity(activityFunc, args...)
	if err != nil {
		return err
	}

	// Use reflection or type assertion to get the value
	if getter, ok := val.(interface{ Get(interface{}) error }); ok {
		return getter.Get(valuePtr)
	}
	return nil
}

// SetHeartbeatDetails sets heartbeat details
func (at *ActivityTest) SetHeartbeatDetails(details interface{}) {
	at.env.SetHeartbeatDetails(details)
}

// Close cleans up the test environment
func (at *ActivityTest) Close() {
	// Environment cleanup is handled automatically
}

// Helper functions for common test patterns

// RunWorkflowTest is a convenience function that sets up and runs a workflow test
func RunWorkflowTest(t *testing.T, workflowFunc interface{}, args ...interface{}) (*WorkflowTest, interface{}) {
	wt := NewWorkflowTest(t)
	wt.RegisterWorkflow(workflowFunc)
	wt.ExecuteWorkflow(workflowFunc, args...)

	if !wt.IsWorkflowCompleted() {
		t.Fatal("Workflow not completed")
	}

	if err := wt.GetWorkflowError(); err != nil {
		t.Fatalf("Workflow failed: %v", err)
	}

	var result interface{}
	if err := wt.GetWorkflowResult(&result); err != nil {
		t.Fatalf("Failed to get workflow result: %v", err)
	}

	return wt, result
}

// RunActivityTest is a convenience function that sets up and runs an activity test
// Returns an encoded value that can be decoded using Get()
func RunActivityTest(t *testing.T, activityFunc interface{}, args ...interface{}) (interface{}, error) {
	at := NewActivityTest(t)
	// Register the activity before executing it
	at.env.RegisterActivity(activityFunc)
	return at.ExecuteActivity(activityFunc, args...)
}

// WorkflowWithTimeout is a helper that creates a context with timeout for workflow testing
func WorkflowWithTimeout(ctx workflow.Context, timeout time.Duration) (workflow.Context, workflow.CancelFunc) {
	return workflow.WithCancel(ctx)
}

// MockActivitySuccess configures an activity mock to succeed with a given result
func MockActivitySuccess(wt *WorkflowTest, activityFunc interface{}, result interface{}, args ...interface{}) {
	wt.OnActivity(activityFunc, args...).Return(result, nil)
}

// MockActivityError configures an activity mock to fail with a given error
func MockActivityError(wt *WorkflowTest, activityFunc interface{}, err error, args ...interface{}) {
	wt.OnActivity(activityFunc, args...).Return(nil, err)
}
