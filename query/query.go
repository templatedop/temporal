package query

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"

	temporalclient "github.com/templatedop/temporal/client"
)

// WorkflowFilter defines filters for workflow queries
type WorkflowFilter struct {
	WorkflowType string
	WorkflowID   string
	Status       string // Running, Completed, Failed, Canceled, Terminated, TimedOut
	StartTime    *time.Time
	EndTime      *time.Time
}

// PageToken represents pagination state
type PageToken struct {
	Token []byte
}

// WorkflowInfo contains information about a workflow execution
type WorkflowInfo struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	StartTime    time.Time
	CloseTime    *time.Time
	Status       string
	Memo         map[string]interface{}
}

// WorkflowQueryResult contains paginated workflow query results
type WorkflowQueryResult struct {
	Workflows    []WorkflowInfo
	NextPageToken *PageToken
	HasMore      bool
}

// QueryBuilder provides a fluent interface for building workflow queries
type QueryBuilder struct {
	client     *temporalclient.Client
	filter     *WorkflowFilter
	pageSize   int
	pageToken  *PageToken
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder(c *temporalclient.Client) *QueryBuilder {
	return &QueryBuilder{
		client:   c,
		filter:   &WorkflowFilter{},
		pageSize: 100,
	}
}

// WorkflowType filters by workflow type
func (qb *QueryBuilder) WorkflowType(workflowType string) *QueryBuilder {
	qb.filter.WorkflowType = workflowType
	return qb
}

// WorkflowID filters by workflow ID (exact match or prefix)
func (qb *QueryBuilder) WorkflowID(workflowID string) *QueryBuilder {
	qb.filter.WorkflowID = workflowID
	return qb
}

// Status filters by workflow status
func (qb *QueryBuilder) Status(status string) *QueryBuilder {
	qb.filter.Status = status
	return qb
}

// StartTimeAfter filters workflows that started after the given time
func (qb *QueryBuilder) StartTimeAfter(t time.Time) *QueryBuilder {
	qb.filter.StartTime = &t
	return qb
}

// CloseTimeBefore filters workflows that closed before the given time
func (qb *QueryBuilder) CloseTimeBefore(t time.Time) *QueryBuilder {
	qb.filter.EndTime = &t
	return qb
}

// PageSize sets the page size for pagination
func (qb *QueryBuilder) PageSize(size int) *QueryBuilder {
	qb.pageSize = size
	return qb
}

// PageToken sets the page token for pagination
func (qb *QueryBuilder) PageToken(token *PageToken) *QueryBuilder {
	qb.pageToken = token
	return qb
}

// Execute runs the query and returns results
func (qb *QueryBuilder) Execute(ctx context.Context) (*WorkflowQueryResult, error) {
	// Build the query string
	query := qb.buildQuery()

	// Execute the list workflow
	var token []byte
	if qb.pageToken != nil {
		token = qb.pageToken.Token
	}

	resp, err := qb.client.Underlying().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     qb.client.Namespace(),
		PageSize:      int32(qb.pageSize),
		NextPageToken: token,
		Query:         query,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	// Convert response to our format
	result := &WorkflowQueryResult{
		Workflows: make([]WorkflowInfo, 0, len(resp.Executions)),
		HasMore:   len(resp.NextPageToken) > 0,
	}

	if len(resp.NextPageToken) > 0 {
		result.NextPageToken = &PageToken{Token: resp.NextPageToken}
	}

	for _, exec := range resp.Executions {
		info := WorkflowInfo{
			WorkflowID:   exec.Execution.WorkflowId,
			RunID:        exec.Execution.RunId,
			WorkflowType: exec.Type.Name,
		}

		// Convert protobuf timestamp to time.Time
		if exec.StartTime != nil {
			startTime := exec.StartTime.AsTime()
			info.StartTime = startTime
		}

		if exec.CloseTime != nil {
			closeTime := exec.CloseTime.AsTime()
			info.CloseTime = &closeTime
		}

		if exec.Status != 0 {
			info.Status = exec.Status.String()
		}

		result.Workflows = append(result.Workflows, info)
	}

	return result, nil
}

// buildQuery constructs the query string from filters
func (qb *QueryBuilder) buildQuery() string {
	conditions := make([]string, 0)

	if qb.filter.WorkflowType != "" {
		conditions = append(conditions, fmt.Sprintf("WorkflowType = '%s'", qb.filter.WorkflowType))
	}

	if qb.filter.WorkflowID != "" {
		conditions = append(conditions, fmt.Sprintf("WorkflowId = '%s'", qb.filter.WorkflowID))
	}

	if qb.filter.Status != "" {
		conditions = append(conditions, fmt.Sprintf("ExecutionStatus = '%s'", qb.filter.Status))
	}

	if qb.filter.StartTime != nil {
		conditions = append(conditions, fmt.Sprintf("StartTime > '%s'", qb.filter.StartTime.Format(time.RFC3339)))
	}

	if qb.filter.EndTime != nil {
		conditions = append(conditions, fmt.Sprintf("CloseTime < '%s'", qb.filter.EndTime.Format(time.RFC3339)))
	}

	if len(conditions) == 0 {
		return ""
	}

	query := conditions[0]
	for i := 1; i < len(conditions); i++ {
		query += " AND " + conditions[i]
	}

	return query
}

// ExecuteAll retrieves all pages of results
func (qb *QueryBuilder) ExecuteAll(ctx context.Context) ([]WorkflowInfo, error) {
	allWorkflows := make([]WorkflowInfo, 0)
	currentToken := qb.pageToken

	for {
		qb.pageToken = currentToken
		result, err := qb.Execute(ctx)
		if err != nil {
			return nil, err
		}

		allWorkflows = append(allWorkflows, result.Workflows...)

		if !result.HasMore {
			break
		}

		currentToken = result.NextPageToken
	}

	return allWorkflows, nil
}

// Common query helpers

// ListRunningWorkflows lists all running workflows of a given type
func ListRunningWorkflows(ctx context.Context, c *temporalclient.Client, workflowType string) ([]WorkflowInfo, error) {
	return NewQueryBuilder(c).
		WorkflowType(workflowType).
		Status("Running").
		ExecuteAll(ctx)
}

// ListCompletedWorkflows lists completed workflows within a time range
func ListCompletedWorkflows(ctx context.Context, c *temporalclient.Client, workflowType string, since time.Time) ([]WorkflowInfo, error) {
	return NewQueryBuilder(c).
		WorkflowType(workflowType).
		Status("Completed").
		StartTimeAfter(since).
		ExecuteAll(ctx)
}

// ListFailedWorkflows lists failed workflows within a time range
func ListFailedWorkflows(ctx context.Context, c *temporalclient.Client, workflowType string, since time.Time) ([]WorkflowInfo, error) {
	return NewQueryBuilder(c).
		WorkflowType(workflowType).
		Status("Failed").
		StartTimeAfter(since).
		ExecuteAll(ctx)
}

// FindWorkflowByID finds a specific workflow by ID
func FindWorkflowByID(ctx context.Context, c *temporalclient.Client, workflowID string) (*WorkflowInfo, error) {
	result, err := NewQueryBuilder(c).
		WorkflowID(workflowID).
		PageSize(1).
		Execute(ctx)

	if err != nil {
		return nil, err
	}

	if len(result.Workflows) == 0 {
		return nil, fmt.Errorf("workflow not found: %s", workflowID)
	}

	return &result.Workflows[0], nil
}

// CountWorkflows counts workflows matching the criteria
func CountWorkflows(ctx context.Context, c *temporalclient.Client, workflowType, status string) (int, error) {
	workflows, err := NewQueryBuilder(c).
		WorkflowType(workflowType).
		Status(status).
		ExecuteAll(ctx)

	if err != nil {
		return 0, err
	}

	return len(workflows), nil
}
