package errors

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
)

// ErrorType categorizes errors for better handling
type ErrorType string

const (
	// ErrorTypeRetryable indicates the error is transient and should be retried
	ErrorTypeRetryable ErrorType = "retryable"

	// ErrorTypeFatal indicates the error is permanent and should not be retried
	ErrorTypeFatal ErrorType = "fatal"

	// ErrorTypeUserError indicates the error is due to user input/config
	ErrorTypeUserError ErrorType = "user_error"

	// ErrorTypeTimeout indicates the operation timed out
	ErrorTypeTimeout ErrorType = "timeout"

	// ErrorTypeValidation indicates a validation failure
	ErrorTypeValidation ErrorType = "validation"

	// ErrorTypeNotFound indicates a resource was not found
	ErrorTypeNotFound ErrorType = "not_found"

	// ErrorTypeConflict indicates a conflict (e.g., duplicate, race condition)
	ErrorTypeConflict ErrorType = "conflict"

	// ErrorTypePermission indicates a permission/authorization error
	ErrorTypePermission ErrorType = "permission"
)

// WorkflowError represents a structured error in workflows
type WorkflowError struct {
	Type    ErrorType              `json:"type"`
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
	Cause   error                  `json:"-"` // Original error (not serialized)
}

// Error implements the error interface
func (e *WorkflowError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s:%s] %s: %v", e.Type, e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s:%s] %s", e.Type, e.Code, e.Message)
}

// Unwrap returns the underlying cause
func (e *WorkflowError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns whether the error should be retried
func (e *WorkflowError) IsRetryable() bool {
	return e.Type == ErrorTypeRetryable || e.Type == ErrorTypeTimeout
}

// NewError creates a new workflow error
func NewError(errType ErrorType, code, message string) *WorkflowError {
	return &WorkflowError{
		Type:    errType,
		Code:    code,
		Message: message,
		Details: make(map[string]interface{}),
	}
}

// NewRetryableError creates a retryable error
func NewRetryableError(code, message string) *WorkflowError {
	return NewError(ErrorTypeRetryable, code, message)
}

// NewFatalError creates a fatal error (non-retryable)
func NewFatalError(code, message string) *WorkflowError {
	return NewError(ErrorTypeFatal, code, message)
}

// NewUserError creates a user error (bad input/config)
func NewUserError(code, message string) *WorkflowError {
	return NewError(ErrorTypeUserError, code, message)
}

// NewValidationError creates a validation error
func NewValidationError(message string) *WorkflowError {
	return NewError(ErrorTypeValidation, "VALIDATION_FAILED", message)
}

// NewNotFoundError creates a not found error
func NewNotFoundError(resource, id string) *WorkflowError {
	return NewError(ErrorTypeNotFound, "NOT_FOUND", fmt.Sprintf("%s not found: %s", resource, id))
}

// NewTimeoutError creates a timeout error
func NewTimeoutError(operation string) *WorkflowError {
	return NewError(ErrorTypeTimeout, "TIMEOUT", fmt.Sprintf("%s timed out", operation))
}

// NewConflictError creates a conflict error
func NewConflictError(message string) *WorkflowError {
	return NewError(ErrorTypeConflict, "CONFLICT", message)
}

// NewPermissionError creates a permission error
func NewPermissionError(action, resource string) *WorkflowError {
	return NewError(ErrorTypePermission, "PERMISSION_DENIED", fmt.Sprintf("permission denied: %s on %s", action, resource))
}

// WithDetails adds details to the error
func (e *WorkflowError) WithDetails(key string, value interface{}) *WorkflowError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// WithCause wraps an underlying error
func (e *WorkflowError) WithCause(cause error) *WorkflowError {
	e.Cause = cause
	return e
}

// AsApplicationError converts WorkflowError to Temporal ApplicationError
func (e *WorkflowError) AsApplicationError() error {
	details := []interface{}{e.Type, e.Code, e.Details}

	err := temporal.NewApplicationError(e.Message, string(e.Type), details...)

	// Set non-retryable based on error type
	if !e.IsRetryable() {
		err = temporal.NewNonRetryableApplicationError(e.Message, string(e.Type), e.Cause, details...)
	}

	return err
}

// FromApplicationError extracts WorkflowError from Temporal ApplicationError
func FromApplicationError(err error) (*WorkflowError, bool) {
	var appErr *temporal.ApplicationError
	ok := false
	if appErr, ok = err.(*temporal.ApplicationError); !ok {
		return nil, false
	}

	wfErr := &WorkflowError{
		Type:    ErrorType(appErr.Type()),
		Message: appErr.Error(),
		Details: make(map[string]interface{}),
	}

	// Extract type from error
	wfErr.Code = appErr.Type()

	return wfErr, true
}

// Error handling helpers

// IsRetryable checks if an error is retryable
func IsRetryable(err error) bool {
	if wfErr, ok := FromApplicationError(err); ok {
		return wfErr.IsRetryable()
	}
	// Default: treat as retryable unless explicitly non-retryable
	return !temporal.IsApplicationError(err) || !IsNonRetryable(err)
}

// IsNonRetryable checks if an error is explicitly non-retryable
func IsNonRetryable(err error) bool {
	if appErr, ok := err.(*temporal.ApplicationError); ok {
		return appErr.NonRetryable()
	}
	return false
}

// IsFatal checks if an error is fatal
func IsFatal(err error) bool {
	if wfErr, ok := FromApplicationError(err); ok {
		return wfErr.Type == ErrorTypeFatal
	}
	return false
}

// IsUserError checks if an error is a user error
func IsUserError(err error) bool {
	if wfErr, ok := FromApplicationError(err); ok {
		return wfErr.Type == ErrorTypeUserError || wfErr.Type == ErrorTypeValidation
	}
	return false
}

// IsTimeout checks if an error is a timeout
func IsTimeout(err error) bool {
	if wfErr, ok := FromApplicationError(err); ok {
		return wfErr.Type == ErrorTypeTimeout
	}
	return temporal.IsTimeoutError(err)
}

// Common error constructors for specific domains

// InsuranceErrors provides insurance-specific error constructors
type InsuranceErrors struct{}

func (InsuranceErrors) ClaimNotFound(claimID string) *WorkflowError {
	return NewNotFoundError("Claim", claimID)
}

func (InsuranceErrors) InvalidClaim(reason string) *WorkflowError {
	return NewValidationError(fmt.Sprintf("Invalid claim: %s", reason))
}

func (InsuranceErrors) FraudDetected(claimID string, reason string) *WorkflowError {
	return NewFatalError("FRAUD_DETECTED", fmt.Sprintf("Fraud detected for claim %s: %s", claimID, reason)).
		WithDetails("claimID", claimID).
		WithDetails("reason", reason)
}

func (InsuranceErrors) DocumentMissing(documentType string) *WorkflowError {
	return NewValidationError(fmt.Sprintf("Required document missing: %s", documentType)).
		WithDetails("documentType", documentType)
}

func (InsuranceErrors) PaymentFailed(reason string) *WorkflowError {
	return NewRetryableError("PAYMENT_FAILED", reason)
}

// HRErrors provides HR-specific error constructors
type HRErrors struct{}

func (HRErrors) EmployeeNotFound(employeeID string) *WorkflowError {
	return NewNotFoundError("Employee", employeeID)
}

func (HRErrors) InvalidReview(reason string) *WorkflowError {
	return NewValidationError(fmt.Sprintf("Invalid performance review: %s", reason))
}

func (HRErrors) ApprovalTimeout(approverID string) *WorkflowError {
	return NewTimeoutError(fmt.Sprintf("Approval from %s", approverID)).
		WithDetails("approverID", approverID)
}

func (HRErrors) BackgroundCheckFailed(reason string) *WorkflowError {
	return NewFatalError("BACKGROUND_CHECK_FAILED", reason)
}

func (HRErrors) UnauthorizedAccess(action, resource string) *WorkflowError {
	return NewPermissionError(action, resource)
}

// Global error instances for convenience
var (
	Insurance = InsuranceErrors{}
	HR        = HRErrors{}
)

// ErrorHandler provides error handling strategies
type ErrorHandler struct {
	onRetryable func(error) error
	onFatal     func(error) error
	onUserError func(error) error
	onTimeout   func(error) error
}

// NewErrorHandler creates a new error handler
func NewErrorHandler() *ErrorHandler {
	return &ErrorHandler{}
}

// OnRetryable sets handler for retryable errors
func (eh *ErrorHandler) OnRetryable(handler func(error) error) *ErrorHandler {
	eh.onRetryable = handler
	return eh
}

// OnFatal sets handler for fatal errors
func (eh *ErrorHandler) OnFatal(handler func(error) error) *ErrorHandler {
	eh.onFatal = handler
	return eh
}

// OnUserError sets handler for user errors
func (eh *ErrorHandler) OnUserError(handler func(error) error) *ErrorHandler {
	eh.onUserError = handler
	return eh
}

// OnTimeout sets handler for timeout errors
func (eh *ErrorHandler) OnTimeout(handler func(error) error) *ErrorHandler {
	eh.onTimeout = handler
	return eh
}

// Handle processes an error with the appropriate handler
func (eh *ErrorHandler) Handle(err error) error {
	if err == nil {
		return nil
	}

	wfErr, ok := FromApplicationError(err)
	if !ok {
		// Not a WorkflowError, treat as retryable by default
		if eh.onRetryable != nil {
			return eh.onRetryable(err)
		}
		return err
	}

	switch wfErr.Type {
	case ErrorTypeFatal:
		if eh.onFatal != nil {
			return eh.onFatal(err)
		}
	case ErrorTypeUserError, ErrorTypeValidation:
		if eh.onUserError != nil {
			return eh.onUserError(err)
		}
	case ErrorTypeTimeout:
		if eh.onTimeout != nil {
			return eh.onTimeout(err)
		}
	case ErrorTypeRetryable:
		if eh.onRetryable != nil {
			return eh.onRetryable(err)
		}
	}

	return err
}
