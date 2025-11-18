package versioning

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// SimpleVersionManager provides basic version management
type SimpleVersionManager struct {
	ctx workflow.Context
}

// NewSimple creates a simple version manager
func NewSimple(ctx workflow.Context) *SimpleVersionManager {
	return &SimpleVersionManager{ctx: ctx}
}

// ExecuteVersioned executes code based on version
func (s *SimpleVersionManager) ExecuteVersioned(
	changeID string,
	minVersion, maxVersion workflow.Version,
	handlers map[workflow.Version]func() error,
) error {
	version := workflow.GetVersion(s.ctx, changeID, minVersion, maxVersion)

	handler, exists := handlers[version]
	if !exists {
		return fmt.Errorf("no handler for version %d", version)
	}

	return handler()
}

// Helper to check if workflow is using a specific version
func IsVersion(ctx workflow.Context, changeID string, expectedVersion workflow.Version) bool {
	version := workflow.GetVersion(ctx, changeID, workflow.DefaultVersion, expectedVersion)
	return version == expectedVersion
}

// Helper to check if workflow is at least a specific version
func IsVersionOrHigher(ctx workflow.Context, changeID string, minVersion workflow.Version) bool {
	version := workflow.GetVersion(ctx, changeID, workflow.DefaultVersion, minVersion)
	return version >= minVersion
}

// ExecuteVersionedCode is a helper function for simple versioned execution
func ExecuteVersionedCode(
	ctx workflow.Context,
	changeID string,
	oldCode func() error,
	newCode func() error,
) error {
	version := workflow.GetVersion(ctx, changeID, workflow.DefaultVersion, 1)

	if version == workflow.DefaultVersion {
		return oldCode()
	}
	return newCode()
}
