package testenv

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryController is the typed controlled-inventory surface.
// It reports through the production [domain.InventoryReporter] and never
// inserts inventory rows by direct database mutation.
type InventoryController struct {
	reporter     domain.InventoryReporter
	resourceType domain.ResourceType
}

// NewInventoryController constructs a controller that writes through
// reporter for the given inventory resource type.
func NewInventoryController(reporter domain.InventoryReporter, resourceType domain.ResourceType) *InventoryController {
	return &InventoryController{reporter: reporter, resourceType: resourceType}
}

// ReplaceLabels replaces inventory labels for the named resource via
// the production inventory write path.
func (c *InventoryController) ReplaceLabels(ctx context.Context, name domain.ResourceName, labels map[string]string) error {
	if c == nil || c.reporter == nil {
		return fmt.Errorf("testenv: inventory controller not configured")
	}
	if name == "" {
		return fmt.Errorf("%w: inventory resource name is required", domain.ErrInvalidArgument)
	}
	copied := make(map[string]string, len(labels))
	maps.Copy(copied, labels)
	return c.reporter.ApplyDeltaBatch(ctx, domain.InventoryDeltaBatch{
		Reports: []domain.InventoryDeltaReport{{
			ResourceType:  c.resourceType,
			Name:          name,
			ReplaceLabels: copied,
			ObservedAt:    time.Now(),
		}},
	})
}

// ResourceType returns the inventory resource type this controller was
// constructed with.
func (c *InventoryController) ResourceType() domain.ResourceType {
	return c.resourceType
}
