package application

import (
	"context"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryReporterAdapter adapts [InventoryReportService] to
// [domain.InventoryReporter] so addons can report inventory without
// importing the application package's input DTOs.
type InventoryReporterAdapter struct {
	svc *InventoryReportService
}

// NewInventoryReporterAdapter returns a [domain.InventoryReporter]
// backed by svc.
func NewInventoryReporterAdapter(svc *InventoryReportService) *InventoryReporterAdapter {
	return &InventoryReporterAdapter{svc: svc}
}

// ApplyDeltaBatch maps domain reports onto
// [InventoryReportService.ApplyDeltaBatch].
func (a *InventoryReporterAdapter) ApplyDeltaBatch(ctx context.Context, batch domain.InventoryDeltaBatch) error {
	if a == nil || a.svc == nil {
		return nil
	}
	reports := make([]InventoryDeltaInput, len(batch.Reports))
	for i, r := range batch.Reports {
		name := r.Name
		reports[i] = InventoryDeltaInput{
			ResourceType:      r.ResourceType,
			Name:              &name,
			UpsertAliases:     r.UpsertAliases,
			DeleteAliases:     r.DeleteAliases,
			ReplaceAliases:    r.ReplaceAliases,
			ReplaceLabels:     r.ReplaceLabels,
			DeleteLabels:      r.DeleteLabels,
			Observation:       r.Observation,
			ReplaceConditions: r.ReplaceConditions,
			UpsertConditions:  r.UpsertConditions,
			DeleteConditions:  r.DeleteConditions,
			ObservedAt:        r.ObservedAt,
		}
	}
	return a.svc.ApplyDeltaBatch(ctx, InventoryDeltaBatchInput{Reports: reports})
}
