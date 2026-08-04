package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryReporter is the reusable inventory report boundary for the
// Kubernetes indexing core. In-process indexing and a future
// external agent both emit these commands; only the transport behind
// the reporter changes. Live upserts and exact deletes flush through
// one mixed ReplaceBatch via [InventoryDeltaReport].
type InventoryReporter interface {
	// ApplyDelta writes complete-object upserts and exact deletes in
	// one mixed ReplaceBatch. Empty reports are a no-op.
	ApplyDelta(ctx context.Context, delta InventoryDeltaReport) error
}

// InventoryObjectReport is the complete latest inventory state for one
// Kubernetes object, or an exact-name whole-resource delete when
// IsDelete is true. Name must already be an [ObjectResourceName]; the
// reporter does not re-derive it. Observation follows
// InventoryReportService replacement semantics: a nil or JSON-null
// value leaves the latest observation untouched; any other value
// replaces it. When IsDelete is true, Labels/Observation/Conditions/
// ObservedAt must be empty.
type InventoryObjectReport struct {
	Name        domain.ResourceName
	IsDelete    bool
	Labels      map[string]string
	Observation *json.RawMessage
	Conditions  []domain.Condition
	ObservedAt  time.Time
}

// InventoryDeltaReport is one incremental flush of object upserts and
// deletes. Upserts are complete latest-state replacements (not
// field-level deltas). Deletes are exact-name whole-resource deletes
// carried as [InventoryObjectReport] with IsDelete true (same DTO as
// upserts; no separate resource-ref type). An empty report is a no-op:
// the reporter must not turn idle flushes into InventoryReportService
// heartbeats. DirectInventoryReporter maps both slices into one
// backend ReplaceBatch.
type InventoryDeltaReport struct {
	Upserts []InventoryObjectReport
	Deletes []InventoryObjectReport
}

// InventoryReportBackend is the application call surface the direct
// [DirectInventoryReporter] adapts onto. It is declared here, at the
// point of use, rather than imported from the application layer, so
// this addon package never depends on internal/application. A thin
// adapter in server composition adapts
// *application.InventoryReportService to this interface.
type InventoryReportBackend interface {
	// ReplaceBatch writes a mixed batch of complete latest-state
	// replacements and exact-name deletes (IsDelete) for the given
	// resource type.
	ReplaceBatch(ctx context.Context, resourceType domain.ResourceType, reports []InventoryObjectReport) error
}

// DirectInventoryReporter is the no-transport [InventoryReporter] used by
// server-side indexing. It always reports under [ObjectResourceType] and
// forwards to an [InventoryReportBackend].
type DirectInventoryReporter struct {
	backend InventoryReportBackend
}

// NewDirectInventoryReporter creates a reporter backed by backend.
func NewDirectInventoryReporter(backend InventoryReportBackend) *DirectInventoryReporter {
	return &DirectInventoryReporter{backend: backend}
}

// ApplyDelta writes upserts and deletes through one mixed
// ReplaceBatch. An empty delta returns nil without calling the backend
// -- idle empty flushes are not heartbeats. Upsert entries are forced
// to IsDelete false and delete entries to IsDelete true so a mis-tagged
// writer entry cannot flip sides.
func (r *DirectInventoryReporter) ApplyDelta(ctx context.Context, delta InventoryDeltaReport) error {
	if len(delta.Upserts) == 0 && len(delta.Deletes) == 0 {
		return nil
	}
	reports := make([]InventoryObjectReport, 0, len(delta.Upserts)+len(delta.Deletes))
	for _, up := range delta.Upserts {
		up.IsDelete = false
		reports = append(reports, up)
	}
	for _, del := range delta.Deletes {
		reports = append(reports, InventoryObjectReport{
			Name:     del.Name,
			IsDelete: true,
		})
	}
	if err := r.backend.ReplaceBatch(ctx, ObjectResourceType, reports); err != nil {
		return fmt.Errorf("direct inventory reporter apply delta: %w", err)
	}
	return nil
}
