package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// kubernetesInProcessIndexing holds the indexing runtime for server composition.
type kubernetesInProcessIndexing struct {
	// Runtime is injected into Kind/GCP agents and used for StopAll / replay.
	Runtime kubernetesaddon.IndexingRuntime
}

// newKubernetesInProcessIndexing wires the Kubernetes indexing runtime
// and inventory reporter for server composition. Callers inject Runtime
// into Kind and GCP HCP agents, run ReplayPersistedIndexers after addon
// connect, and call StopAll on shutdown. inventoryReports is the shared
// InventoryReportService used elsewhere in composition.
func newKubernetesInProcessIndexing(
	ctx context.Context,
	vault domain.Vault,
	inventoryReports *application.InventoryReportService,
	logger *slog.Logger,
) *kubernetesInProcessIndexing {
	reporter := kubernetesaddon.NewDirectInventoryReporter(
		newDirectInventoryReportBackend(inventoryReports),
	)
	host := kubernetesaddon.NewKubernetesInProcessIndexHost(
		ctx,
		vault,
		reporter,
		kubernetesaddon.DefaultIndexerClients{},
		logger,
	)
	return &kubernetesInProcessIndexing{Runtime: host}
}

// directInventoryReportBackend adapts InventoryReportService onto the
// Kubernetes addon's InventoryReportBackend at the server composition
// boundary.
type directInventoryReportBackend struct {
	reports *application.InventoryReportService
}

// newDirectInventoryReportBackend adapts InventoryReportService for the
// Kubernetes addon's InventoryReportBackend port.
func newDirectInventoryReportBackend(
	reports *application.InventoryReportService,
) *directInventoryReportBackend {
	return &directInventoryReportBackend{reports: reports}
}

// ReplaceBatch implements [kubernetesaddon.InventoryReportBackend].
func (b *directInventoryReportBackend) ReplaceBatch(ctx context.Context, resourceType domain.ResourceType, reports []kubernetesaddon.InventoryObjectReport) error {
	in := application.InventoryReplacementBatchInput{
		Reports: make([]application.InventoryReplacementInput, len(reports)),
	}
	for i, report := range reports {
		name := report.Name
		in.Reports[i] = application.InventoryReplacementInput{
			ResourceType: resourceType,
			Name:         &name,
			IsDelete:     report.IsDelete,
			Labels:       report.Labels,
			Observation:  report.Observation,
			Conditions:   report.Conditions,
			ObservedAt:   report.ObservedAt,
		}
	}
	if err := b.reports.ReplaceBatch(ctx, in); err != nil {
		return fmt.Errorf("kubernetes inventory report adapter replace batch: %w", err)
	}
	return nil
}

// storeTargetLister adapts FleetShift's target store onto the Kubernetes
// startup replayer's TargetLister port at the server composition boundary.
type storeTargetLister struct {
	store domain.Store
}

// ListTargets implements [kubernetesaddon.TargetLister].
func (l storeTargetLister) ListTargets(ctx context.Context) ([]domain.TargetInfo, error) {
	tx, err := l.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("list targets: begin read-only tx: %w", err)
	}
	defer tx.Rollback()
	targets, err := tx.Targets().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}
	return targets, nil
}

// startKubernetesIndexStartupReplay runs one-shot persisted-target recovery
// and returns a channel that closes when the replay goroutine finishes.
func startKubernetesIndexStartupReplay(ctx context.Context, run func(context.Context)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx)
	}()
	return done
}
