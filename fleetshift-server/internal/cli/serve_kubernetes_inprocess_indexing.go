package cli

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
	// Host is the concrete in-process host (same instance as Runtime).
	Host *kubernetesaddon.KubernetesInProcessIndexHost
}

// newKubernetesInProcessIndexing wires the Kubernetes indexing runtime
// and inventory reporter for server composition. Callers inject Runtime
// into Kind/GCP HCP agents, run one-shot startup replay, and StopAll on
// shutdown.
func newKubernetesInProcessIndexing(
	ctx context.Context,
	store domain.Store,
	vault domain.Vault,
	logger *slog.Logger,
) *kubernetesInProcessIndexing {
	inventoryReportSvc := application.NewInventoryReportService(store)
	reporter := kubernetesaddon.NewDirectInventoryReporter(
		newDirectInventoryReportBackend(inventoryReportSvc),
	)
	host := kubernetesaddon.NewKubernetesInProcessIndexHost(
		ctx,
		vault,
		reporter,
		kubernetesaddon.DefaultIndexerClients{},
		logger,
	)
	return &kubernetesInProcessIndexing{
		Runtime: host,
		Host:    host,
	}
}

// directInventoryReportBackend adapts InventoryReportService onto the
// Kubernetes addon's InventoryReportBackend at the server composition
// boundary.
type directInventoryReportBackend struct {
	reports *application.InventoryReportService
}

// newDirectInventoryReportBackend adapts InventoryReportService onto
// [kubernetesaddon.InventoryReportBackend].
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
