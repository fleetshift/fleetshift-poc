package cli

import (
	"context"
	"fmt"
	"log/slog"

	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// startKubernetesIndexController runs the controller and returns a channel
// that closes after the controller loop returns.
func startKubernetesIndexController(ctx context.Context, run func(context.Context)) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx)
	}()
	return done
}

// stopKubernetesIndexController cancels the controller and waits for its loop
// to return after its bounded hosted-indexer shutdown attempt.
func stopKubernetesIndexController(cancel context.CancelFunc, done <-chan struct{}) {
	cancel()
	<-done
}

// newKubernetesInProcessIndexing wires the Kubernetes in-process index
// controller, inventory reporter, and target indexed-inventory cleaner
// for server composition. The returned hooks implement
// [domain.TargetOutputHooks]; the controller is started separately by
// the caller once the process is ready to serve.
func newKubernetesInProcessIndexing(
	ctx context.Context,
	store domain.Store,
	vault domain.Vault,
	logger *slog.Logger,
) (domain.TargetOutputHooks, *kubernetesaddon.InProcessIndexController) {
	inventoryReportSvc := application.NewInventoryReportService(store)
	targetInventoryCleanupSvc := application.NewTargetInventoryCleanupService(store)
	reporter := kubernetesaddon.NewDirectInventoryReporter(
		newDirectInventoryReportBackend(inventoryReportSvc, targetInventoryCleanupSvc),
	)
	indexHost := kubernetesaddon.NewKubernetesInProcessIndexHost(ctx, vault, reporter, logger)
	controller := kubernetesaddon.NewInProcessIndexController(
		storeTargetLister{store: store},
		indexHost,
		kubernetesaddon.DefaultInProcessIndexPolicy{},
		logger,
	)
	hooks := application.NewTargetOutputHookService(
		application.WithTargetRuntimeNotifier(controller),
		application.WithTargetIndexedInventoryCleaner(
			kubernetesaddon.TargetType,
			kubernetesaddon.NewKubernetesTargetIndexedInventoryCleaner(targetInventoryCleanupSvc),
		),
	)
	return hooks, controller
}

// storeTargetLister adapts FleetShift's target store onto the Kubernetes
// in-process controller's TargetLister port at the server composition boundary.
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

// directInventoryReportBackend adapts application services onto the
// Kubernetes addon's InventoryReportBackend at the server composition boundary.
type directInventoryReportBackend struct {
	reports  *application.InventoryReportService
	subtrees *application.TargetInventoryCleanupService
}

// newDirectInventoryReportBackend adapts InventoryReportService and
// TargetInventoryCleanupService onto [kubernetesaddon.InventoryReportBackend].
func newDirectInventoryReportBackend(
	reports *application.InventoryReportService,
	subtrees *application.TargetInventoryCleanupService,
) *directInventoryReportBackend {
	return &directInventoryReportBackend{reports: reports, subtrees: subtrees}
}

// Compile-time check that the addon controller satisfies the
// application notifier port. Kept here (composition) because the
// implementer and the interface live in different packages; the two
// local adapters omit this pattern since New* already type-checks them.
var _ application.TargetRuntimeNotifier = (*kubernetesaddon.InProcessIndexController)(nil)

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

// DeleteBatch implements [kubernetesaddon.InventoryReportBackend].
func (b *directInventoryReportBackend) DeleteBatch(ctx context.Context, resources []domain.InventoryResourceRef) error {
	in := application.InventoryDeleteBatchInput{
		Resources: make([]application.InventoryDeleteInput, len(resources)),
	}
	for i, ref := range resources {
		in.Resources[i] = application.InventoryDeleteInput{
			ResourceType: ref.ResourceType,
			Name:         ref.Name,
		}
	}
	if err := b.reports.DeleteBatch(ctx, in); err != nil {
		return fmt.Errorf("kubernetes inventory report adapter delete batch: %w", err)
	}
	return nil
}

// ReplaceCollection implements [kubernetesaddon.InventoryReportBackend].
func (b *directInventoryReportBackend) ReplaceCollection(ctx context.Context, resourceType domain.ResourceType, collection domain.CollectionName, reports []kubernetesaddon.InventoryObjectReport) error {
	in := application.InventoryCollectionReplacementInput{
		ResourceType: resourceType,
		Collection:   collection,
		Reports:      make([]application.InventoryReplacementInput, len(reports)),
	}
	for i, report := range reports {
		name := report.Name
		in.Reports[i] = application.InventoryReplacementInput{
			ResourceType: resourceType,
			Name:         &name,
			Labels:       report.Labels,
			Observation:  report.Observation,
			Conditions:   report.Conditions,
			ObservedAt:   report.ObservedAt,
		}
	}
	if err := b.reports.ReplaceCollection(ctx, in); err != nil {
		return fmt.Errorf("kubernetes inventory report adapter replace collection: %w", err)
	}
	return nil
}

// DeleteCollection implements [kubernetesaddon.InventoryReportBackend].
func (b *directInventoryReportBackend) DeleteCollection(ctx context.Context, resourceType domain.ResourceType, collection domain.CollectionName) error {
	if err := b.reports.DeleteCollection(ctx, application.InventoryCollectionDeleteInput{
		ResourceType: resourceType,
		Collection:   collection,
	}); err != nil {
		return fmt.Errorf("kubernetes inventory report adapter delete collection: %w", err)
	}
	return nil
}

// DeleteSubtree implements [kubernetesaddon.InventoryReportBackend].
func (b *directInventoryReportBackend) DeleteSubtree(ctx context.Context, ref domain.InventorySubtreeRef) error {
	if err := b.subtrees.DeleteOwnedInventorySubtree(ctx, kubernetesaddon.AddonID, ref); err != nil {
		return fmt.Errorf("kubernetes inventory report adapter delete subtree: %w", err)
	}
	return nil
}
