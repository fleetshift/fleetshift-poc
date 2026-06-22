package gcphcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const defaultReconcileTimeout = 55 * time.Minute

var reconcileTimeout = defaultReconcileTimeout

// AgentDeps holds dependencies for creating an Agent.
type AgentDeps struct {
	Gateway  GatewayConfig
	Infra    *InfraRunner
	Observer AgentObserver
	Reporter domain.DeliveryReporter
}

// Agent implements the domain.DeliveryAgent interface for the GCP HCP addon.
// It coordinates cluster provisioning and deletion through the Reconciler.
type Agent struct {
	reconciler    *Reconciler
	observer      AgentObserver
	reporter      domain.DeliveryReporter
	trustMu       sync.RWMutex
	trustMap      map[domain.IssuerURL]domain.TrustBundleEntry
	clusterMu     sync.Mutex
	clusterGen    map[string]domain.Generation
	deliveryLocks sync.Map
}

// NewAgent creates a new Agent with the given dependencies.
// If Observer is nil, a no-op observer is used.
// If Infra is nil, a new InfraRunner is created.
func NewAgent(deps AgentDeps) *Agent {
	observer := deps.Observer
	if observer == nil {
		observer = noopObserver{}
	}

	infra := deps.Infra
	if infra == nil {
		infra = NewInfraRunner()
	}

	reconciler := NewReconciler(deps.Gateway, infra)

	return &Agent{
		reconciler: reconciler,
		observer:   observer,
		reporter:   deps.Reporter,
		trustMap:   make(map[domain.IssuerURL]domain.TrustBundleEntry),
		clusterGen: make(map[string]domain.Generation),
	}
}

// TrustBundles returns the current trust bundles stored in the agent.
func (a *Agent) TrustBundles() []domain.TrustBundleEntry {
	a.trustMu.RLock()
	defer a.trustMu.RUnlock()

	bundles := make([]domain.TrustBundleEntry, 0, len(a.trustMap))
	for _, entry := range a.trustMap {
		bundles = append(bundles, entry)
	}
	sort.Slice(bundles, func(i, j int) bool {
		return bundles[i].IssuerURL < bundles[j].IssuerURL
	})
	return bundles
}

// acceptGeneration atomically checks and updates the per-cluster
// generation high-water mark. Returns false if gen is strictly older
// than the highest generation already accepted for that cluster.
// Same-generation retries are allowed because the orchestration
// retries with an unchanged generation after transient failures;
// the per-cluster mutex prevents concurrent duplicates.
func (a *Agent) acceptGeneration(clusterName string, gen domain.Generation) bool {
	a.clusterMu.Lock()
	defer a.clusterMu.Unlock()
	if current, ok := a.clusterGen[clusterName]; ok && gen < current {
		return false
	}
	a.clusterGen[clusterName] = gen
	return true
}

func (a *Agent) clusterLock(name string) *sync.Mutex {
	val, _ := a.deliveryLocks.LoadOrStore(name, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// RecoverActiveDeliveries queries the reporter for non-terminal
// deliveries targeting the given targets and re-launches the addon
// goroutine for each one. Call this at startup to resume work that
// was interrupted by a server restart.
func (a *Agent) RecoverActiveDeliveries(ctx context.Context, targetIDs []domain.TargetID) error {
	active, err := a.reporter.ListActiveDeliveries(ctx, targetIDs)
	if err != nil {
		return fmt.Errorf("list active deliveries: %w", err)
	}
	if len(active) == 0 {
		a.observer.Info("no active deliveries to recover")
		return nil
	}

	for _, ad := range active {
		manifests := ad.Delivery.Manifests()
		var clusterManifest *domain.Manifest
		for i, m := range manifests {
			if m.ManifestType == ClusterManifestType {
				clusterManifest = &manifests[i]
				break
			}
		}
		if clusterManifest == nil {
			continue
		}

		spec, err := ParseClusterSpec(clusterManifest.Raw)
		if err != nil {
			a.observer.Error("recovery: failed to parse cluster spec", "delivery", ad.Delivery.ID(), "error", err)
			continue
		}
		spec.Name = string(clusterManifest.ManifestID)

		if !a.acceptGeneration(spec.Name, ad.Delivery.Generation()) {
			continue
		}

		if ad.Auth.Token == "" {
			a.observer.Error("recovery: auth token empty, skipping", "delivery", ad.Delivery.ID())
			continue
		}

		a.observer.Info("recovering active delivery",
			"delivery", ad.Delivery.ID(),
			"cluster", spec.Name,
			"state", ad.Delivery.State(),
			"generation", ad.Delivery.Generation(),
		)

		targetCfg := TargetConfigFromProperties(ad.Target.Properties())
		progress := newDeliveryProgress(a.reporter, ad.Delivery.ID(), ad.Delivery.Generation())

		lock := a.clusterLock(spec.Name)
		lock.Lock()
		if ad.Delivery.Operation() == domain.DeliveryOperationRemove {
			go func() {
				defer lock.Unlock()
				a.deleteAsync(ctx, spec, targetCfg, string(ad.Auth.Token), progress)
			}()
		} else {
			go func() {
				defer lock.Unlock()
				a.deliverAsync(ctx, spec, targetCfg, string(ad.Auth.Token), progress)
			}()
		}
	}
	return nil
}

// Deliver implements domain.DeliveryAgent.Deliver.
// It processes manifests in two categories:
// 1. Trust bundle manifests - stored immediately
// 2. Cluster manifests - provisioned asynchronously
//
// All delivery outcomes are reported through the DeliveryReporter.
func (a *Agent) Deliver(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	generation domain.Generation,
) error {
	progress := newDeliveryProgress(a.reporter, deliveryID, generation)

	// Separate trust bundles from cluster manifests
	var trustBundles []domain.Manifest
	var clusterManifests []domain.Manifest

	for _, m := range manifests {
		if m.ManifestType == domain.TrustBundleManifestType {
			trustBundles = append(trustBundles, m)
		} else {
			clusterManifests = append(clusterManifests, m)
		}
	}

	// Process trust bundles
	for _, tb := range trustBundles {
		entry, err := a.storeTrustBundle(tb)
		if err != nil {
			a.failDelivery(ctx, progress, domain.DeliveryStateFailed, fmt.Sprintf("failed to unmarshal trust bundle: %v", err))
			return nil
		}
		a.observer.Info("stored trust bundle", "issuer", entry.IssuerURL)
	}

	// If only trust bundles (no cluster manifests), signal done
	if len(clusterManifests) == 0 {
		asyncCtx := context.WithoutCancel(ctx)
		go func() {
			if err := progress.Complete(asyncCtx, domain.DeliveryResult{State: domain.DeliveryStateDelivered}); err != nil {
				a.observer.Error("failed to report trust bundle delivery", "error", err)
			}
		}()
		return nil
	}

	// Expect exactly 1 cluster manifest
	if len(clusterManifests) != 1 {
		a.failDelivery(ctx, progress, domain.DeliveryStateFailed, fmt.Sprintf("expected exactly 1 cluster manifest, got %d", len(clusterManifests)))
		return nil
	}

	// Parse cluster spec and derive cluster name from managed resource ID
	clusterManifest := clusterManifests[0]
	spec, err := ParseClusterSpec(clusterManifest.Raw)
	if err != nil {
		a.failDelivery(ctx, progress, domain.DeliveryStateFailed, fmt.Sprintf("failed to parse cluster spec: %v", err))
		return nil
	}
	spec.Name = string(clusterManifest.ManifestID)
	if err := ValidateClusterName(spec.Name); err != nil {
		a.failDelivery(ctx, progress, domain.DeliveryStateFailed, fmt.Sprintf("invalid cluster name: %v", err))
		return nil
	}

	if !a.acceptGeneration(spec.Name, generation) {
		a.observer.Info("rejecting stale delivery", "cluster", spec.Name, "generation", generation)
		a.failDelivery(ctx, progress, domain.DeliveryStateFailed, fmt.Sprintf("stale generation %d for cluster %s", generation, spec.Name))
		return nil
	}

	// Check auth token is non-empty
	if auth.Token == "" {
		a.failDelivery(ctx, progress, domain.DeliveryStateAuthFailed, "auth token is required")
		return nil
	}

	// Extract target config from properties
	targetCfg := TargetConfigFromProperties(target.Properties())

	// Launch async provisioning with per-cluster serialization
	lock := a.clusterLock(spec.Name)
	lock.Lock()
	asyncCtx := context.WithoutCancel(ctx)
	go func() {
		defer lock.Unlock()
		a.deliverAsync(asyncCtx, spec, targetCfg, string(auth.Token), progress)
	}()

	return nil
}

func (a *Agent) failDelivery(ctx context.Context, progress *deliveryProgress, state domain.DeliveryState, msg string) {
	a.observer.Error(msg)
	reportCtx := context.WithoutCancel(ctx)
	if err := progress.Complete(reportCtx, domain.DeliveryResult{State: state, Message: msg}); err != nil {
		a.observer.Error("failed to report delivery failure", "error", err)
	}
}

func newReconcileContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, reconcileTimeout)
}

// deliverAsync performs the asynchronous cluster provisioning.
// It calls the reconciler and reports completion through the reporter.
func (a *Agent) deliverAsync(
	ctx context.Context,
	spec ClusterSpec,
	target TargetConfig,
	callerToken string,
	progress *deliveryProgress,
) {
	runCtx, cancel := newReconcileContext(ctx)
	defer cancel()

	output, err := a.reconciler.Ensure(runCtx, spec, target, callerToken, progress)
	if err != nil {
		a.observer.Error("reconcile failed", "error", err, "cluster", spec.Name)
		if reportErr := progress.Complete(ctx, deliveryResultForReconcileError(err)); reportErr != nil {
			a.observer.Error("failed to report reconcile failure", "error", reportErr, "cluster", spec.Name)
		}
		return
	}
	output.TrustBundles = a.TrustBundles()

	// Build delivery result from cluster output
	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		ProvisionedTargets: []domain.ProvisionedTarget{output.Target()},
		ProducedSecrets:    output.Secrets(),
	}

	a.observer.Info("cluster provisioned successfully", "cluster", spec.Name, "target_id", output.TargetID)
	if reportErr := progress.Complete(ctx, result); reportErr != nil {
		a.observer.Error("failed to report delivery result", "error", reportErr, "cluster", spec.Name)
	}
}

// deleteAsync performs asynchronous cluster deletion for crash recovery.
// It mirrors deliverAsync but calls reconciler.Delete instead of Reconcile.
func (a *Agent) deleteAsync(
	ctx context.Context,
	spec ClusterSpec,
	target TargetConfig,
	callerToken string,
	progress *deliveryProgress,
) {
	runCtx, cancel := newReconcileContext(ctx)
	defer cancel()

	a.observer.Info("deleting cluster", "cluster", spec.Name)
	if err := a.reconciler.Delete(runCtx, spec, target, callerToken, progress); err != nil {
		a.observer.Error("reconcile failed", "error", err, "cluster", spec.Name)
		if reportErr := progress.Complete(ctx, deliveryResultForReconcileError(err)); reportErr != nil {
			a.observer.Error("failed to report reconcile failure", "error", reportErr, "cluster", spec.Name)
		}
		return
	}

	a.clusterMu.Lock()
	delete(a.clusterGen, spec.Name)
	a.clusterMu.Unlock()

	a.observer.Info("cluster deleted successfully", "cluster", spec.Name)
	if reportErr := progress.Complete(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered}); reportErr != nil {
		a.observer.Error("failed to report removal result", "error", reportErr, "cluster", spec.Name)
	}
}

// Remove implements domain.DeliveryAgent.Remove.
// It deletes clusters specified in the manifests. All outcomes
// (success, auth failure, reconciler error) are reported via the
// DeliveryReporter; the return value is reserved for infrastructure
// failures (e.g. unparseable manifests).
func (a *Agent) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	deliveryID domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	generation domain.Generation,
) error {
	progress := newDeliveryProgress(a.reporter, deliveryID, generation)

	// Extract target config from properties
	targetCfg := TargetConfigFromProperties(target.Properties())

	// Process each cluster manifest
	for _, m := range manifests {
		if m.ManifestType == domain.TrustBundleManifestType {
			entry, err := a.removeTrustBundle(m)
			if err != nil {
				a.observer.Error("failed to remove trust bundle", "error", err)
				return fmt.Errorf("failed to remove trust bundle: %w", err)
			}
			a.observer.Info("removed trust bundle", "issuer", entry.IssuerURL)
			continue
		}
		if m.ManifestType != ClusterManifestType {
			continue
		}

		// Parse cluster spec and derive cluster name from managed resource ID
		spec, err := ParseClusterSpec(m.Raw)
		if err != nil {
			a.observer.Error("failed to parse cluster spec for removal", "error", err)
			return fmt.Errorf("failed to parse cluster spec: %w", err)
		}
		spec.Name = string(m.ManifestID)

		if !a.acceptGeneration(spec.Name, generation) {
			a.observer.Info("rejecting stale removal", "cluster", spec.Name, "generation", generation)
			continue
		}

		lock := a.clusterLock(spec.Name)
		lock.Lock()

		// Delete the cluster
		a.observer.Info("deleting cluster", "cluster", spec.Name)
		if err := a.reconciler.Delete(ctx, spec, targetCfg, string(auth.Token), progress); err != nil {
			lock.Unlock()
			a.observer.Error("failed to delete cluster", "error", err, "cluster", spec.Name)
			if IsAuthExpiredError(err) || containsInvalidGrant(err) {
				a.failDelivery(ctx, progress, domain.DeliveryStateAuthFailed,
					fmt.Sprintf("auth expired deleting cluster %s: %v", spec.Name, err))
			} else {
				a.failDelivery(ctx, progress, domain.DeliveryStateFailed,
					fmt.Sprintf("failed to delete cluster %s: %v", spec.Name, err))
			}
			return nil
		}

		a.clusterMu.Lock()
		delete(a.clusterGen, spec.Name)
		a.clusterMu.Unlock()

		lock.Unlock()
		a.observer.Info("cluster deleted successfully", "cluster", spec.Name)
	}

	asyncCtx := context.WithoutCancel(ctx)
	go func() {
		if err := progress.Complete(asyncCtx, domain.DeliveryResult{State: domain.DeliveryStateDelivered}); err != nil {
			a.observer.Error("failed to report removal completion", "error", err)
		}
	}()
	return nil
}

func (a *Agent) storeTrustBundle(m domain.Manifest) (domain.TrustBundleEntry, error) {
	entry, err := parseTrustBundleManifest(m)
	if err != nil {
		return domain.TrustBundleEntry{}, err
	}

	a.trustMu.Lock()
	defer a.trustMu.Unlock()
	a.trustMap[entry.IssuerURL] = entry
	return entry, nil
}

func (a *Agent) removeTrustBundle(m domain.Manifest) (domain.TrustBundleEntry, error) {
	entry, err := parseTrustBundleManifest(m)
	if err != nil {
		return domain.TrustBundleEntry{}, err
	}

	a.trustMu.Lock()
	defer a.trustMu.Unlock()
	delete(a.trustMap, entry.IssuerURL)
	return entry, nil
}

func parseTrustBundleManifest(m domain.Manifest) (domain.TrustBundleEntry, error) {
	var entry domain.TrustBundleEntry
	if err := json.Unmarshal(m.Raw, &entry); err != nil {
		return domain.TrustBundleEntry{}, err
	}
	return entry, nil
}
