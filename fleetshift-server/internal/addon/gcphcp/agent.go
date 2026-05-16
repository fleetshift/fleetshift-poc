package gcphcp

import (
	"context"
	"encoding/json"
	"fmt"
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
}

// Agent implements the domain.DeliveryAgent interface for the GCP HCP addon.
// It coordinates cluster provisioning and deletion through the Reconciler.
type Agent struct {
	reconciler *Reconciler
	observer   AgentObserver
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
	}
}

// TrustBundles returns the current trust bundles stored in the agent.
// This delegates to the reconciler's trust bundle storage.
func (a *Agent) TrustBundles() []domain.TrustBundleEntry {
	return a.reconciler.TrustBundles()
}

// Deliver implements domain.DeliveryAgent.Deliver.
// It processes manifests in two categories:
// 1. Trust bundle manifests - stored immediately
// 2. Cluster manifests - provisioned asynchronously
//
// For trust-bundle-only deliveries, it signals completion immediately.
// For cluster deliveries, it returns Accepted and provisions asynchronously.
func (a *Agent) Deliver(
	ctx context.Context,
	target domain.TargetInfo,
	_ domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	signaler *domain.DeliverySignaler,
) (domain.DeliveryResult, error) {
	// Separate trust bundles from cluster manifests
	var trustBundles []domain.Manifest
	var clusterManifests []domain.Manifest

	for _, m := range manifests {
		if m.ResourceType == domain.TrustBundleResourceType {
			trustBundles = append(trustBundles, m)
		} else {
			clusterManifests = append(clusterManifests, m)
		}
	}

	// Process trust bundles
	for _, tb := range trustBundles {
		var entry domain.TrustBundleEntry
		if err := json.Unmarshal(tb.Raw, &entry); err != nil {
			a.observer.Error("failed to unmarshal trust bundle", "error", err)
			return domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: fmt.Sprintf("failed to unmarshal trust bundle: %v", err),
			}, nil
		}
		a.reconciler.StoreTrustBundle(entry)
		a.observer.Info("stored trust bundle", "issuer", entry.IssuerURL)
	}

	// If only trust bundles (no cluster manifests), signal done and return accepted
	if len(clusterManifests) == 0 {
		asyncCtx := context.WithoutCancel(ctx)
		go signaler.Done(asyncCtx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
	}

	// Expect exactly 1 cluster manifest
	if len(clusterManifests) != 1 {
		msg := fmt.Sprintf("expected exactly 1 cluster manifest, got %d", len(clusterManifests))
		a.observer.Error(msg)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: msg,
		}, nil
	}

	// Parse cluster spec
	spec, err := ParseClusterSpec(clusterManifests[0].Raw)
	if err != nil {
		a.observer.Error("failed to parse cluster spec", "error", err)
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("failed to parse cluster spec: %v", err),
		}, nil
	}

	// Check auth token is non-empty
	if auth.Token == "" {
		a.observer.Error("missing auth token")
		return domain.DeliveryResult{
			State:   domain.DeliveryStateAuthFailed,
			Message: "auth token is required",
		}, nil
	}

	// Extract target config from properties
	targetCfg := targetConfigFromProperties(target.Properties)

	// Launch async provisioning
	asyncCtx := context.WithoutCancel(ctx)
	go a.deliverAsync(asyncCtx, spec, targetCfg, string(auth.Token), signaler)

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func newReconcileContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, reconcileTimeout)
}

// deliverAsync performs the asynchronous cluster provisioning.
// It calls the reconciler and signals completion through the signaler.
func (a *Agent) deliverAsync(
	ctx context.Context,
	spec ClusterSpec,
	target TargetConfig,
	callerToken string,
	signaler *domain.DeliverySignaler,
) {
	runCtx, cancel := newReconcileContext(ctx)
	defer cancel()

	output, err := a.reconciler.Reconcile(runCtx, spec, target, callerToken, signaler)
	if err != nil {
		a.observer.Error("reconcile failed", "error", err, "cluster", spec.Name)
		signaler.Done(ctx, deliveryResultForReconcileError(err))
		return
	}

	// Build delivery result from cluster output
	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		ProvisionedTargets: []domain.ProvisionedTarget{output.Target()},
		ProducedSecrets:    output.Secrets(),
	}

	a.observer.Info("cluster provisioned successfully", "cluster", spec.Name, "target_id", output.TargetID)
	signaler.Done(ctx, result)
}

// Remove implements domain.DeliveryAgent.Remove.
// It deletes clusters specified in the manifests.
func (a *Agent) Remove(
	ctx context.Context,
	target domain.TargetInfo,
	_ domain.DeliveryID,
	manifests []domain.Manifest,
	auth domain.DeliveryAuth,
	_ *domain.Attestation,
	signaler *domain.DeliverySignaler,
) error {
	// Extract target config from properties
	targetCfg := targetConfigFromProperties(target.Properties)

	// Process each cluster manifest
	for _, m := range manifests {
		if m.ResourceType != ClusterResourceType {
			continue
		}

		// Parse cluster spec
		spec, err := ParseClusterSpec(m.Raw)
		if err != nil {
			a.observer.Error("failed to parse cluster spec for removal", "error", err)
			return fmt.Errorf("failed to parse cluster spec: %w", err)
		}

		// Delete the cluster
		a.observer.Info("deleting cluster", "cluster", spec.Name)
		if err := a.reconciler.Delete(ctx, spec, targetCfg, string(auth.Token), signaler); err != nil {
			a.observer.Error("failed to delete cluster", "error", err, "cluster", spec.Name)
			return fmt.Errorf("failed to delete cluster %s: %w", spec.Name, err)
		}

		a.observer.Info("cluster deleted successfully", "cluster", spec.Name)
	}

	return nil
}

// targetConfigFromProperties maps domain.TargetInfo.Properties to TargetConfig.
// It extracts the GCP-specific configuration from the properties map.
func targetConfigFromProperties(props map[string]string) TargetConfig {
	return TargetConfig{
		ID:                props["id"],
		GCPProject:        props["gcp_project"],
		Region:            props["region"],
		WorkforcePool:     props["workforce_pool"],
		WorkforceProvider: props["workforce_provider"],
		BrokerSAEmail:     props["broker_sa_email"],
	}
}
