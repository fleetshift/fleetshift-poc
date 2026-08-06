// Package kind implements a [domain.DeliveryAgent] for managing kind
// (Kubernetes-in-Docker) clusters. Manifests are interpreted as kind
// cluster specifications; delivery creates or updates clusters, and
// removal deletes them.
//
// Ownership is encoded in the kind/docker cluster name as fs--{resourceID}
// so provider.List recovers ownership across agent restarts (including
// create-then-crash before the generation ConfigMap is written). The
// last-accepted delivery generation is stored in a ConfigMap inside the
// cluster and fenced with Get + CheckAndAdvance. Same-generation
// deliveries ensure without recreate; higher generations delete and
// recreate, then establish generation on the replacement. A missing
// ownership ConfigMap on an owned cluster means configuration is
// unknown: delete without advancing, clear local generation state,
// stop inventory watch, recreate from the proposed spec, persist
// generation on the replacement, then ensure. Lower generations
// (deliver and remove) are rejected as stale. List and KubeConfig
// errors are terminal (do not treat as absent / succeed). Every
// CheckAndAdvance that returns GenerationStale fails the delivery.
//
// Intentional toy limitations: fs-- is convention not proof of
// ownership; create-crash before ConfigMap write and tombstone gaps
// after remove/failed recreation need an external journal; peek-then-
// recreate has a concurrency gap; higher gen always recreates;
// persistence failure after recreate leaves a cluster without a
// ConfigMap, so the next retry recreates again.
package kind

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for kind-managed targets.
const TargetType domain.TargetType = "kind"

// ClusterResourceType is the [domain.ResourceType] for kind cluster
// specifications (used in the managed resource system).
const ClusterResourceType domain.ResourceType = "kind.fleetshift.io/Cluster"

// ClusterManifestType is the [domain.ManifestType] for a bare kind
// [ClusterSpec] delivered directly to the kind agent (non-managed
// target deliveries).
const ClusterManifestType domain.ManifestType = "api.kind.cluster"

// ManagedClusterManifestType is the [domain.ManifestType] for a
// managed-resource delivery of a kind cluster. The payload is a
// [domain.ManagedResourceSpecManifest] envelope, not a bare
// [ClusterSpec].
const ManagedClusterManifestType domain.ManifestType = "managed.api.kind.cluster"

// ClusterSpec is the canonical spec for a kind cluster resource. It
// mirrors the proto KindClusterSpec message and is used by both the
// managed resource API and the delivery agent. Structured fields
// (Nodes, Networking) map directly to kind's v1alpha4 Cluster config.
type ClusterSpec struct {
	// TODO: consider kube go-to-protobuf for addons to define this shape once if they want to support a struct + proto
	// Or should tooling allow them to go the other way?

	// Name is the platform resource ID (e.g. "demo" for clusters/demo).
	// The kind/docker cluster name is the ownership-encoded form
	// fs--{Name}; see encodeKindClusterName.
	Name string `json:"name"`
	// ResourceName is the full platform resource name (e.g.
	// "clusters/foo"). Used for inventory reporting and display.
	ResourceName domain.ResourceName `json:"-"`
	Nodes        []NodeSpec          `json:"nodes,omitempty"`
	Networking   *NetworkSpec        `json:"networking,omitempty"`
	OIDC         *OIDCSpec           `json:"oidc,omitempty"`
}

// NodeSpec describes a node in the kind cluster.
type NodeSpec struct {
	Role  string `json:"role"`
	Image string `json:"image,omitempty"`
}

// NetworkSpec holds cluster networking settings.
type NetworkSpec struct {
	APIServerPort int32  `json:"apiServerPort,omitempty"`
	PodSubnet     string `json:"podSubnet,omitempty"`
	ServiceSubnet string `json:"serviceSubnet,omitempty"`
}

func (s ClusterSpec) hasClusterConfig() bool {
	return len(s.Nodes) > 0 || s.Networking != nil
}

// resourceID returns the platform resource ID used for ownership encoding.
func (s ClusterSpec) resourceID() domain.ResourceID {
	if s.ResourceName != "" {
		return s.ResourceName.ID()
	}
	return domain.ResourceID(s.Name)
}

// ClusterProvider abstracts the kind cluster operations needed by the
// delivery agent. [cluster.Provider] satisfies this interface.
type ClusterProvider interface {
	Create(name string, options ...cluster.CreateOption) error
	Delete(name, kubeconfig string) error
	List() ([]string, error)
	KubeConfig(name string, internal bool) (string, error)
}

// ClusterProviderFactory creates a [ClusterProvider] with the given
// logger wired in. Each delivery creates its own provider so that
// kind's log output is captured per-delivery via the
// [domain.DeliveryReporter].
type ClusterProviderFactory func(logger log.Logger) ClusterProvider

// Agent implements [domain.DeliveryAgent] for kind clusters.
type Agent struct {
	reporter        domain.DeliveryReporter
	providerFactory ClusterProviderFactory
	observer        AgentObserver
	oidcCABundle    []byte
	tokenVerifier   domain.OIDCTokenVerifier
	oidcConfig      *domain.OIDCConfig
	inventory       *InventoryWatcher
	generations     GenerationStore
	indexingRuntime kubernetes.IndexingRuntime
	bootstrapSA     func(context.Context, []byte, domain.TargetID) (domain.SecretRef, []byte, error)

	trustMu      sync.RWMutex
	trustBundles []domain.TrustBundleEntry

	// inflight tracks delivery IDs with work currently in progress.
	// The platform provides at-least-once delivery; a retry for a
	// delivery that is already being processed is safely skipped.
	inflight sync.Map // map[domain.DeliveryID]struct{}
}

// AgentOption configures an [Agent].
type AgentOption func(*Agent)

// WithObserver sets the [AgentObserver] for delivery lifecycle events.
func WithObserver(o AgentObserver) AgentOption {
	return func(a *Agent) { a.observer = o }
}

// WithOIDCCABundle sets a PEM-encoded CA certificate for trusting the
// OIDC issuer's TLS. When set, the agent mounts it into kind nodes and
// configures --oidc-ca-file. When empty, the API server uses its system
// trust store.
func WithOIDCCABundle(pem []byte) AgentOption {
	return func(a *Agent) { a.oidcCABundle = pem }
}

// WithTokenVerifier configures the agent to verify the caller's JWT
// before acting on a delivery. This simulates the provenance check
// that a remote delivery agent would perform: verifying that the
// token is valid (signature, expiry, issuer, audience) before
// creating infrastructure on behalf of the caller.
func WithTokenVerifier(v domain.OIDCTokenVerifier, cfg domain.OIDCConfig) AgentOption {
	return func(a *Agent) {
		a.tokenVerifier = v
		a.oidcConfig = &cfg
	}
}

// WithInventoryWatcher registers a watcher that starts per-cluster
// Node informers after successful create and stops them on remove.
func WithInventoryWatcher(w *InventoryWatcher) AgentOption {
	return func(a *Agent) { a.inventory = w }
}

// WithGenerationStore overrides the generation high-water store.
// Tests inject a [MemoryGenerationStore]; production defaults to the
// Kubernetes ConfigMap implementation.
func WithGenerationStore(s GenerationStore) AgentOption {
	return func(a *Agent) { a.generations = s }
}

// WithIndexingRuntime injects the Kubernetes indexing runtime
// used to EnsureIndexer before Delivered and StopIndexer at teardown.
// Nil is allowed for unit tests that do not exercise indexing.
func WithIndexingRuntime(m kubernetes.IndexingRuntime) AgentOption {
	return func(a *Agent) { a.indexingRuntime = m }
}

// WithPlatformSABootstrap overrides platform ServiceAccount credential
// provisioning. Intended for unit tests that cannot reach a real API server.
func WithPlatformSABootstrap(fn func(context.Context, []byte, domain.TargetID) (domain.SecretRef, []byte, error)) AgentOption {
	return func(a *Agent) {
		if fn != nil {
			a.bootstrapSA = fn
		}
	}
}

// NewAgent returns an Agent. The reporter is the addon's client
// interface for communicating delivery updates back to the platform.
func NewAgent(reporter domain.DeliveryReporter, factory ClusterProviderFactory, opts ...AgentOption) *Agent {
	a := &Agent{
		reporter:        reporter,
		providerFactory: factory,
		generations:     newKubeGenerationStore(),
		bootstrapSA:     bootstrapPlatformSA,
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

func (a *Agent) agentObserver() AgentObserver {
	if a.observer != nil {
		return a.observer
	}
	return NoOpAgentObserver{}
}

func (a *Agent) generationStore() GenerationStore {
	if a.generations != nil {
		return a.generations
	}
	return newKubeGenerationStore()
}

// Deliver dispatches on manifest resource type. Trust-bundle manifests
// are stored in memory synchronously. Cluster manifests follow the
// existing async flow. All delivery outcomes are reported through the
// [domain.DeliveryReporter].
func (a *Agent) Deliver(ctx context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	if _, loaded := a.inflight.LoadOrStore(deliveryID, struct{}{}); loaded {
		return nil
	}

	var clusterManifests []domain.Manifest
	for _, m := range manifests {
		switch m.ManifestType {
		case domain.TrustBundleManifestType:
			if err := a.storeTrustBundle(m); err != nil {
				a.inflight.Delete(deliveryID)
				_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
					State:   domain.DeliveryStateFailed,
					Message: fmt.Sprintf("store trust bundle: %v", err),
				})
				return nil
			}
		case ClusterManifestType, ManagedClusterManifestType:
			clusterManifests = append(clusterManifests, m)
		default:
			a.inflight.Delete(deliveryID)
			_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: fmt.Sprintf("unsupported manifest type %q", m.ManifestType),
			})
			return nil
		}
	}

	if len(clusterManifests) == 0 {
		a.inflight.Delete(deliveryID)
		asyncCtx := context.WithoutCancel(ctx)
		go func() {
			_ = a.reporter.ReportResult(asyncCtx, deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		}()
		return nil
	}

	specs, err := a.validateManifests(clusterManifests)
	if err != nil {
		a.inflight.Delete(deliveryID)
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("invalid manifests: %v", err),
		})
		return nil
	}

	if err := a.verifyToken(ctx, auth); err != nil {
		a.inflight.Delete(deliveryID)
		_ = a.reporter.ReportResult(ctx, deliveryID, generation, domain.DeliveryResult{
			State:   domain.DeliveryStateAuthFailed,
			Message: fmt.Sprintf("token verification failed: %v", err),
		})
		return nil
	}

	asyncCtx := context.WithoutCancel(ctx)
	provider := a.providerFactory(NewObserverLogger(asyncCtx, a.reporter, deliveryID, generation, time.Now))

	go a.deliverAsync(asyncCtx, provider, specs, auth, deliveryID, generation)

	return nil
}

// storeTrustBundle unmarshals and stores a trust bundle entry.
func (a *Agent) storeTrustBundle(m domain.Manifest) error {
	var entry domain.TrustBundleEntry
	if err := json.Unmarshal(m.Raw, &entry); err != nil {
		return fmt.Errorf("unmarshal trust bundle entry: %w", err)
	}
	a.trustMu.Lock()
	defer a.trustMu.Unlock()
	a.trustBundles = append(a.trustBundles, entry)
	return nil
}

// TrustBundles returns a snapshot of the currently stored trust bundle
// entries. Used by tests and the cluster provisioning flow.
func (a *Agent) TrustBundles() []domain.TrustBundleEntry {
	a.trustMu.RLock()
	defer a.trustMu.RUnlock()
	out := make([]domain.TrustBundleEntry, len(a.trustBundles))
	copy(out, a.trustBundles)
	return out
}

// verifyToken checks the caller's JWT when a verifier is configured.
// This simulates the provenance responsibility of a remote delivery
// agent: refuse to act if the token is expired, has a bad signature,
// or targets the wrong audience.
func (a *Agent) verifyToken(ctx context.Context, auth domain.DeliveryAuth) error {
	if a.tokenVerifier == nil || a.oidcConfig == nil {
		return nil
	}
	if auth.Token == "" {
		return nil
	}
	_, err := a.tokenVerifier.Verify(ctx, *a.oidcConfig, string(auth.Token))
	return err
}

// Remove deletes kind clusters described by the manifests.
// Clusters that are already gone are silently skipped.
// Lower-generation removals are rejected when the ownership ConfigMap
// records a higher generation. Like Deliver, the work runs
// asynchronously and reports via [domain.DeliveryReporter.ReportResult].
// StopIndexer runs once teardown is committed (after the generation
// fence), or when the cluster is already gone; it is not called on
// stale rejects. Teardown continues if stop fails.
func (a *Agent) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	specs, err := a.validateManifests(manifests)
	if err != nil {
		return fmt.Errorf("validate manifests: %w", err)
	}

	if _, loaded := a.inflight.LoadOrStore(deliveryID, struct{}{}); loaded {
		return nil
	}

	go func() {
		defer a.inflight.Delete(deliveryID)
		ctx := context.Background()

		provider := a.providerFactory(NewObserverLogger(context.Background(), a.reporter, deliveryID, generation, time.Now))
		for _, spec := range specs {
			kindName, err := encodeKindClusterName(spec.resourceID())
			if err != nil {
				_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
					State: domain.DeliveryStateFailed, Message: err.Error(),
				})
				return
			}
			targetID := domain.TargetID("k8s-" + string(spec.resourceID()))

			listed, err := provider.List()
			if err != nil {
				_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
					State: domain.DeliveryStateFailed, Message: fmt.Sprintf("list kind clusters: %v", err),
				})
				return
			}
			owned, found := findOwnedCluster(listed, spec.resourceID())
			if !found {
				// Cluster already gone: stop any leftover indexer, drop
				// local generation state, and stop inventory observation.
				a.stopIndexer(ctx, targetID)
				a.generationStore().Forget(kindName)
				if a.inventory != nil {
					a.inventory.Unwatch(spec.ResourceName)
				}
				continue
			}
			useInternal := os.Getenv("KIND_EXPERIMENTAL_DOCKER_NETWORK") != ""
			kc, err := provider.KubeConfig(owned, useInternal)
			if err != nil {
				_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
					State: domain.DeliveryStateFailed, Message: fmt.Sprintf("get kubeconfig for %q: %v", owned, err),
				})
				return
			}
			recorded, hasGen, err := a.generationStore().Get(ctx, owned, []byte(kc))
			if err != nil {
				_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
					State: domain.DeliveryStateFailed, Message: fmt.Sprintf("read ownership generation for %q: %v", owned, err),
				})
				return
			}
			if hasGen && generation < recorded {
				_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
					State:   domain.DeliveryStateFailed,
					Message: fmt.Sprintf("stale generation %d for kind cluster %q (recorded %d)", generation, owned, recorded),
				})
				return
			}
			// Committed to teardown: stop the object indexer before delete.
			// Inventory Unwatch stays after confirmed deletion (below).
			a.stopIndexer(ctx, targetID)
			if err := provider.Delete(owned, ""); err != nil {
				_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
					State: domain.DeliveryStateFailed, Message: err.Error(),
				})
				return
			}
			a.generationStore().Forget(owned)
			// Unwatch only after confirmed deletion so a stale or failed
			// remove cannot stop observing a still-running cluster.
			if a.inventory != nil {
				a.inventory.Unwatch(spec.ResourceName)
			}
		}
		_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
			State: domain.DeliveryStateDelivered,
		})
	}()
	return nil
}

func (a *Agent) validateManifests(manifests []domain.Manifest) ([]ClusterSpec, error) {
	specs := make([]ClusterSpec, len(manifests))
	for i, m := range manifests {
		spec, err := normalizeClusterManifest(m)
		if err != nil {
			return nil, err
		}
		if _, err := encodeKindClusterName(spec.resourceID()); err != nil {
			return nil, err
		}
		specs[i] = spec
	}
	return specs, nil
}

func (a *Agent) deliverAsync(ctx context.Context, provider ClusterProvider, specs []ClusterSpec, auth domain.DeliveryAuth, deliveryID domain.DeliveryID, generation domain.Generation) {
	defer a.inflight.Delete(deliveryID)

	var outputs []ClusterOutput

	for _, spec := range specs {
		out, ok := a.deliverCluster(ctx, provider, spec, auth, deliveryID, generation)
		if !ok {
			return
		}
		if out != nil {
			outputs = append(outputs, *out)
		}
	}

	for _, out := range outputs {
		if err := a.ensureIndexerReady(ctx, out, generation); err != nil {
			a.failDelivery(ctx, deliveryID, generation, "ensure indexer for %q: %v", out.ClusterResourceName.ID(), err)
			return
		}
	}

	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	for _, out := range outputs {
		result.ProvisionedTargets = append(result.ProvisionedTargets, out.Target())
		result.ProducedSecrets = append(result.ProducedSecrets, out.Secrets()...)
	}
	// Release before the terminal report so an at-least-once redelivery of
	// the same delivery ID cannot race with this goroutine's exit and be
	// dropped as still in-flight after the result is already observed.
	a.inflight.Delete(deliveryID)
	_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, result)
}

// deliverCluster handles a single cluster spec. Returns the output on
// success and true to continue, or nil and false if the delivery failed
// (reporter.ReportResult already called). EnsureIndexer runs in
// deliverAsync after all clusters succeed.
func (a *Agent) deliverCluster(ctx context.Context, provider ClusterProvider, spec ClusterSpec, auth domain.DeliveryAuth, deliveryID domain.DeliveryID, generation domain.Generation) (*ClusterOutput, bool) {
	ctx, probe := a.agentObserver().ClusterDeliverStarted(ctx, string(spec.resourceID()))
	defer probe.End()

	kindName, err := encodeKindClusterName(spec.resourceID())
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "%v", err)
		return nil, false
	}

	listed, err := provider.List()
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "list kind clusters: %v", err)
		return nil, false
	}

	owned, found := findOwnedCluster(listed, spec.resourceID())
	useInternal := os.Getenv("KIND_EXPERIMENTAL_DOCKER_NETWORK") != ""

	if !found {
		if foreignClusterConflict(listed, spec.resourceID()) {
			probe.Error(fmt.Errorf("kind cluster %q already exists and is not managed by this agent", spec.resourceID()))
			a.failDelivery(ctx, deliveryID, generation, "kind cluster %q already exists and is not managed by this agent", spec.resourceID())
			return nil, false
		}
		if !a.createKindCluster(ctx, provider, probe, spec, auth, kindName, deliveryID, generation) {
			return nil, false
		}
		owned = kindName
		kc, ok := a.requireKubeconfig(ctx, provider, owned, useInternal, deliveryID, generation, probe)
		if !ok {
			return nil, false
		}
		if !a.advanceOrFail(ctx, owned, kc, generation, deliveryID, probe) {
			return nil, false
		}
		return a.ensureCluster(ctx, provider, spec, auth, owned, kc, deliveryID, generation, probe)
	}

	kc, ok := a.requireKubeconfig(ctx, provider, owned, useInternal, deliveryID, generation, probe)
	if !ok {
		return nil, false
	}

	recorded, hasGen, err := a.generationStore().Get(ctx, owned, kc)
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "read ownership generation for %q: %v", owned, err)
		return nil, false
	}

	if !hasGen {
		// Missing ConfigMap: configuration is unknown. Recreate without
		// advancing on the existing cluster (there is nothing to advance).
		_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Kind cluster %q owned without generation record; configuration unknown, recreating for generation %d", owned, generation),
		})
		return a.recreateOwnedCluster(ctx, provider, probe, spec, auth, owned, kindName, useInternal, deliveryID, generation)
	}

	if generation < recorded {
		probe.Error(fmt.Errorf("stale generation %d for kind cluster %q (recorded %d)", generation, owned, recorded))
		a.failDelivery(ctx, deliveryID, generation, "stale generation %d for kind cluster %q (recorded %d)", generation, owned, recorded)
		return nil, false
	}

	if generation == recorded {
		_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Kind cluster %q already at generation %d; skipping create", owned, generation),
		})
		return a.ensureCluster(ctx, provider, spec, auth, owned, kc, deliveryID, generation, probe)
	}

	// Higher generation: recreate without advancing the old ConfigMap.
	_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Recreating kind cluster %q for generation %d (was %d)", owned, generation, recorded),
	})
	return a.recreateOwnedCluster(ctx, provider, probe, spec, auth, owned, kindName, useInternal, deliveryID, generation)
}

// recreateOwnedCluster deletes an owned cluster without advancing its
// generation ConfigMap, clears local generation state, stops inventory
// watch and the object indexer, creates a replacement from the proposed
// spec, persists the proposed generation on the replacement, then runs
// ensureCluster. Used for both higher-generation and missing-ConfigMap
// paths. Failed deletion leaves the existing cluster and inventory watch
// intact; StopIndexer may already have run after prep succeeded.
// Create configuration is prepared before deletion so invalid OIDC input,
// CA-file failure, or config construction failure cannot destroy the
// existing cluster.
func (a *Agent) recreateOwnedCluster(
	ctx context.Context,
	provider ClusterProvider,
	probe ClusterDeliverProbe,
	spec ClusterSpec,
	auth domain.DeliveryAuth,
	owned, kindName string,
	useInternal bool,
	deliveryID domain.DeliveryID,
	generation domain.Generation,
) (*ClusterOutput, bool) {
	prep, ok := a.prepareKindCreate(ctx, probe, spec, auth, kindName, deliveryID, generation)
	if !ok {
		return nil, false
	}

	targetID := domain.TargetID("k8s-" + string(spec.resourceID()))
	a.stopIndexer(ctx, targetID)

	if err := provider.Delete(owned, ""); err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "delete kind cluster %q for recreate: %v", owned, err)
		return nil, false
	}
	a.generationStore().Forget(owned)
	// Drop the old informer so ensureCluster can Watch the replacement
	// (Watch is a no-op while a watch for the resource name exists).
	if a.inventory != nil {
		a.inventory.Unwatch(spec.ResourceName)
	}

	if !a.createPreparedKindCluster(ctx, provider, probe, kindName, prep, deliveryID, generation) {
		return nil, false
	}
	kc, ok := a.requireKubeconfig(ctx, provider, kindName, useInternal, deliveryID, generation, probe)
	if !ok {
		return nil, false
	}
	if !a.advanceOrFail(ctx, kindName, kc, generation, deliveryID, probe) {
		return nil, false
	}
	return a.ensureCluster(ctx, provider, spec, auth, kindName, kc, deliveryID, generation, probe)
}

// preparedKindCreate holds create options resolved before any destructive
// recreate step.
type preparedKindCreate struct {
	opts []cluster.CreateOption
}

func (a *Agent) prepareKindCreate(
	ctx context.Context,
	probe ClusterDeliverProbe,
	spec ClusterSpec,
	auth domain.DeliveryAuth,
	kindName string,
	deliveryID domain.DeliveryID,
	generation domain.Generation,
) (preparedKindCreate, bool) {
	rawConfig, source, err := a.resolveConfig(spec, auth)
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "resolve config for kind cluster %q: %v", kindName, err)
		return preparedKindCreate{}, false
	}

	var issuer domain.IssuerURL
	var aud domain.Audience
	if source == ConfigSourceOIDC {
		issuer = auth.Caller.Issuer
		aud = auth.Audience[0]
	}
	probe.ConfigResolved(source, issuer, aud)

	var opts []cluster.CreateOption
	if rawConfig != nil {
		opts = append(opts, cluster.CreateWithRawConfig(rawConfig))
	}
	return preparedKindCreate{opts: opts}, true
}

func (a *Agent) createPreparedKindCluster(
	ctx context.Context,
	provider ClusterProvider,
	probe ClusterDeliverProbe,
	kindName string,
	prep preparedKindCreate,
	deliveryID domain.DeliveryID,
	generation domain.Generation,
) bool {
	if err := provider.Create(kindName, prep.opts...); err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "create kind cluster %q: %v", kindName, err)
		return false
	}
	return true
}

func (a *Agent) createKindCluster(ctx context.Context, provider ClusterProvider, probe ClusterDeliverProbe, spec ClusterSpec, auth domain.DeliveryAuth, kindName string, deliveryID domain.DeliveryID, generation domain.Generation) bool {
	prep, ok := a.prepareKindCreate(ctx, probe, spec, auth, kindName, deliveryID, generation)
	if !ok {
		return false
	}
	return a.createPreparedKindCluster(ctx, provider, probe, kindName, prep, deliveryID, generation)
}

func (a *Agent) requireKubeconfig(ctx context.Context, provider ClusterProvider, kindName string, useInternal bool, deliveryID domain.DeliveryID, generation domain.Generation, probe ClusterDeliverProbe) ([]byte, bool) {
	kc, err := provider.KubeConfig(kindName, useInternal)
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "get kubeconfig for %q: %v", kindName, err)
		return nil, false
	}
	return []byte(kc), true
}

func (a *Agent) advanceOrFail(ctx context.Context, kindName string, kc []byte, generation domain.Generation, deliveryID domain.DeliveryID, probe ClusterDeliverProbe) bool {
	disp, recorded, err := a.generationStore().CheckAndAdvance(ctx, kindName, kc, generation)
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "advance ownership generation for %q: %v", kindName, err)
		return false
	}
	if disp == GenerationStale {
		probe.Error(fmt.Errorf("stale generation %d for kind cluster %q (recorded %d)", generation, kindName, recorded))
		a.failDelivery(ctx, deliveryID, generation, "stale generation %d for kind cluster %q (recorded %d)", generation, kindName, recorded)
		return false
	}
	return true
}

func (a *Agent) ensureCluster(ctx context.Context, _ ClusterProvider, spec ClusterSpec, auth domain.DeliveryAuth, kindName string, kc []byte, deliveryID domain.DeliveryID, generation domain.Generation, probe ClusterDeliverProbe) (*ClusterOutput, bool) {
	if auth.Caller != nil {
		_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Bootstrapping RBAC for %s on %q", auth.Caller.Subject, kindName),
		})
		username := string(auth.Caller.Issuer) + "#" + string(auth.Caller.Subject)
		if err := bootstrapRBAC(ctx, kc, auth.Caller.Issuer, auth.Caller); err != nil {
			probe.Error(err)
			a.failDelivery(ctx, deliveryID, generation, "bootstrap RBAC on %q: %v", kindName, err)
			return nil, false
		}
		probe.RBACBootstrapped(auth.Caller.Subject, username)
	}

	apiServer, caCert, err := ExtractClusterConnInfo(kc)
	if err != nil {
		probe.Error(err)
		a.failDelivery(ctx, deliveryID, generation, "extract connection info for %q: %v", kindName, err)
		return nil, false
	}

	platformID := string(spec.resourceID())
	targetID := domain.TargetID("k8s-" + platformID)
	out := ClusterOutput{
		TargetID:            targetID,
		ClusterResourceName: spec.ResourceName,
		APIServer:           apiServer,
		CACert:              caCert,
		TrustBundles:        a.TrustBundles(),
	}

	_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventProgress,
		Message: fmt.Sprintf("Bootstrapping platform ServiceAccount on %q", kindName),
	})
	ref, token, saErr := a.bootstrapSA(ctx, kc, targetID)
	if saErr != nil {
		probe.Error(saErr)
		a.failDelivery(ctx, deliveryID, generation, "platform SA bootstrap on %q: %v", kindName, saErr)
		return nil, false
	}
	out.SATokenRef = ref
	out.SAToken = token

	if a.inventory != nil {
		if err := a.inventory.Watch(spec.ResourceName, kc); err != nil {
			_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
				Kind:    domain.DeliveryEventWarning,
				Message: fmt.Sprintf("start inventory watch for %q: %v", spec.ResourceName, err),
			})
		}
	}

	return &out, true
}

// ensureIndexerReady calls EnsureIndexer when an IndexingRuntime is
// configured. Invalid indexing input is a permanent error.
// Nil runtime is a no-op (unit tests without indexing).
func (a *Agent) ensureIndexerReady(ctx context.Context, out ClusterOutput, generation domain.Generation) error {
	if a.indexingRuntime == nil {
		return nil
	}
	input, err := kubernetes.NewIndexRuntimeInput(
		out.TargetID,
		out.ClusterResourceName,
		out.APIServer,
		string(out.CACert),
		out.SAToken,
		out.SATokenRef,
		generation,
		kubernetes.DefaultIndexConfig(),
	)
	if err != nil {
		return fmt.Errorf("%w: for %s", err, out.TargetID)
	}
	return kubernetes.RetryLocalEnvelope(ctx, kubernetes.LocalEnsureRetryDeadline, func(attemptCtx context.Context) error {
		return a.indexingRuntime.EnsureIndexer(attemptCtx, input)
	})
}

// stopIndexer stops the indexer for targetID when an IndexingRuntime is
// configured. Errors are discarded so teardown is not blocked by a stuck stop.
func (a *Agent) stopIndexer(ctx context.Context, targetID domain.TargetID) {
	if a.indexingRuntime == nil {
		return
	}
	_ = a.indexingRuntime.StopIndexer(ctx, targetID)
}

// failDelivery clears the in-flight mark before reporting a terminal
// failure so at-least-once redelivery of the same delivery ID cannot be
// dropped as still in-flight after the failure result is observed.
func (a *Agent) failDelivery(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, format string, args ...any) {
	a.inflight.Delete(deliveryID)
	msg := fmt.Sprintf(format, args...)
	_ = a.reporter.ReportEvent(ctx, deliveryID, generation, domain.DeliveryEvent{
		Kind:    domain.DeliveryEventError,
		Message: msg,
	})
	_ = reportResultWithRetry(ctx, a.reporter, deliveryID, generation, domain.DeliveryResult{
		State:   domain.DeliveryStateFailed,
		Message: msg,
	})
}

// reportResultWithRetry calls ReportResult under [kubernetes.RetryLocalEnvelope]
// with [kubernetes.ReportResultRetryDeadline].
func reportResultWithRetry(
	ctx context.Context,
	reporter domain.DeliveryReporter,
	deliveryID domain.DeliveryID,
	generation domain.Generation,
	result domain.DeliveryResult,
) error {
	return kubernetes.RetryLocalEnvelope(ctx, kubernetes.ReportResultRetryDeadline, func(attemptCtx context.Context) error {
		return reporter.ReportResult(attemptCtx, deliveryID, generation, result)
	})
}

// resolveConfig returns the raw kind config bytes and the
// [ConfigSource] for a ClusterSpec. When an authenticated caller is
// present, the spec's structured fields (nodes, networking) are used
// as the base config and OIDC API server flags are overlaid on top.
// When no caller is present but the spec has structured fields, those
// are used as-is. Returns nil when neither applies (kind defaults).
func (a *Agent) resolveConfig(spec ClusterSpec, auth domain.DeliveryAuth) ([]byte, ConfigSource, error) {
	if auth.Caller != nil {
		if len(auth.Audience) == 0 {
			return nil, "", fmt.Errorf("%w: OIDC config requires at least one audience", domain.ErrInvalidArgument)
		}
		var caCertHostPath string
		if len(a.oidcCABundle) > 0 {
			path, err := writeCABundle(a.oidcCABundle)
			if err != nil {
				return nil, "", err
			}
			caCertHostPath = path
		}
		oidcSpec := spec.OIDC
		if oidcSpec == nil {
			oidcSpec = &OIDCSpec{}
		}
		// TODO: audience policy -- for now we use the first audience from
		// the caller's token. This couples the cluster's oidc-client-id to
		// whatever audience the platform validated the user against.
		issuer := auth.Caller.Issuer

		config := toKindConfig(spec)
		applyOIDCOverlay(&config, oidcSpec, string(issuer), string(auth.Audience[0]), caCertHostPath)

		raw, err := json.Marshal(config)
		if err != nil {
			return nil, "", fmt.Errorf("marshal oidc kind config: %w", err)
		}
		return raw, ConfigSourceOIDC, nil
	}
	if spec.hasClusterConfig() {
		cfg, err := buildKindConfig(spec)
		if err != nil {
			return nil, "", err
		}
		return cfg, ConfigSourceCustom, nil
	}
	return nil, ConfigSourceDefault, nil
}
