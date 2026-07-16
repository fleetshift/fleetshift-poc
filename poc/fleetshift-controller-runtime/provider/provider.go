// Package provider implements a multicluster-runtime Provider that discovers
// FleetShift delivery targets and engages an fsruntime Cluster per target.
//
// It also implements contract.DeliveryAgent: platform Deliver/Remove calls
// are projected into Delivery CRs in the target's store, which the
// controller-runtime reconciler watches. Status updates from the reconciler
// are reported back through the injected DeliveryReporter.
package provider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	deliveryv1 "github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/apis/delivery/v1alpha1"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/contract"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/fsruntime"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/store"
)

var (
	_ multicluster.Provider         = (*Provider)(nil)
	_ multicluster.ProviderRunnable = (*Provider)(nil)
	_ contract.DeliveryAgent        = (*Provider)(nil)
)

// Options configures the FleetShift delivery provider.
type Options struct {
	Scheme   *runtime.Scheme
	Reporter contract.DeliveryReporter
	Logger   logr.Logger
	// Targets are known at construction time. A production provider would
	// discover them dynamically (connect-time seeding, ListActiveDeliveries,
	// or a control-plane watch).
	Targets []contract.TargetInfo
}

// Provider bridges FleetShift's delivery contract to multicluster-runtime.
type Provider struct {
	scheme   *runtime.Scheme
	reporter contract.DeliveryReporter
	logger   logr.Logger

	mu        sync.RWMutex
	targets   map[multicluster.ClusterName]contract.TargetInfo
	clusters  map[multicluster.ClusterName]*fsruntime.Cluster
	cancelFns map[multicluster.ClusterName]context.CancelFunc
	mcAware   multicluster.Aware

	// generationFence tracks the highest accepted generation per
	// (target, fulfillment-ish key). For the POC we fence per delivery
	// object name (= delivery ID) using Spec.Generation.
	genMu   sync.Mutex
	genHigh map[string]contract.Generation // key: targetID/deliveryID

	inflight sync.Map // deliveryID -> struct{}
}

// New creates a Provider.
func New(opts Options) (*Provider, error) {
	if opts.Scheme == nil {
		return nil, fmt.Errorf("provider: Scheme is required")
	}
	if opts.Reporter == nil {
		return nil, fmt.Errorf("provider: Reporter is required")
	}
	if opts.Logger.GetSink() == nil {
		opts.Logger = log.Log.WithName("fleetshift-provider")
	}
	p := &Provider{
		scheme:    opts.Scheme,
		reporter:  opts.Reporter,
		logger:    opts.Logger,
		targets:   make(map[multicluster.ClusterName]contract.TargetInfo),
		clusters:  make(map[multicluster.ClusterName]*fsruntime.Cluster),
		cancelFns: make(map[multicluster.ClusterName]context.CancelFunc),
		genHigh:   make(map[string]contract.Generation),
	}
	for _, t := range opts.Targets {
		p.targets[multicluster.ClusterName(t.ID)] = t
	}
	return p, nil
}

// Reporter returns the DeliveryReporter (for reconcilers that need to
// report progress/results).
func (p *Provider) Reporter() contract.DeliveryReporter { return p.reporter }

// Start engages a cluster per known target and blocks until ctx is done.
func (p *Provider) Start(ctx context.Context, aware multicluster.Aware) error {
	p.mu.Lock()
	p.mcAware = aware
	targets := make([]contract.TargetInfo, 0, len(p.targets))
	for _, t := range p.targets {
		targets = append(targets, t)
	}
	p.mu.Unlock()

	for _, t := range targets {
		if err := p.engageTarget(ctx, t); err != nil {
			return err
		}
	}

	// Recover in-progress work, mirroring gcphcp.RecoverActiveDeliveries.
	if err := p.recoverActive(ctx); err != nil {
		p.logger.Error(err, "recover active deliveries")
	}

	<-ctx.Done()
	return nil
}

func (p *Provider) engageTarget(ctx context.Context, t contract.TargetInfo) error {
	name := multicluster.ClusterName(t.ID)

	p.mu.Lock()
	if _, ok := p.clusters[name]; ok {
		p.mu.Unlock()
		return nil
	}
	st := store.New(p.scheme)
	cl, err := fsruntime.NewCluster(fsruntime.Options{
		Scheme: p.scheme,
		Store:  st,
		Logger: p.logger.WithValues("target", t.ID),
	})
	if err != nil {
		p.mu.Unlock()
		return err
	}
	clusterCtx, cancel := context.WithCancel(ctx)
	p.clusters[name] = cl
	p.cancelFns[name] = cancel
	aware := p.mcAware
	p.mu.Unlock()

	go func() {
		if err := cl.Start(clusterCtx); err != nil {
			p.logger.Error(err, "cluster stopped", "target", t.ID)
		}
	}()

	// Wait briefly for cache readiness (no informers yet is OK).
	waitCtx, waitCancel := context.WithTimeout(clusterCtx, 5*time.Second)
	defer waitCancel()
	_ = cl.GetCache().WaitForCacheSync(waitCtx)

	if aware != nil {
		if err := aware.Engage(clusterCtx, name, cl); err != nil {
			cancel()
			p.mu.Lock()
			delete(p.clusters, name)
			delete(p.cancelFns, name)
			p.mu.Unlock()
			return fmt.Errorf("engage target %q: %w", t.ID, err)
		}
	}
	p.logger.Info("engaged target", "target", t.ID, "type", t.Type)
	return nil
}

// Get returns the cluster for a target ID.
func (p *Provider) Get(ctx context.Context, clusterName multicluster.ClusterName) (cluster.Cluster, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cl, ok := p.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("target %q: %w", clusterName, multicluster.ErrClusterNotFound)
	}
	return cl, nil
}

// IndexField is a no-op; fsruntime does not support field indexes.
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return nil
}

// Deliver implements contract.DeliveryAgent.
func (p *Provider) Deliver(ctx context.Context, target contract.TargetInfo, deliveryID contract.DeliveryID, manifests []contract.Manifest, auth contract.DeliveryAuth, attestation *contract.Attestation, generation contract.Generation) error {
	return p.upsertDelivery(ctx, target, deliveryID, manifests, auth, generation, contract.DeliveryOperationDeliver)
}

// Remove implements contract.DeliveryAgent.
func (p *Provider) Remove(ctx context.Context, target contract.TargetInfo, deliveryID contract.DeliveryID, manifests []contract.Manifest, auth contract.DeliveryAuth, attestation *contract.Attestation, generation contract.Generation) error {
	return p.upsertDelivery(ctx, target, deliveryID, manifests, auth, generation, contract.DeliveryOperationRemove)
}

func (p *Provider) upsertDelivery(ctx context.Context, target contract.TargetInfo, deliveryID contract.DeliveryID, manifests []contract.Manifest, auth contract.DeliveryAuth, generation contract.Generation, op contract.DeliveryOperation) error {
	if _, loaded := p.inflight.LoadOrStore(string(deliveryID), struct{}{}); loaded {
		// Deduplicate concurrent Deliver for the same ID (at-least-once).
		return nil
	}
	// Clear inflight after projection; the reconciler owns the work.
	defer p.inflight.Delete(string(deliveryID))

	fenceKey := string(target.ID) + "/" + string(deliveryID)
	if !p.acceptGeneration(fenceKey, generation) {
		p.logger.Info("skipping stale delivery", "delivery", deliveryID, "generation", generation)
		return nil
	}

	// Ensure the target cluster exists (dynamic discovery via delivery).
	if err := p.ensureTarget(ctx, target); err != nil {
		return err
	}

	cl, err := p.Get(ctx, multicluster.ClusterName(target.ID))
	if err != nil {
		return err
	}

	manifestType := ""
	manifestJSON := ""
	if len(manifests) > 0 {
		manifestType = string(manifests[0].ManifestType)
		manifestJSON = string(manifests[0].Raw)
	}

	obj := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(deliveryID),
			Namespace: "default",
		},
		Spec: deliveryv1.DeliverySpec{
			DeliveryID:   string(deliveryID),
			TargetID:     string(target.ID),
			Generation:   int64(generation),
			Operation:    string(op),
			ManifestType: manifestType,
			ManifestJSON: manifestJSON,
			AuthToken:    string(auth.Token),
		},
	}

	// Project into the store directly so the write is visible immediately.
	// Reconcilers read via GetClient() (Reflector cache); projection must
	// not depend on informer lag.
	direct := cl.(*fsruntime.Cluster).DirectClient()
	existing := &deliveryv1.Delivery{}
	err = direct.Get(ctx, client.ObjectKey{Namespace: "default", Name: string(deliveryID)}, existing)
	if apierrors.IsNotFound(err) {
		return direct.Create(ctx, obj)
	}
	if err != nil {
		return err
	}
	existing.Spec = obj.Spec
	existing.Status.Reported = false
	existing.Status.Phase = ""
	return direct.Update(ctx, existing)
}

func (p *Provider) ensureTarget(ctx context.Context, t contract.TargetInfo) error {
	name := multicluster.ClusterName(t.ID)
	p.mu.RLock()
	_, ok := p.clusters[name]
	p.mu.RUnlock()
	if ok {
		return nil
	}
	p.mu.Lock()
	p.targets[name] = t
	p.mu.Unlock()
	return p.engageTarget(ctx, t)
}

func (p *Provider) acceptGeneration(key string, gen contract.Generation) bool {
	p.genMu.Lock()
	defer p.genMu.Unlock()
	if current, ok := p.genHigh[key]; ok && gen < current {
		return false
	}
	p.genHigh[key] = gen
	return true
}

func (p *Provider) recoverActive(ctx context.Context) error {
	p.mu.RLock()
	ids := make([]contract.TargetID, 0, len(p.targets))
	for _, t := range p.targets {
		ids = append(ids, t.ID)
	}
	p.mu.RUnlock()

	active, err := p.reporter.ListActiveDeliveries(ctx, ids)
	if err != nil {
		return err
	}
	for _, ad := range active {
		p.logger.Info("recovering active delivery", "delivery", ad.DeliveryID, "target", ad.Target.ID, "generation", ad.Generation)
		if err := p.upsertDelivery(ctx, ad.Target, ad.DeliveryID, ad.Manifests, ad.Auth, ad.Generation, ad.Operation); err != nil {
			p.logger.Error(err, "recover delivery", "delivery", ad.DeliveryID)
		}
	}
	return nil
}
