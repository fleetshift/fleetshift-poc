package kubernetes

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// GenericInformer performs LIST+WATCH for a single GVR and sends events to
// channels. It tracks only UID -> resourceVersion for minimal memory usage.
type GenericInformer struct {
	client        dynamic.Interface
	gvr           schema.GroupVersionResource
	resourceIndex map[string]string // UID -> resourceVersion
	initialized   atomic.Bool
	retries       int64
	eventCh       chan<- ResourceEvent
	resyncCh      chan<- ResyncEvent
	nsFilter      *NamespaceFilter
	listRV        string // saved resourceVersion from last LIST for watch continuity
	logger        *slog.Logger
}

// NewInformer creates a GenericInformer for the given GVR. Events are sent to
// eventCh and resync snapshots to resyncCh. If nsFilter is non-nil, only
// resources in allowed namespaces are forwarded.
func NewInformer(
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	eventCh chan<- ResourceEvent,
	resyncCh chan<- ResyncEvent,
	nsFilter *NamespaceFilter,
	logger *slog.Logger,
) *GenericInformer {
	return &GenericInformer{
		client:        client,
		gvr:           gvr,
		resourceIndex: make(map[string]string),
		eventCh:       eventCh,
		resyncCh:      resyncCh,
		nsFilter:      nsFilter,
		logger:        logger.With("gvr", gvr.String()),
	}
}

// Run starts the informer loop. It blocks until ctx is cancelled.
// On shutdown it sends Delete events for all tracked resources.
func (i *GenericInformer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			i.logger.Info("informer stopped")
			for uid := range i.resourceIndex {
				i.logger.Debug("removing tracked resource on stop", "uid", uid)
				obj := newUnstructured(i.gvr.Resource, uid)
				i.eventCh <- ResourceEvent{
					Op:       EventDelete,
					Resource: obj,
					GVR:      i.gvr,
				}
			}
			return
		default:
			if i.retries > 0 {
				// Backoff: 2s increments, max 2min.
				wait := time.Duration(min(i.retries*2, 120)) * time.Second
				i.logger.Debug("backoff before retry", "wait", wait)
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					continue // re-enter loop to hit the ctx.Done() case above
				}
			}

			i.logger.Debug("starting list and resync")
			err := i.listAndResync(ctx)
			if err == nil {
				i.initialized.Store(true)
				i.watch(ctx)
			}
		}
	}
}

// newUnstructured creates a minimal unstructured object with the given kind and UID.
func newUnstructured(kind, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"kind": kind,
			"metadata": map[string]any{
				"uid": uid,
			},
		},
	}
}

// listAndResync does a paginated LIST, sends Add events for each resource,
// deletes stale resources, and sends a ResyncEvent with the full set.
func (i *GenericInformer) listAndResync(ctx context.Context) error {
	newResourceIndex := make(map[string]string)
	var allResources []*unstructured.Unstructured

	opts := metav1.ListOptions{Limit: 250}
	for {
		resources, err := i.client.Resource(i.gvr).List(ctx, opts)
		if err != nil {
			if ctx.Err() == nil {
				i.logger.Warn("error listing resources", "error", err)
			}
			i.retries++
			return err
		}

		for idx := range resources.Items {
			item := &resources.Items[idx]

			// Namespace filtering: skip resources in disallowed namespaces.
			if i.nsFilter != nil && !i.nsFilter.IsNamespaceAllowed(item.GetNamespace()) {
				continue
			}

			uid := string(item.GetUID())
			rv := item.GetResourceVersion()

			i.logger.Debug("listed resource", "uid", uid, "rv", rv)
			i.eventCh <- ResourceEvent{
				Op:       EventAdd,
				Resource: item,
				GVR:      i.gvr,
			}
			newResourceIndex[uid] = rv
			allResources = append(allResources, item)
		}

		i.logger.Debug("list page complete",
			"group", i.gvr.Group,
			"resource", i.gvr.Resource,
			"count", len(resources.Items),
			"rv", resources.GetResourceVersion())

		// Check for remaining pages.
		metadata := resources.UnstructuredContent()["metadata"].(map[string]any)
		if metadata["remainingItemCount"] != nil && metadata["remainingItemCount"] != 0 {
			opts.Continue = metadata["continue"].(string)
		} else {
			// Save the list resourceVersion for the subsequent watch.
			i.listRV = resources.GetResourceVersion()
			break
		}
	}

	// Delete stale resources that existed before but are no longer present.
	for uid := range i.resourceIndex {
		if _, exists := newResourceIndex[uid]; !exists {
			i.logger.Debug("deleting stale resource", "uid", uid)
			obj := newUnstructured(i.gvr.Resource, uid)
			i.eventCh <- ResourceEvent{
				Op:       EventDelete,
				Resource: obj,
				GVR:      i.gvr,
			}
		}
	}

	// BUG FIX 1: write newResourceIndex back (search-collector never did this).
	i.resourceIndex = newResourceIndex

	// Send resync with full resource set after list completes.
	i.resyncCh <- ResyncEvent{
		GVR:       i.gvr,
		Resources: allResources,
	}

	return nil
}

// watch starts a WATCH from the last list's resourceVersion and processes events.
func (i *GenericInformer) watch(ctx context.Context) {
	// BUG FIX 2: pass the list's resourceVersion instead of empty ListOptions.
	watcher, err := i.client.Resource(i.gvr).Watch(ctx, metav1.ListOptions{
		ResourceVersion: i.listRV,
	})
	if err != nil {
		i.logger.Warn("error starting watch", "error", err)
		i.retries++
		return
	}
	defer watcher.Stop()

	i.logger.Info("watching", "group", i.gvr.Group, "resource", i.gvr.Resource)
	i.retries = 0 // Reset retries on successful list + watch.

	for {
		select {
		case <-ctx.Done():
			i.logger.Debug("watch stopped by context")
			return

		case event, ok := <-watcher.ResultChan():
			if !ok {
				i.logger.Debug("watch channel closed")
				return
			}

			switch event.Type {
			case watch.Added:
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					i.logger.Warn("cannot convert ADDED event object to Unstructured")
					continue
				}
				if i.nsFilter != nil && !i.nsFilter.IsNamespaceAllowed(obj.GetNamespace()) {
					continue
				}
				i.eventCh <- ResourceEvent{
					Op:       EventAdd,
					Resource: obj,
					GVR:      i.gvr,
				}
				i.resourceIndex[string(obj.GetUID())] = obj.GetResourceVersion()

			case watch.Modified:
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					i.logger.Warn("cannot convert MODIFIED event object to Unstructured")
					continue
				}
				if i.nsFilter != nil && !i.nsFilter.IsNamespaceAllowed(obj.GetNamespace()) {
					continue
				}
				i.eventCh <- ResourceEvent{
					Op:       EventUpdate,
					Resource: obj,
					GVR:      i.gvr,
				}
				i.resourceIndex[string(obj.GetUID())] = obj.GetResourceVersion()

			case watch.Deleted:
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					i.logger.Warn("cannot convert DELETED event object to Unstructured")
					continue
				}
				if i.nsFilter != nil && !i.nsFilter.IsNamespaceAllowed(obj.GetNamespace()) {
					continue
				}
				i.eventCh <- ResourceEvent{
					Op:       EventDelete,
					Resource: obj,
					GVR:      i.gvr,
				}
				delete(i.resourceIndex, string(obj.GetUID()))

			case watch.Error:
				i.logger.Warn("received ERROR event, ending watch", "event", event)
				return

			default:
				i.logger.Warn("received unexpected event type, ending watch", "type", event.Type)
				return
			}
		}
	}
}

// WaitUntilInitialized blocks until the informer has completed its initial LIST,
// the context is cancelled, or the timeout expires.
func (i *GenericInformer) WaitUntilInitialized(ctx context.Context, timeout time.Duration) {
	start := time.Now()
	for !i.initialized.Load() {
		if time.Since(start) > timeout {
			i.logger.Warn("timed out waiting for initialization", "timeout", timeout)
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// InformerManager manages the lifecycle of GenericInformer instances. It
// reconciles running informers against a desired set of GVRs, starting new
// informers and stopping removed ones.
type InformerManager struct {
	client    dynamic.Interface
	discovery discovery.DiscoveryInterface
	eventCh   chan<- ResourceEvent
	resyncCh  chan<- ResyncEvent
	nsFilter  *NamespaceFilter
	stoppers  map[schema.GroupVersionResource]context.CancelFunc
	logger    *slog.Logger
}

// NewInformerManager creates an InformerManager. If nsFilter is non-nil it is
// passed to each GenericInformer to restrict events by namespace.
func NewInformerManager(
	client dynamic.Interface,
	disc discovery.DiscoveryInterface,
	eventCh chan<- ResourceEvent,
	resyncCh chan<- ResyncEvent,
	nsFilter *NamespaceFilter,
	logger *slog.Logger,
) *InformerManager {
	return &InformerManager{
		client:    client,
		discovery: disc,
		eventCh:   eventCh,
		resyncCh:  resyncCh,
		nsFilter:  nsFilter,
		stoppers:  make(map[schema.GroupVersionResource]context.CancelFunc),
		logger:    logger,
	}
}

// Reconcile adjusts running informers to match the desired set of GVRs.
// The caller is responsible for filtering desired to supported/allowed GVRs
// (see discoverAndReconcile). Reconcile stops informers for removed GVRs
// and starts informers for new ones. New informers are started serially
// and each waits up to 10s for initialization to avoid memory spikes.
func (m *InformerManager) Reconcile(ctx context.Context, desired []schema.GroupVersionResource) {
	m.logger.Info("reconciling informers", "running", len(m.stoppers), "desired", len(desired))

	desiredSet := make(map[schema.GroupVersionResource]struct{}, len(desired))
	for _, gvr := range desired {
		desiredSet[gvr] = struct{}{}
	}

	// Stop informers that are no longer desired; keep the rest.
	// Also remove already-running GVRs from desiredSet so we only start new ones.
	for gvr, stopper := range m.stoppers {
		if _, ok := desiredSet[gvr]; ok {
			// Already running, don't restart.
			delete(desiredSet, gvr)
		} else {
			// No longer desired, stop.
			m.logger.Info("stopping informer", "gvr", gvr.String())
			stopper()
			delete(m.stoppers, gvr)
		}
	}

	// Start new informers for the remaining desired GVRs.
	for gvr := range desiredSet {
		m.logger.Info("informer started", "gvr", gvr.String())
		informer := NewInformer(m.client, gvr, m.eventCh, m.resyncCh, m.nsFilter, m.logger)
		informerCtx, cancel := context.WithCancel(ctx)
		m.stoppers[gvr] = cancel
		go informer.Run(informerCtx)
		// Serialize startup to avoid memory spikes.
		informer.WaitUntilInitialized(ctx, 10*time.Second)
	}

	m.logger.Info("reconcile complete", "running", len(m.stoppers))
}

// StopAll cancels all running informers.
func (m *InformerManager) StopAll() {
	for gvr, stopper := range m.stoppers {
		m.logger.Info("stopping informer", "gvr", gvr.String())
		stopper()
		delete(m.stoppers, gvr)
	}
}

// crdGVR is the GVR for CustomResourceDefinitions.
var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// RunContinuous performs initial discovery + reconciliation, then starts a CRD
// informer that triggers throttled re-reconciliation whenever CRDs change.
// It blocks until ctx is cancelled.
func (m *InformerManager) RunContinuous(ctx context.Context, denyList, allowList []Resource) {
	// Initial discovery and reconcile.
	m.discoverAndReconcile(ctx, denyList, allowList)

	// Start a CRD informer to detect custom resource changes.
	crdEventCh := make(chan ResourceEvent, 64)
	crdResyncCh := make(chan ResyncEvent, 4)
	crdInformer := NewInformer(m.client, crdGVR, crdEventCh, crdResyncCh, nil, m.logger)

	crdCtx, crdCancel := context.WithCancel(ctx)
	defer crdCancel()
	go crdInformer.Run(crdCtx)
	crdInformer.WaitUntilInitialized(ctx, 10*time.Second)

	// Throttled re-reconciliation: minimum 10s between cycles.
	const minReconcileInterval = 10 * time.Second
	lastReconcile := time.Now()
	pending := false

	// Timer for throttled reconcile — initially stopped.
	reconcileTimer := time.NewTimer(0)
	if !reconcileTimer.Stop() {
		<-reconcileTimer.C
	}

	for {
		select {
		case <-ctx.Done():
			reconcileTimer.Stop()
			return

		case <-crdEventCh:
			// CRD changed — schedule a throttled re-reconcile.
			sinceLastReconcile := time.Since(lastReconcile)
			if sinceLastReconcile >= minReconcileInterval {
				m.discoverAndReconcile(ctx, denyList, allowList)
				lastReconcile = time.Now()
				pending = false
			} else if !pending {
				pending = true
				reconcileTimer.Reset(minReconcileInterval - sinceLastReconcile)
			}
			// Drain any other CRD events that arrived simultaneously.
			drainChannel(crdEventCh)

		case <-crdResyncCh:
			// CRD resync (initial list complete) — trigger reconcile.
			sinceLastReconcile := time.Since(lastReconcile)
			if sinceLastReconcile >= minReconcileInterval {
				m.discoverAndReconcile(ctx, denyList, allowList)
				lastReconcile = time.Now()
				pending = false
			} else if !pending {
				pending = true
				reconcileTimer.Reset(minReconcileInterval - sinceLastReconcile)
			}

		case <-reconcileTimer.C:
			m.discoverAndReconcile(ctx, denyList, allowList)
			lastReconcile = time.Now()
			pending = false
		}
	}
}

// discoverAndReconcile discovers all supported GVRs, filters them, and
// reconciles the running informers.
func (m *InformerManager) discoverAndReconcile(ctx context.Context, denyList, allowList []Resource) {
	supported, err := SupportedResources(m.discovery, m.logger)
	if err != nil {
		if ctx.Err() == nil {
			m.logger.Error("failed to discover supported resources", "error", err)
		}
		if supported == nil {
			return
		}
	}

	desiredGVRs := FilterSupportedResources(supported, denyList, allowList, m.logger)
	m.Reconcile(ctx, desiredGVRs)
}

// drainChannel reads and discards any pending items in a ResourceEvent channel.
func drainChannel(ch <-chan ResourceEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
