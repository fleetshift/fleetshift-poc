package kubernetes

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
)

// EventOp represents the type of informer event.
type EventOp int

const (
	EventAdd EventOp = iota
	EventUpdate
	EventDelete
)

// ResourceEvent is a single informer event for a Kubernetes resource.
// Generation tags the GVR process generation that produced the event so
// the writer can reject late deliveries after that generation closes.
// Scope is the discovery-authoritative scope bound when the informer
// generation started; it is never inferred from object metadata.
type ResourceEvent struct {
	Op         EventOp
	Resource   *unstructured.Unstructured
	GVR        schema.GroupVersionResource
	Scope      ObjectScope
	Generation uint64
}

// ResyncEvent carries the full resource set for a GVR after an informer
// completes a LIST (initial generation LIST or expired-cursor relist).
// Scope is the discovery-authoritative scope bound when the informer
// generation started; it is never inferred from object metadata.
// When Ack is non-nil, the writer must send one result (nil on success)
// after the LIST write commits or the generation ends; the informer
// waits for that ack before starting WATCH so the cursor is only used
// after persistence succeeds.
type ResyncEvent struct {
	GVR        schema.GroupVersionResource
	Scope      ObjectScope
	Resources  []*unstructured.Unstructured
	Generation uint64
	// Ack receives a single write result. Buffer size 1 so the writer
	// never blocks if the informer has already canceled. Nil means no
	// wait (unit tests that inject events directly).
	Ack chan<- error
}

// RemoveGVREvent signals that a GVR generation is no longer being
// indexed (for example it left the desired set). The writer drops
// in-memory state for that generation only; persisted inventory is left
// unchanged. Scope is the discovery scope that was bound on the removed
// generation (currently unused by the writer). Informer shutdown /
// StopAll must not emit this event — stopping the process is not a
// source-of-truth GVR removal.
type RemoveGVREvent struct {
	GVR        schema.GroupVersionResource
	Scope      ObjectScope
	Generation uint64
}

// watchOutcome classifies how a watch ended so Run can choose between
// resumable reconnect and full LIST.
type watchOutcome int

const (
	watchOutcomeContextCanceled watchOutcome = iota
	watchOutcomeResume
	watchOutcomeNeedList
)

// GenericInformer performs LIST+WATCH for a single GVR and sends events to
// channels. It tracks only UID -> resourceVersion for minimal memory usage.
// WatchResourceVersion is the cursor used to resume a clean watch disconnect;
// it is distinct from the writer's ReportedNames persistence baseline.
// scope is bound from discovery when the informer generation starts and is
// copied onto every emitted event.
type GenericInformer struct {
	client               dynamic.Interface
	gvr                  schema.GroupVersionResource
	scope                ObjectScope
	generation           uint64
	resourceIndex        map[string]string // UID -> resourceVersion
	initialized          atomic.Bool
	retries              int64
	eventCh              chan<- ResourceEvent
	resyncCh             chan<- ResyncEvent
	nsFilter             *NamespaceFilter
	watchResourceVersion string // cursor for clean watch resume
	logger               *slog.Logger
}

// NewInformer creates a GenericInformer for the given GVR and discovery
// scope. Events are sent to eventCh and resync snapshots to resyncCh. If
// nsFilter is non-nil, only resources in allowed namespaces are forwarded.
// Emitted events are untagged (generation 0); use [NewInformerGeneration]
// when the writer must fence by process generation.
func NewInformer(
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	scope ObjectScope,
	eventCh chan<- ResourceEvent,
	resyncCh chan<- ResyncEvent,
	nsFilter *NamespaceFilter,
	logger *slog.Logger,
) *GenericInformer {
	return NewInformerGeneration(client, gvr, scope, 0, eventCh, resyncCh, nsFilter, logger)
}

// NewInformerGeneration is like [NewInformer] but assigns an explicit GVR
// process generation to every emitted ResourceEvent and ResyncEvent so the
// writer can reject late deliveries after that generation closes. scope is
// copied onto every emitted event as the discovery-authoritative
// [ObjectScope] for that generation.
func NewInformerGeneration(
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	scope ObjectScope,
	generation uint64,
	eventCh chan<- ResourceEvent,
	resyncCh chan<- ResyncEvent,
	nsFilter *NamespaceFilter,
	logger *slog.Logger,
) *GenericInformer {
	return &GenericInformer{
		client:        client,
		gvr:           gvr,
		scope:         scope,
		generation:    generation,
		resourceIndex: make(map[string]string),
		eventCh:       eventCh,
		resyncCh:      resyncCh,
		nsFilter:      nsFilter,
		logger:        logger.With("gvr", gvr.String(), "scope", scope, "generation", generation),
	}
}

// Run starts the informer loop. It blocks until ctx is cancelled.
// Shutdown is a runtime lifecycle event, not a Kubernetes source-of-truth
// delete: tracked UIDs are discarded locally and no EventDelete is emitted.
//
// A new generation always begins with LIST. The LIST write must be
// acknowledged by the writer before WATCH starts from that cursor.
// Clean watch terminations resume WATCH from WatchResourceVersion
// without LIST. Expired or unsafe watch endings force another LIST
// (writer reconciles omissions against ReportedNames for this generation).
func (i *GenericInformer) Run(ctx context.Context) {
	needList := true
	for {
		select {
		case <-ctx.Done():
			i.logger.Info("informer stopped")
			return
		default:
		}

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

		if needList || i.watchResourceVersion == "" {
			i.logger.Debug("starting list and resync")
			err := i.listAndResync(ctx)
			if err != nil {
				continue
			}
			i.initialized.Store(true)
			needList = false
		}

		outcome := i.watch(ctx)
		switch outcome {
		case watchOutcomeContextCanceled:
			i.logger.Info("informer stopped")
			return
		case watchOutcomeResume:
			needList = false
		case watchOutcomeNeedList:
			needList = true
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
// and sends a ResyncEvent with the full set. It waits for the writer's
// persistence ack before returning so Run does not start WATCH until the
// LIST write has succeeded (or the generation/context ends). Stale local
// index UIDs are dropped without EventDelete; omission deletes are the
// writer's ReportedNames diff on ResyncEvent.
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
			if !i.sendEvent(ctx, ResourceEvent{
				Op:         EventAdd,
				Resource:   item,
				GVR:        i.gvr,
				Scope:      i.scope,
				Generation: i.generation,
			}) {
				return ctx.Err()
			}
			newResourceIndex[uid] = rv
			allResources = append(allResources, item)
		}

		i.logger.Debug("list page complete",
			"group", i.gvr.Group,
			"resource", i.gvr.Resource,
			"count", len(resources.Items),
			"rv", resources.GetResourceVersion())

		// Pagination is controlled by the continue token. Optional
		// remainingItemCount is not proof that no next page exists.
		if cont := resources.GetContinue(); cont != "" {
			opts.Continue = cont
			continue
		}
		i.watchResourceVersion = resources.GetResourceVersion()
		break
	}

	// Drop UIDs that disappeared from the LIST from the local index only.
	// Do not emit EventDelete for them: the ResyncEvent below becomes an
	// ApplyDelta that upserts the LIST and, when the writer already has
	// ReportedNames for this GVR generation, deletes only those absent from
	// the LIST (a first LIST has none, so upserts only). Per-UID deletes
	// here would only duplicate that reconciliation. Watch tombstones
	// still use EventDelete.
	i.resourceIndex = newResourceIndex

	ack := make(chan error, 1)
	if !i.sendResync(ctx, ResyncEvent{
		GVR:        i.gvr,
		Scope:      i.scope,
		Resources:  allResources,
		Generation: i.generation,
		Ack:        ack,
	}) {
		return ctx.Err()
	}

	select {
	case err := <-ack:
		if err != nil {
			i.retries++
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// watch starts a WATCH from WatchResourceVersion and processes events.
// Returns how the watch ended so Run can resume or relist:
// clean channel close with a cursor resumes; start failures, ERROR
// events, unexpected types, and an empty cursor force LIST. BOOKMARK
// advances the cursor without changing membership.
func (i *GenericInformer) watch(ctx context.Context) watchOutcome {
	watcher, err := i.client.Resource(i.gvr).Watch(ctx, metav1.ListOptions{
		ResourceVersion:     i.watchResourceVersion,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		i.logger.Warn("error starting watch", "error", err)
		i.retries++
		if isExpiredResourceVersionError(err) {
			return watchOutcomeNeedList
		}
		// Watch start failures are not positively classified as clean
		// disconnects; force a LIST to re-establish a safe cursor.
		return watchOutcomeNeedList
	}
	defer watcher.Stop()

	i.logger.Info("watching", "group", i.gvr.Group, "resource", i.gvr.Resource)
	i.retries = 0 // Reset retries on successful watch start.

	for {
		select {
		case <-ctx.Done():
			i.logger.Debug("watch stopped by context")
			return watchOutcomeContextCanceled

		case event, ok := <-watcher.ResultChan():
			if !ok {
				i.logger.Debug("watch channel closed")
				// Ordinary transport / API-server close: resume from cursor.
				if i.watchResourceVersion == "" {
					return watchOutcomeNeedList
				}
				return watchOutcomeResume
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
				if !i.sendEvent(ctx, ResourceEvent{
					Op:         EventAdd,
					Resource:   obj,
					GVR:        i.gvr,
					Scope:      i.scope,
					Generation: i.generation,
				}) {
					return watchOutcomeContextCanceled
				}
				i.resourceIndex[string(obj.GetUID())] = obj.GetResourceVersion()
				i.advanceWatchRV(obj.GetResourceVersion())

			case watch.Modified:
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					i.logger.Warn("cannot convert MODIFIED event object to Unstructured")
					continue
				}
				if i.nsFilter != nil && !i.nsFilter.IsNamespaceAllowed(obj.GetNamespace()) {
					continue
				}
				if !i.sendEvent(ctx, ResourceEvent{
					Op:         EventUpdate,
					Resource:   obj,
					GVR:        i.gvr,
					Scope:      i.scope,
					Generation: i.generation,
				}) {
					return watchOutcomeContextCanceled
				}
				i.resourceIndex[string(obj.GetUID())] = obj.GetResourceVersion()
				i.advanceWatchRV(obj.GetResourceVersion())

			case watch.Deleted:
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					i.logger.Warn("cannot convert DELETED event object to Unstructured")
					continue
				}
				if i.nsFilter != nil && !i.nsFilter.IsNamespaceAllowed(obj.GetNamespace()) {
					continue
				}
				if !i.sendEvent(ctx, ResourceEvent{
					Op:         EventDelete,
					Resource:   obj,
					GVR:        i.gvr,
					Scope:      i.scope,
					Generation: i.generation,
				}) {
					return watchOutcomeContextCanceled
				}
				delete(i.resourceIndex, string(obj.GetUID()))
				i.advanceWatchRV(obj.GetResourceVersion())

			case watch.Bookmark:
				// Bookmark is only resourceVersion progress; it does not
				// change membership and is not a second checkpoint kind.
				if rv := bookmarkResourceVersion(event.Object); rv != "" {
					i.advanceWatchRV(rv)
				}

			case watch.Error:
				i.logger.Warn("received ERROR event, ending watch", "event", event)
				i.retries++
				if isExpiredWatchObject(event.Object) {
					return watchOutcomeNeedList
				}
				return watchOutcomeNeedList

			default:
				i.logger.Warn("received unexpected event type, ending watch", "type", event.Type)
				i.retries++
				return watchOutcomeNeedList
			}
		}
	}
}

// advanceWatchRV updates the resume cursor used after a clean watch
// disconnect. Empty rv is ignored so a malformed event cannot clear a
// known-good cursor.
func (i *GenericInformer) advanceWatchRV(rv string) {
	if rv != "" {
		i.watchResourceVersion = rv
	}
}

// sendEvent delivers an event or returns false when ctx is cancelled.
// WatchResourceVersion is advanced by the caller after the event is
// retained in the in-process queue (successful send).
func (i *GenericInformer) sendEvent(ctx context.Context, ev ResourceEvent) bool {
	select {
	case i.eventCh <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

// sendResync delivers a resync snapshot or returns false when ctx is
// cancelled. It does not wait for ResyncEvent.Ack; [listAndResync] waits
// after a successful send.
func (i *GenericInformer) sendResync(ctx context.Context, ev ResyncEvent) bool {
	select {
	case i.resyncCh <- ev:
		return true
	case <-ctx.Done():
		return false
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

// bookmarkResourceVersion extracts the resourceVersion from a BOOKMARK
// event object. BOOKMARKs may be unstructured or other metav1 objects.
func bookmarkResourceVersion(obj runtime.Object) string {
	switch t := obj.(type) {
	case *unstructured.Unstructured:
		return t.GetResourceVersion()
	case metav1.Object:
		return t.GetResourceVersion()
	default:
		return ""
	}
}

// isExpiredResourceVersionError reports whether err means the watch
// cursor is no longer usable and a fresh LIST is required.
func isExpiredResourceVersionError(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// isExpiredWatchObject reports whether a watch ERROR payload indicates
// an expired/gone resourceVersion (410).
func isExpiredWatchObject(obj runtime.Object) bool {
	if obj == nil {
		return false
	}
	if err, ok := obj.(error); ok {
		return isExpiredResourceVersionError(err)
	}
	return isExpiredStatus(obj)
}

// isExpiredStatus reports whether obj is a metav1.Status with expired
// or gone semantics (HTTP 410 or matching reason).
func isExpiredStatus(obj runtime.Object) bool {
	status, ok := obj.(*metav1.Status)
	if !ok {
		return false
	}
	if status.Code == 410 {
		return true
	}
	return status.Reason == metav1.StatusReasonExpired || status.Reason == metav1.StatusReasonGone
}

// InformerManager manages the lifecycle of GenericInformer instances. It
// reconciles running informers against a desired set of
// [DiscoveredAPIResource] values, starting new informers and stopping
// removed ones. Each start assigns a new GVR process generation so
// remove/re-add cannot share state with a closed generation.
type InformerManager struct {
	client      dynamic.Interface
	discovery   discovery.DiscoveryInterface
	eventCh     chan<- ResourceEvent
	resyncCh    chan<- ResyncEvent
	removeCh    chan<- RemoveGVREvent
	nsFilter    *NamespaceFilter
	stoppers    map[schema.GroupVersionResource]context.CancelFunc
	generations map[schema.GroupVersionResource]uint64
	// scopes records the discovery [ObjectScope] bound when each GVR
	// informer generation started (copied onto RemoveGVREvent).
	scopes  map[schema.GroupVersionResource]ObjectScope
	nextGen uint64
	logger  *slog.Logger

	// informerWG tracks every ordinary and CRD informer goroutine so StopAll
	// can await them after cancellation.
	informerWG sync.WaitGroup
}

// NewInformerManager creates an InformerManager. If nsFilter is non-nil it is
// passed to each GenericInformer to restrict events by namespace. removeCh may
// be nil; when set, Reconcile sends [RemoveGVREvent] for GVRs that leave the
// desired set. StopAll does not send remove events.
func NewInformerManager(
	client dynamic.Interface,
	disc discovery.DiscoveryInterface,
	eventCh chan<- ResourceEvent,
	resyncCh chan<- ResyncEvent,
	removeCh chan<- RemoveGVREvent,
	nsFilter *NamespaceFilter,
	logger *slog.Logger,
) *InformerManager {
	return &InformerManager{
		client:      client,
		discovery:   disc,
		eventCh:     eventCh,
		resyncCh:    resyncCh,
		removeCh:    removeCh,
		nsFilter:    nsFilter,
		stoppers:    make(map[schema.GroupVersionResource]context.CancelFunc),
		generations: make(map[schema.GroupVersionResource]uint64),
		scopes:      make(map[schema.GroupVersionResource]ObjectScope),
		logger:      logger,
	}
}

// Reconcile adjusts running informers to match the desired set of
// [DiscoveredAPIResource] values. The caller is responsible for filtering
// desired to supported/allowed [DiscoveredAPIResource] values (see
// discoverAndReconcile). Reconcile stops informers for removed GVRs and
// starts informers for new ones. New informers are started serially and
// each waits up to 10s for initialization to avoid memory spikes. State
// transitions for a GVR are serialized here so reconnect, relist,
// removal, and fast re-add cannot install two active generations for the
// same GVR concurrently. Scope is bound from each [DiscoveredAPIResource]
// onto the informer generation; same-GVR scope changes are not restarted
// here (deferred discovery quarantine).
func (m *InformerManager) Reconcile(ctx context.Context, desired []DiscoveredAPIResource) {
	m.logger.Info("reconciling informers", "running", len(m.stoppers), "desired", len(desired))

	desiredByGVR := make(map[schema.GroupVersionResource]DiscoveredAPIResource, len(desired))
	for _, desc := range desired {
		desiredByGVR[desc.GVR] = desc
	}

	// Stop informers that are no longer desired; keep the rest.
	// Also remove already-running GVRs from desiredByGVR so we only start new ones.
	for gvr, stopper := range m.stoppers {
		if _, ok := desiredByGVR[gvr]; ok {
			// Already running, don't restart.
			delete(desiredByGVR, gvr)
		} else {
			// No longer desired: stop the informer and tell the writer to
			// drop in-memory state for this generation (non-destructive to
			// persisted inventory). StopAll does not take this path —
			// shutdown is not a desired-set removal.
			gen := m.generations[gvr]
			scope := m.scopes[gvr]
			m.logger.Info("stopping informer", "gvr", gvr.String(), "generation", gen)
			stopper()
			delete(m.stoppers, gvr)
			delete(m.generations, gvr)
			delete(m.scopes, gvr)
			if m.removeCh != nil {
				select {
				case m.removeCh <- RemoveGVREvent{GVR: gvr, Scope: scope, Generation: gen}:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	// Start new informers for the remaining desired [DiscoveredAPIResource] values.
	for gvr, desc := range desiredByGVR {
		m.nextGen++
		gen := m.nextGen
		m.logger.Info("informer started", "gvr", gvr.String(), "scope", desc.Scope, "generation", gen)
		informer := NewInformerGeneration(m.client, gvr, desc.Scope, gen, m.eventCh, m.resyncCh, m.nsFilter, m.logger)
		informerCtx, cancel := context.WithCancel(ctx)
		m.stoppers[gvr] = cancel
		m.generations[gvr] = gen
		m.scopes[gvr] = desc.Scope
		m.startInformer(informer, informerCtx)
		// Serialize startup to avoid memory spikes.
		informer.WaitUntilInitialized(ctx, 10*time.Second)
	}

	m.logger.Info("reconcile complete", "running", len(m.stoppers))
}

// startInformer runs informer.Run in a tracked goroutine so StopAll can
// wait for it after cancellation.
func (m *InformerManager) startInformer(informer *GenericInformer, ctx context.Context) {
	m.informerWG.Add(1)
	go func() {
		defer m.informerWG.Done()
		informer.Run(ctx)
	}()
}

// StopAll cancels all running ordinary informers and waits for every tracked
// informer goroutine (ordinary and CRD) to exit, bounded by ctx.
// It does not emit RemoveGVR events.
func (m *InformerManager) StopAll(ctx context.Context) error {
	for gvr, stopper := range m.stoppers {
		m.logger.Info("stopping informer", "gvr", gvr.String())
		stopper()
		delete(m.stoppers, gvr)
		delete(m.generations, gvr)
	}

	done := make(chan struct{})
	go func() {
		m.informerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
	m.runContinuous(ctx, denyList, allowList, 10*time.Second)
}

// runContinuous is the testable implementation of [RunContinuous].
// The CRD informer is discovery-only: its ResyncEvent.Ack values are
// acknowledged locally so WaitUntilInitialized is not blocked waiting
// for the inventory Writer.
func (m *InformerManager) runContinuous(ctx context.Context, denyList, allowList []Resource, minReconcileInterval time.Duration) {
	// Initial discovery and reconcile.
	m.discoverAndReconcile(ctx, denyList, allowList)

	// Start a CRD informer to detect custom resource changes.
	crdEventCh := make(chan ResourceEvent, 64)
	crdResyncCh := make(chan ResyncEvent, 4)
	// CRD LIST writes are not persisted through the inventory Writer; ack
	// them here and forward a signal so Run can wait for initialization
	// without deadlocking on ResyncEvent.Ack.
	crdResyncSignal := make(chan struct{}, 1)
	m.nextGen++
	crdInformer := NewInformerGeneration(m.client, crdGVR, ObjectScopeCluster, m.nextGen, crdEventCh, crdResyncCh, nil, m.logger)

	crdCtx, crdCancel := context.WithCancel(ctx)
	defer crdCancel()
	go func() {
		for {
			select {
			case <-crdCtx.Done():
				return
			case rs, ok := <-crdResyncCh:
				if !ok {
					return
				}
				if rs.Ack != nil {
					select {
					case rs.Ack <- nil:
					default:
					}
				}
				select {
				case crdResyncSignal <- struct{}{}:
				default:
				}
			}
		}
	}()
	m.startInformer(crdInformer, crdCtx)
	crdInformer.WaitUntilInitialized(ctx, 10*time.Second)

	lastReconcile := time.Now()
	pending := false

	// Timer for throttled reconcile. Create already-fired and drain so later
	// Reset calls are safe without a Stop race on startup.
	reconcileTimer := time.NewTimer(0)
	<-reconcileTimer.C

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

		case <-crdResyncSignal:
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
	if ctx.Err() != nil {
		return
	}

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
