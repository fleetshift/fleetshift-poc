package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	worklister "open-cluster-management.io/api/client/work/listers/work/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"
)

const (
	AnnotationAttestationRef = "fleetshift.io/attestation-ref"
	labelDeliveryID          = "fleetshift.io/delivery-id"
	labelTargetID            = "fleetshift.io/target-id"
	defaultHubHash           = "fleetshift-demo-hub"
	defaultAgentID           = "fleetshift-demo-agent"
)

type DeliveryID string
type TargetID string
type UpdateMode string

const (
	UpdateModeUpdate          UpdateMode = "Update"
	UpdateModeCreateOnly      UpdateMode = "CreateOnly"
	UpdateModeServerSideApply UpdateMode = "ServerSideApply"
	UpdateModeReadOnly        UpdateMode = "ReadOnly"
)

// Manifest is the tiny FleetShift-shaped input the POC translates into
// OCM ManifestWork payloads. It is intentionally smaller than the real
// server-side domain types so the POC can live under the top-level poc/
// tree without importing FleetShift internal packages.
type Manifest struct {
	Raw   json.RawMessage
	Watch bool
}

// DeliveryEnvelope is a small stand-in for the orchestration output that
// would normally be passed to a FleetShift delivery agent.
type DeliveryEnvelope struct {
	DeliveryID     DeliveryID
	TargetID       TargetID
	Manifests      []Manifest
	UpdateMode     UpdateMode
	ForceOwnership bool
	AttestationRef string
	Labels         map[string]string
}

// manifestWorkClient is the narrow compatibility surface this POC needs
// from a hub-side ManifestWork API. The implementation below is purely
// in-process; it is intentionally not backed by a Kubernetes API server.
type manifestWorkClient interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*workapiv1.ManifestWork, error)
	UpdateStatus(ctx context.Context, work *workapiv1.ManifestWork, opts metav1.UpdateOptions) (*workapiv1.ManifestWork, error)
}

// Hub materializes FleetShift-style deliveries into an in-memory desired
// work source. Each target sees that source through a target-scoped
// informer/lister view, mirroring the spoke-side contract OCM expects
// without persisting ManifestWork to any API server.
type Hub struct {
	source *manifestWorkSource

	mu    sync.Mutex
	views map[TargetID]*TargetView
}

func NewHub() *Hub {
	return &Hub{
		source: newManifestWorkSource(),
		views:  make(map[TargetID]*TargetView),
	}
}

func (h *Hub) Deliver(ctx context.Context, in DeliveryEnvelope) (*workapiv1.ManifestWork, error) {
	work, err := ManifestWorkForDelivery(in)
	if err != nil {
		return nil, err
	}
	return h.source.Upsert(ctx, in.TargetID, work)
}

func (h *Hub) View(target TargetID) *TargetView {
	h.mu.Lock()
	defer h.mu.Unlock()

	if view, ok := h.views[target]; ok {
		return view
	}

	view := h.source.View(target)
	h.views[target] = view
	return view
}

type manifestWorkSource struct {
	mu           sync.RWMutex
	revision     int64
	works        map[TargetID]map[string]*workapiv1.ManifestWork
	broadcasters map[TargetID]*watch.Broadcaster
}

func newManifestWorkSource() *manifestWorkSource {
	return &manifestWorkSource{
		works:        make(map[TargetID]map[string]*workapiv1.ManifestWork),
		broadcasters: make(map[TargetID]*watch.Broadcaster),
	}
}

func (s *manifestWorkSource) View(target TargetID) *TargetView {
	informer := NewVirtualManifestWorkInformer(s, target)

	return &TargetView{
		target:   target,
		client:   &virtualManifestWorkClient{source: s, target: target},
		informer: informer,
		lister:   worklister.NewManifestWorkLister(informer.GetIndexer()).ManifestWorks(string(target)),
	}
}

func (s *manifestWorkSource) List(target TargetID) (*workapiv1.ManifestWorkList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]workapiv1.ManifestWork, 0, len(s.works[target]))
	for _, work := range s.works[target] {
		items = append(items, *work.DeepCopy())
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})

	return &workapiv1.ManifestWorkList{Items: items}, nil
}

func (s *manifestWorkSource) Watch(target TargetID) watch.Interface {
	s.mu.Lock()
	defer s.mu.Unlock()

	watcher, err := s.ensureBroadcasterLocked(target).Watch()
	if err != nil {
		panic(fmt.Sprintf("create synthetic watch for %q: %v", target, err))
	}
	return watcher
}

func (s *manifestWorkSource) Upsert(ctx context.Context, target TargetID, work *workapiv1.ManifestWork) (*workapiv1.ManifestWork, error) {
	_ = ctx

	s.mu.Lock()
	targetWorks := s.ensureTargetLocked(target)
	broadcaster := s.ensureBroadcasterLocked(target)
	_, existed := targetWorks[work.Name]

	stored := work.DeepCopy()
	stored.Namespace = string(target)
	s.revision++
	stored.ResourceVersion = fmt.Sprintf("%d", s.revision)
	targetWorks[work.Name] = stored
	s.mu.Unlock()

	eventType := watch.Added
	if existed {
		eventType = watch.Modified
	}
	broadcaster.Action(eventType, stored.DeepCopy())
	return stored.DeepCopy(), nil
}

func (s *manifestWorkSource) Get(target TargetID, name string) (*workapiv1.ManifestWork, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	targetWorks := s.works[target]
	if targetWorks == nil {
		return nil, manifestWorkNotFound(name)
	}
	work, ok := targetWorks[name]
	if !ok {
		return nil, manifestWorkNotFound(name)
	}
	return work.DeepCopy(), nil
}

func (s *manifestWorkSource) UpdateStatus(ctx context.Context, target TargetID, work *workapiv1.ManifestWork) (*workapiv1.ManifestWork, error) {
	_ = ctx

	s.mu.Lock()
	targetWorks := s.works[target]
	if targetWorks == nil {
		s.mu.Unlock()
		return nil, manifestWorkNotFound(work.Name)
	}
	if _, ok := targetWorks[work.Name]; !ok {
		s.mu.Unlock()
		return nil, manifestWorkNotFound(work.Name)
	}
	broadcaster := s.ensureBroadcasterLocked(target)
	stored := work.DeepCopy()
	stored.Namespace = string(target)
	s.revision++
	stored.ResourceVersion = fmt.Sprintf("%d", s.revision)
	targetWorks[work.Name] = stored
	s.mu.Unlock()

	broadcaster.Action(watch.Modified, stored.DeepCopy())
	return stored.DeepCopy(), nil
}

func (s *manifestWorkSource) ensureTargetLocked(target TargetID) map[string]*workapiv1.ManifestWork {
	targetWorks := s.works[target]
	if targetWorks == nil {
		targetWorks = make(map[string]*workapiv1.ManifestWork)
		s.works[target] = targetWorks
	}
	return targetWorks
}

func (s *manifestWorkSource) ensureBroadcasterLocked(target TargetID) *watch.Broadcaster {
	broadcaster := s.broadcasters[target]
	if broadcaster == nil {
		broadcaster = watch.NewBroadcaster(100, watch.DropIfChannelFull)
		s.broadcasters[target] = broadcaster
	}
	return broadcaster
}

type virtualManifestWorkClient struct {
	source *manifestWorkSource
	target TargetID
}

func (c *virtualManifestWorkClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*workapiv1.ManifestWork, error) {
	_ = opts
	return c.source.Get(c.target, name)
}

func (c *virtualManifestWorkClient) UpdateStatus(ctx context.Context, work *workapiv1.ManifestWork, opts metav1.UpdateOptions) (*workapiv1.ManifestWork, error) {
	_ = opts
	return c.source.UpdateStatus(ctx, c.target, work)
}

// VirtualManifestWorkInformer is a tiny in-process informer tailored to the
// compatibility seam this POC needs. It maintains a real indexer and emits
// add/update/delete callbacks, but its source is the synthetic desired-work
// cache rather than a Kubernetes API server.
type VirtualManifestWorkInformer struct {
	source  *manifestWorkSource
	target  TargetID
	indexer cache.Indexer

	handlersMu sync.RWMutex
	handlers   []cache.ResourceEventHandler

	syncedCh  chan struct{}
	syncOnce  sync.Once
	startOnce sync.Once
}

func NewVirtualManifestWorkInformer(source *manifestWorkSource, target TargetID) *VirtualManifestWorkInformer {
	return &VirtualManifestWorkInformer{
		source:   source,
		target:   target,
		indexer:  cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}),
		syncedCh: make(chan struct{}),
	}
}

func (i *VirtualManifestWorkInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	i.handlersMu.Lock()
	defer i.handlersMu.Unlock()

	i.handlers = append(i.handlers, handler)
	return nil, nil
}

func (i *VirtualManifestWorkInformer) GetIndexer() cache.Indexer {
	return i.indexer
}

func (i *VirtualManifestWorkInformer) HasSynced() bool {
	select {
	case <-i.syncedCh:
		return true
	default:
		return false
	}
}

func (i *VirtualManifestWorkInformer) Run(stopCh <-chan struct{}) {
	i.startOnce.Do(func() {
		initial, err := i.source.List(i.target)
		if err != nil {
			panic(fmt.Sprintf("list synthetic manifestwork view for %q: %v", i.target, err))
		}
		for index := range initial.Items {
			work := initial.Items[index].DeepCopy()
			if err := i.indexer.Add(work); err != nil {
				panic(fmt.Sprintf("seed synthetic informer for %q: %v", i.target, err))
			}
			i.dispatchAdd(work, true)
		}
		i.syncOnce.Do(func() { close(i.syncedCh) })

		watcher := i.source.Watch(i.target)
		defer watcher.Stop()

		for {
			select {
			case <-stopCh:
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					return
				}
				work, ok := event.Object.(*workapiv1.ManifestWork)
				if !ok {
					continue
				}
				i.handleEvent(event.Type, work.DeepCopy())
			}
		}
	})
}

func (i *VirtualManifestWorkInformer) handleEvent(eventType watch.EventType, work *workapiv1.ManifestWork) {
	key, err := cache.MetaNamespaceKeyFunc(work)
	if err != nil {
		panic(fmt.Sprintf("build synthetic informer key: %v", err))
	}

	var oldWork *workapiv1.ManifestWork
	if old, exists, err := i.indexer.GetByKey(key); err == nil && exists {
		oldWork = old.(*workapiv1.ManifestWork).DeepCopy()
	}

	switch eventType {
	case watch.Added:
		if err := i.indexer.Add(work); err != nil {
			panic(fmt.Sprintf("add synthetic informer object: %v", err))
		}
		i.dispatchAdd(work, false)
	case watch.Modified:
		if err := i.indexer.Update(work); err != nil {
			panic(fmt.Sprintf("update synthetic informer object: %v", err))
		}
		if oldWork == nil {
			i.dispatchAdd(work, false)
			return
		}
		i.dispatchUpdate(oldWork, work)
	case watch.Deleted:
		if err := i.indexer.Delete(work); err != nil {
			panic(fmt.Sprintf("delete synthetic informer object: %v", err))
		}
		i.dispatchDelete(work)
	}
}

func (i *VirtualManifestWorkInformer) dispatchAdd(work *workapiv1.ManifestWork, isInInitialList bool) {
	i.handlersMu.RLock()
	defer i.handlersMu.RUnlock()

	for _, handler := range i.handlers {
		handler.OnAdd(work.DeepCopy(), isInInitialList)
	}
}

func (i *VirtualManifestWorkInformer) dispatchUpdate(oldWork, newWork *workapiv1.ManifestWork) {
	i.handlersMu.RLock()
	defer i.handlersMu.RUnlock()

	for _, handler := range i.handlers {
		handler.OnUpdate(oldWork.DeepCopy(), newWork.DeepCopy())
	}
}

func (i *VirtualManifestWorkInformer) dispatchDelete(work *workapiv1.ManifestWork) {
	i.handlersMu.RLock()
	defer i.handlersMu.RUnlock()

	for _, handler := range i.handlers {
		handler.OnDelete(work.DeepCopy())
	}
}

// TargetView is the reusable spoke-side seam from OCM: a ManifestWork
// client plus a namespace-scoped informer/lister for one target. In this
// POC the view is backed by an in-memory list/watch source rather than an
// API server.
type TargetView struct {
	target   TargetID
	client   manifestWorkClient
	informer *VirtualManifestWorkInformer
	lister   worklister.ManifestWorkNamespaceLister

	startOnce sync.Once
}

func (v *TargetView) Start(ctx context.Context) {
	v.startOnce.Do(func() {
		go v.informer.Run(ctx.Done())
	})
}

func (v *TargetView) WaitForSync(ctx context.Context) bool {
	v.Start(ctx)
	select {
	case <-ctx.Done():
		return false
	case <-v.informer.syncedCh:
		return true
	}
}

func (v *TargetView) Lister() worklister.ManifestWorkNamespaceLister {
	return v.lister
}

func (v *TargetView) Informer() *VirtualManifestWorkInformer {
	return v.informer
}

func (v *TargetView) Client() manifestWorkClient {
	return v.client
}

func ManifestWorkForDelivery(in DeliveryEnvelope) (*workapiv1.ManifestWork, error) {
	if in.TargetID == "" {
		return nil, errors.New("target id is required")
	}
	if in.DeliveryID == "" {
		return nil, errors.New("delivery id is required")
	}
	if len(in.Manifests) == 0 {
		return nil, errors.New("at least one manifest is required")
	}

	updateStrategy, err := updateStrategyFor(in.UpdateMode, in.ForceOwnership)
	if err != nil {
		return nil, err
	}

	workManifests := make([]workapiv1.Manifest, 0, len(in.Manifests))
	manifestConfigs := make([]workapiv1.ManifestConfigOption, 0, len(in.Manifests))
	for i, manifest := range in.Manifests {
		desc, err := describeManifest(manifest.Raw, i)
		if err != nil {
			return nil, fmt.Errorf("manifest %d: %w", i, err)
		}

		cfg := workapiv1.ManifestConfigOption{
			ResourceIdentifier: desc.identifier,
		}
		if updateStrategy != nil {
			cfg.UpdateStrategy = updateStrategy.DeepCopy()
		}
		if manifest.Watch {
			cfg.FeedbackScrapeType = workapiv1.FeedbackWatchType
		}

		workManifests = append(workManifests, desc.manifest)
		manifestConfigs = append(manifestConfigs, cfg)
	}

	labels := copyStringMap(in.Labels)
	labels[labelDeliveryID] = string(in.DeliveryID)
	labels[labelTargetID] = string(in.TargetID)

	annotations := make(map[string]string)
	if in.AttestationRef != "" {
		annotations[AnnotationAttestationRef] = in.AttestationRef
	}

	return &workapiv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   string(in.TargetID),
			Name:        string(in.DeliveryID),
			Labels:      labels,
			Annotations: annotations,
			Finalizers:  []string{workapiv1.ManifestWorkFinalizer},
		},
		Spec: workapiv1.ManifestWorkSpec{
			Workload: workapiv1.ManifestsTemplate{
				Manifests: workManifests,
			},
			ManifestConfigs: manifestConfigs,
		},
	}, nil
}

type SpokeOptions struct {
	HubHash string
	AgentID string
	Journal *AppliedManifestJournal
}

// DeliveryFeedback is a FleetShift-shaped status projection built from
// ManifestWork and AppliedManifestWork state.
type DeliveryFeedback struct {
	DeliveryID       DeliveryID
	TargetID         TargetID
	Applied          bool
	Available        bool
	AppliedResources []workapiv1.AppliedManifestResourceMeta
}

// SpokeReconciler is a deliberately small target-local loop that mirrors
// OCM's spoke-side flow: watch ManifestWork for one target, materialize an
// AppliedManifestWork locally, and project status back onto the hub work.
type SpokeReconciler struct {
	target TargetID
	view   *TargetView

	journal *AppliedManifestJournal
	queue   workqueue.TypedRateLimitingInterface[string]
	hubHash string
	agentID string

	feedbackMu sync.RWMutex
	feedback   map[DeliveryID]DeliveryFeedback
}

func NewSpokeReconciler(target TargetID, view *TargetView, opts SpokeOptions) *SpokeReconciler {
	journal := opts.Journal
	if journal == nil {
		journal = NewAppliedManifestJournal()
	}

	hubHash := opts.HubHash
	if hubHash == "" {
		hubHash = defaultHubHash
	}
	agentID := opts.AgentID
	if agentID == "" {
		agentID = defaultAgentID
	}

	reconciler := &SpokeReconciler{
		target:   target,
		view:     view,
		journal:  journal,
		queue:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		hubHash:  hubHash,
		agentID:  agentID,
		feedback: make(map[DeliveryID]DeliveryFeedback),
	}

	_, _ = view.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    reconciler.onAdd(),
		UpdateFunc: reconciler.onUpdate(),
	})

	return reconciler
}

func (s *SpokeReconciler) Run(ctx context.Context) error {
	s.view.Start(ctx)
	if !s.view.WaitForSync(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("manifestwork informer failed to sync")
	}

	go func() {
		<-ctx.Done()
		s.queue.ShutDown()
	}()

	for {
		key, shutdown := s.queue.Get()
		if shutdown {
			return ctx.Err()
		}

		err := s.Sync(ctx, key)
		s.queue.Done(key)
		if err != nil {
			s.queue.AddRateLimited(key)
			continue
		}
		s.queue.Forget(key)
	}
}

func (s *SpokeReconciler) Sync(ctx context.Context, manifestWorkName string) error {
	work, err := s.view.Lister().Get(manifestWorkName)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get manifestwork %q from cache: %w", manifestWorkName, err)
	}

	work = work.DeepCopy()
	if !work.DeletionTimestamp.IsZero() {
		return nil
	}
	if !hasFinalizer(work.Finalizers, workapiv1.ManifestWorkFinalizer) {
		return nil
	}
	if apimeta.IsStatusConditionTrue(work.Status.Conditions, workapiv1.WorkComplete) {
		return nil
	}

	resourceStatus, appliedResources, err := statusForWork(work)
	if err != nil {
		return err
	}

	deliveryID := DeliveryID(work.Name)
	s.journal.Upsert(JournalEntry{
		DeliveryID:       deliveryID,
		HubHash:          s.hubHash,
		AgentID:          s.agentID,
		ManifestWorkName: work.Name,
		AppliedResources: appliedResources,
	})

	applied, err := s.journal.Project(deliveryID)
	if err != nil {
		return fmt.Errorf("project appliedmanifestwork for %q: %w", deliveryID, err)
	}

	now := metav1.Now()
	work.Status.ResourceStatus = resourceStatus
	applyCondition := metav1.Condition{
		Type:               workapiv1.WorkApplied,
		Status:             metav1.ConditionTrue,
		Reason:             "POCApplied",
		Message:            "POC spoke reconciler materialized the desired manifests.",
		LastTransitionTime: now,
		ObservedGeneration: work.Generation,
	}
	availableCondition := metav1.Condition{
		Type:               workapiv1.WorkAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             "POCAvailable",
		Message:            "POC spoke reconciler reported the manifests as available.",
		LastTransitionTime: now,
		ObservedGeneration: work.Generation,
	}
	apimeta.SetStatusCondition(&work.Status.Conditions, applyCondition)
	apimeta.SetStatusCondition(&work.Status.Conditions, availableCondition)

	work, err = s.view.Client().UpdateStatus(ctx, work, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update manifestwork %q status: %w", work.Name, err)
	}

	s.feedbackMu.Lock()
	s.feedback[deliveryID] = FeedbackFrom(s.target, work, applied)
	s.feedbackMu.Unlock()

	return nil
}

func (s *SpokeReconciler) AppliedManifestWork(ctx context.Context, deliveryID DeliveryID) (*workapiv1.AppliedManifestWork, error) {
	_ = ctx
	return s.journal.Project(deliveryID)
}

func (s *SpokeReconciler) JournalEntry(deliveryID DeliveryID) (JournalEntry, bool) {
	return s.journal.Entry(deliveryID)
}

// JournalEntry is the minimal local checkpoint this POC keeps for target-
// side cleanup and restart safety. It intentionally stores less than a full
// AppliedManifestWork object and projects the OCM shape only on demand.
type JournalEntry struct {
	DeliveryID       DeliveryID
	HubHash          string
	AgentID          string
	ManifestWorkName string
	AppliedResources []workapiv1.AppliedManifestResourceMeta
}

type AppliedManifestJournal struct {
	mu      sync.RWMutex
	entries map[DeliveryID]JournalEntry
}

func NewAppliedManifestJournal() *AppliedManifestJournal {
	return &AppliedManifestJournal{
		entries: make(map[DeliveryID]JournalEntry),
	}
}

func (j *AppliedManifestJournal) Upsert(entry JournalEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.entries[entry.DeliveryID] = copyJournalEntry(entry)
}

func (j *AppliedManifestJournal) Entry(deliveryID DeliveryID) (JournalEntry, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	entry, ok := j.entries[deliveryID]
	if !ok {
		return JournalEntry{}, false
	}
	return copyJournalEntry(entry), true
}

func (j *AppliedManifestJournal) Project(deliveryID DeliveryID) (*workapiv1.AppliedManifestWork, error) {
	entry, ok := j.Entry(deliveryID)
	if !ok {
		return nil, appliedManifestWorkNotFound(string(deliveryID))
	}

	return &workapiv1.AppliedManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("%s-%s", entry.HubHash, deliveryID),
			Finalizers: []string{workapiv1.AppliedManifestWorkFinalizer},
		},
		Spec: workapiv1.AppliedManifestWorkSpec{
			HubHash:          entry.HubHash,
			AgentID:          entry.AgentID,
			ManifestWorkName: entry.ManifestWorkName,
		},
		Status: workapiv1.AppliedManifestWorkStatus{
			AppliedResources: append([]workapiv1.AppliedManifestResourceMeta(nil), entry.AppliedResources...),
		},
	}, nil
}

func copyJournalEntry(entry JournalEntry) JournalEntry {
	entry.AppliedResources = append([]workapiv1.AppliedManifestResourceMeta(nil), entry.AppliedResources...)
	return entry
}

func (s *SpokeReconciler) Feedback(deliveryID DeliveryID) (DeliveryFeedback, bool) {
	s.feedbackMu.RLock()
	defer s.feedbackMu.RUnlock()

	feedback, ok := s.feedback[deliveryID]
	return feedback, ok
}

func FeedbackFrom(target TargetID, work *workapiv1.ManifestWork, applied *workapiv1.AppliedManifestWork) DeliveryFeedback {
	return DeliveryFeedback{
		DeliveryID:       DeliveryID(work.Name),
		TargetID:         target,
		Applied:          apimeta.IsStatusConditionTrue(work.Status.Conditions, workapiv1.WorkApplied),
		Available:        apimeta.IsStatusConditionTrue(work.Status.Conditions, workapiv1.WorkAvailable),
		AppliedResources: append([]workapiv1.AppliedManifestResourceMeta(nil), applied.Status.AppliedResources...),
	}
}

func (s *SpokeReconciler) onAdd() func(obj interface{}) {
	return func(obj interface{}) {
		accessor, err := apimeta.Accessor(obj)
		if err != nil {
			return
		}
		if hasFinalizer(accessor.GetFinalizers(), workapiv1.ManifestWorkFinalizer) {
			s.queue.Add(accessor.GetName())
		}
	}
}

func (s *SpokeReconciler) onUpdate() func(oldObj, newObj interface{}) {
	return func(oldObj, newObj interface{}) {
		newWork, ok := newObj.(*workapiv1.ManifestWork)
		if !ok {
			return
		}
		oldWork, ok := oldObj.(*workapiv1.ManifestWork)
		if !ok {
			return
		}

		if !hasFinalizer(newWork.GetFinalizers(), workapiv1.ManifestWorkFinalizer) {
			return
		}
		if !hasFinalizer(oldWork.GetFinalizers(), workapiv1.ManifestWorkFinalizer) ||
			!apiequality.Semantic.DeepEqual(newWork.Spec, oldWork.Spec) ||
			!apiequality.Semantic.DeepEqual(newWork.Labels, oldWork.Labels) {
			s.queue.Forget(newWork.GetName())
			s.queue.Add(newWork.GetName())
		}
	}
}

func statusForWork(work *workapiv1.ManifestWork) (workapiv1.ManifestResourceStatus, []workapiv1.AppliedManifestResourceMeta, error) {
	manifestStatuses := make([]workapiv1.ManifestCondition, 0, len(work.Spec.Workload.Manifests))
	appliedResources := make([]workapiv1.AppliedManifestResourceMeta, 0, len(work.Spec.Workload.Manifests))
	now := metav1.Now()

	for i, manifest := range work.Spec.Workload.Manifests {
		desc, err := describeManifest(manifest.Raw, i)
		if err != nil {
			return workapiv1.ManifestResourceStatus{}, nil, fmt.Errorf("describe manifest %d: %w", i, err)
		}

		manifestStatuses = append(manifestStatuses, workapiv1.ManifestCondition{
			ResourceMeta: desc.resourceMeta,
			Conditions: []metav1.Condition{
				{
					Type:               workapiv1.WorkApplied,
					Status:             metav1.ConditionTrue,
					Reason:             "POCApplied",
					Message:            "POC spoke reconciler accepted the desired manifest.",
					LastTransitionTime: now,
					ObservedGeneration: work.Generation,
				},
				{
					Type:               workapiv1.WorkAvailable,
					Status:             metav1.ConditionTrue,
					Reason:             "POCAvailable",
					Message:            "POC spoke reconciler marked the manifest as available.",
					LastTransitionTime: now,
					ObservedGeneration: work.Generation,
				},
			},
		})
		appliedResources = append(appliedResources, desc.appliedMeta)
	}

	return workapiv1.ManifestResourceStatus{Manifests: manifestStatuses}, appliedResources, nil
}

type manifestDescriptor struct {
	manifest     workapiv1.Manifest
	identifier   workapiv1.ResourceIdentifier
	resourceMeta workapiv1.ManifestResourceMeta
	appliedMeta  workapiv1.AppliedManifestResourceMeta
}

func describeManifest(raw []byte, ordinal int) (manifestDescriptor, error) {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return manifestDescriptor{}, fmt.Errorf("parse manifest json: %w", err)
	}

	gvk := obj.GroupVersionKind()
	plural, _ := apimeta.UnsafeGuessKindToResource(gvk)
	identifier := workapiv1.ResourceIdentifier{
		Group:     gvk.Group,
		Resource:  plural.Resource,
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}

	return manifestDescriptor{
		manifest: workapiv1.Manifest{
			RawExtension: runtime.RawExtension{
				Raw: append([]byte(nil), raw...),
			},
		},
		identifier: identifier,
		resourceMeta: workapiv1.ManifestResourceMeta{
			Ordinal:   int32(ordinal),
			Group:     gvk.Group,
			Version:   gvk.Version,
			Kind:      gvk.Kind,
			Resource:  plural.Resource,
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		},
		appliedMeta: workapiv1.AppliedManifestResourceMeta{
			ResourceIdentifier: identifier,
			Version:            gvk.Version,
		},
	}, nil
}

func updateStrategyFor(mode UpdateMode, force bool) (*workapiv1.UpdateStrategy, error) {
	switch mode {
	case "":
		return nil, nil
	case UpdateModeUpdate:
		return &workapiv1.UpdateStrategy{Type: workapiv1.UpdateStrategyTypeUpdate}, nil
	case UpdateModeCreateOnly:
		return &workapiv1.UpdateStrategy{Type: workapiv1.UpdateStrategyTypeCreateOnly}, nil
	case UpdateModeServerSideApply:
		return &workapiv1.UpdateStrategy{
			Type: workapiv1.UpdateStrategyTypeServerSideApply,
			ServerSideApply: &workapiv1.ServerSideApplyConfig{
				Force: force,
			},
		}, nil
	case UpdateModeReadOnly:
		return &workapiv1.UpdateStrategy{Type: workapiv1.UpdateStrategyTypeReadOnly}, nil
	default:
		return nil, fmt.Errorf("unsupported update mode %q", mode)
	}
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return make(map[string]string)
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func hasFinalizer(finalizers []string, want string) bool {
	for _, finalizer := range finalizers {
		if finalizer == want {
			return true
		}
	}
	return false
}

func manifestWorkNotFound(name string) error {
	return apierrors.NewNotFound(
		schema.GroupResource{
			Group:    "work.open-cluster-management.io",
			Resource: "manifestworks",
		},
		name,
	)
}

func appliedManifestWorkNotFound(name string) error {
	return apierrors.NewNotFound(
		schema.GroupResource{
			Group:    "work.open-cluster-management.io",
			Resource: "appliedmanifestworks",
		},
		name,
	)
}
