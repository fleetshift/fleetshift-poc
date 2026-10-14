// Package store is an in-memory Kubernetes-shaped object store with
// list/watch semantics. It is the POC analogue of postgres-controller-backend's
// internal reader/writer: controllers talk to it through controller-runtime
// Client/Cache interfaces, not through kube-apiserver.
//
// Watch delivery uses a resourceVersion journal and watch.Broadcaster so a
// stock Reflector can consume List/Watch via ListerWatcher without a custom
// informer loop. Compaction of the journal surfaces as 410 Gone /
// ResourceExpired, which triggers Reflector relist.
package store

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type key struct {
	Namespace string
	Name      string
}

// Event is a watch event carrying a deep-copied object.
type Event struct {
	Type   watch.EventType
	Object client.Object
}

type journalEntry struct {
	seq int64
	ev  watch.Event
}

type gvkWatch struct {
	journal          []journalEntry
	compactedThrough int64
	broadcaster      *watch.Broadcaster
}

// Store is a thread-safe, GVK-partitioned object store.
type Store struct {
	scheme *runtime.Scheme

	mu      sync.RWMutex
	objects map[schema.GroupVersionKind]map[key]client.Object
	seq     atomic.Int64

	watchMu sync.Mutex
	watches map[schema.GroupVersionKind]*gvkWatch
}

// New returns an empty Store.
func New(scheme *runtime.Scheme) *Store {
	return &Store{
		scheme:  scheme,
		objects: make(map[schema.GroupVersionKind]map[key]client.Object),
		watches: make(map[schema.GroupVersionKind]*gvkWatch),
	}
}

// Scheme returns the scheme used to resolve GVKs.
func (s *Store) Scheme() *runtime.Scheme { return s.scheme }

func (s *Store) gvkOf(obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("store: no GVK for %T", obj)
	}
	// Prefer a non-List GVK if ObjectKinds returns multiple.
	for _, gvk := range gvks {
		if gvk.Kind != "" && !isListKind(gvk.Kind) {
			return gvk, nil
		}
	}
	return gvks[0], nil
}

func isListKind(kind string) bool {
	return len(kind) >= 4 && kind[len(kind)-4:] == "List"
}

func objectKey(obj client.Object) key {
	return key{Namespace: obj.GetNamespace(), Name: obj.GetName()}
}

func notFound(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, name)
}

func alreadyExists(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewAlreadyExists(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, name)
}

func conflict(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, name, fmt.Errorf("resourceVersion mismatch"))
}

func (s *Store) ensureWatch(gvk schema.GroupVersionKind) *gvkWatch {
	gw, ok := s.watches[gvk]
	if !ok {
		gw = &gvkWatch{
			broadcaster: watch.NewBroadcaster(100, watch.WaitIfChannelFull),
		}
		s.watches[gvk] = gw
	}
	return gw
}

// Get copies the named object into out.
func (s *Store) Get(gvk schema.GroupVersionKind, ns, name string, out client.Object) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byKey, ok := s.objects[gvk]
	if !ok {
		return notFound(gvk, name)
	}
	obj, ok := byKey[key{Namespace: ns, Name: name}]
	if !ok {
		return notFound(gvk, name)
	}
	return copyInto(obj, out)
}

// List returns deep copies of all objects of the given GVK, optionally
// filtered by namespace. The returned resourceVersion is the store's
// current sequence number.
func (s *Store) List(gvk schema.GroupVersionKind, namespace string) ([]client.Object, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byKey := s.objects[gvk]
	out := make([]client.Object, 0, len(byKey))
	for _, obj := range byKey {
		if namespace != "" && obj.GetNamespace() != namespace {
			continue
		}
		out = append(out, obj.DeepCopyObject().(client.Object))
	}
	return out, strconv.FormatInt(s.seq.Load(), 10), nil
}

// ListMeta returns a scheme List object for gvk with resourceVersion set,
// suitable for cache.ListerWatcher / Reflector.
func (s *Store) ListMeta(gvk schema.GroupVersionKind, options metav1.ListOptions) (runtime.Object, error) {
	items, rv, err := s.List(gvk, "")
	if err != nil {
		return nil, err
	}
	if options.LabelSelector != "" {
		sel, err := labels.Parse(options.LabelSelector)
		if err != nil {
			return nil, err
		}
		filtered := items[:0]
		for _, obj := range items {
			if sel.Matches(labels.Set(obj.GetLabels())) {
				filtered = append(filtered, obj)
			}
		}
		items = filtered
	}

	listGVK := gvk.GroupVersion().WithKind(gvk.Kind + "List")
	listObj, err := s.scheme.New(listGVK)
	if err != nil {
		return nil, fmt.Errorf("store: no List type for %s: %w", listGVK, err)
	}
	runtimeItems := make([]runtime.Object, len(items))
	for i, obj := range items {
		runtimeItems[i] = obj
	}
	if err := meta.SetList(listObj, runtimeItems); err != nil {
		return nil, err
	}
	accessor, err := meta.ListAccessor(listObj)
	if err != nil {
		return nil, err
	}
	accessor.SetResourceVersion(rv)
	return listObj, nil
}

// Create inserts a new object. Fails if it already exists.
func (s *Store) Create(obj client.Object) error {
	gvk, err := s.gvkOf(obj)
	if err != nil {
		return err
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	s.mu.Lock()
	byKey := s.ensure(gvk)
	k := objectKey(obj)
	if _, exists := byKey[k]; exists {
		s.mu.Unlock()
		return alreadyExists(gvk, obj.GetName())
	}

	now := metav1.Now()
	if obj.GetCreationTimestamp().Time.IsZero() {
		obj.SetCreationTimestamp(now)
	}
	seq := s.seq.Add(1)
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(fmt.Sprintf("%s-%d", obj.GetName(), seq)))
	}
	obj.SetResourceVersion(strconv.FormatInt(seq, 10))
	if obj.GetGeneration() == 0 {
		obj.SetGeneration(1)
	}

	stored := obj.DeepCopyObject().(client.Object)
	byKey[k] = stored
	ev := watch.Event{Type: watch.Added, Object: stored.DeepCopyObject()}
	s.recordLocked(gvk, seq, ev)
	s.mu.Unlock()

	return s.broadcast(gvk, ev)
}

// Update replaces an existing object. Enforces optimistic concurrency on
// resourceVersion when the incoming object carries one.
func (s *Store) Update(obj client.Object) error {
	gvk, err := s.gvkOf(obj)
	if err != nil {
		return err
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	s.mu.Lock()
	byKey := s.ensure(gvk)
	k := objectKey(obj)
	existing, ok := byKey[k]
	if !ok {
		s.mu.Unlock()
		return notFound(gvk, obj.GetName())
	}
	if rv := obj.GetResourceVersion(); rv != "" && rv != existing.GetResourceVersion() {
		s.mu.Unlock()
		return conflict(gvk, obj.GetName())
	}

	seq := s.seq.Add(1)
	obj.SetUID(existing.GetUID())
	obj.SetCreationTimestamp(existing.GetCreationTimestamp())
	obj.SetResourceVersion(strconv.FormatInt(seq, 10))
	if obj.GetGeneration() == 0 {
		obj.SetGeneration(existing.GetGeneration())
	}

	stored := obj.DeepCopyObject().(client.Object)
	byKey[k] = stored
	ev := watch.Event{Type: watch.Modified, Object: stored.DeepCopyObject()}
	s.recordLocked(gvk, seq, ev)
	s.mu.Unlock()

	return s.broadcast(gvk, ev)
}

// Delete removes an object. If finalizers remain, sets deletionTimestamp
// and emits a Modified event instead of deleting.
func (s *Store) Delete(obj client.Object) error {
	gvk, err := s.gvkOf(obj)
	if err != nil {
		return err
	}

	s.mu.Lock()
	byKey, ok := s.objects[gvk]
	if !ok {
		s.mu.Unlock()
		return notFound(gvk, obj.GetName())
	}
	k := objectKey(obj)
	existing, ok := byKey[k]
	if !ok {
		s.mu.Unlock()
		return notFound(gvk, obj.GetName())
	}

	if len(existing.GetFinalizers()) > 0 {
		if existing.GetDeletionTimestamp() == nil {
			now := metav1.Now()
			seq := s.seq.Add(1)
			updated := existing.DeepCopyObject().(client.Object)
			updated.SetDeletionTimestamp(&now)
			updated.SetResourceVersion(strconv.FormatInt(seq, 10))
			byKey[k] = updated
			ev := watch.Event{Type: watch.Modified, Object: updated.DeepCopyObject()}
			s.recordLocked(gvk, seq, ev)
			s.mu.Unlock()
			return s.broadcast(gvk, ev)
		}
		s.mu.Unlock()
		return nil
	}

	seq := s.seq.Add(1)
	delete(byKey, k)
	deleted := existing.DeepCopyObject().(client.Object)
	deleted.SetResourceVersion(strconv.FormatInt(seq, 10))
	ev := watch.Event{Type: watch.Deleted, Object: deleted}
	s.recordLocked(gvk, seq, ev)
	s.mu.Unlock()

	return s.broadcast(gvk, ev)
}

// WatchMeta starts a watch after options.ResourceVersion. If that version has
// been compacted out of the journal, it returns ResourceExpired (410).
func (s *Store) WatchMeta(gvk schema.GroupVersionKind, options metav1.ListOptions) (watch.Interface, error) {
	rvStr := options.ResourceVersion
	if rvStr == "" {
		rvStr = "0"
	}
	rv, err := strconv.ParseInt(rvStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("store: invalid resourceVersion %q: %w", options.ResourceVersion, err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	s.watchMu.Lock()
	gw := s.ensureWatch(gvk)
	s.watchMu.Unlock()

	current := s.seq.Load()
	if rv > current {
		return nil, fmt.Errorf("store: resourceVersion %d is in the future (current %d)", rv, current)
	}
	if rv < gw.compactedThrough {
		return nil, apierrors.NewResourceExpired(fmt.Sprintf(
			"too old resource version: %d (compacted through %d)", rv, gw.compactedThrough))
	}

	// Empty journal: only a watch from the current RV is continuous.
	if len(gw.journal) == 0 {
		if rv < current {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %d (current %d)", rv, current))
		}
		return gw.broadcaster.Watch()
	}

	oldest := gw.journal[0].seq
	if rv < oldest {
		return nil, apierrors.NewResourceExpired(fmt.Sprintf(
			"too old resource version: %d (oldest %d)", rv, oldest))
	}

	var prefix []watch.Event
	for _, entry := range gw.journal {
		if entry.seq > rv {
			prefix = append(prefix, watch.Event{
				Type:   entry.ev.Type,
				Object: entry.ev.Object.DeepCopyObject(),
			})
		}
	}
	if len(prefix) > 0 {
		return gw.broadcaster.WatchWithPrefix(prefix)
	}
	return gw.broadcaster.Watch()
}

// Compact drops journal entries with seq <= throughRV (parsed as int).
// Subsequent WatchMeta calls with a resourceVersion at or before that
// horizon return ResourceExpired.
func (s *Store) Compact(gvk schema.GroupVersionKind, throughRV string) {
	through, err := strconv.ParseInt(throughRV, 10, 64)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchMu.Lock()
	defer s.watchMu.Unlock()

	gw := s.ensureWatch(gvk)
	if through > gw.compactedThrough {
		gw.compactedThrough = through
	}
	kept := gw.journal[:0]
	for _, entry := range gw.journal {
		if entry.seq > through {
			kept = append(kept, entry)
		}
	}
	gw.journal = kept
}

// Watch returns a channel of events for the given GVK from the current
// resourceVersion. Call cancel to unsubscribe. Prefer WatchMeta for
// Reflector integration.
func (s *Store) Watch(gvk schema.GroupVersionKind) (<-chan Event, func()) {
	rv := strconv.FormatInt(s.seq.Load(), 10)
	w, err := s.WatchMeta(gvk, metav1.ListOptions{ResourceVersion: rv})
	if err != nil {
		ch := make(chan Event)
		close(ch)
		return ch, func() {}
	}
	out := make(chan Event, 64)
	done := make(chan struct{})
	go func() {
		defer close(out)
		for {
			select {
			case <-done:
				w.Stop()
				return
			case ev, ok := <-w.ResultChan():
				if !ok {
					return
				}
				obj, _ := ev.Object.(client.Object)
				select {
				case out <- Event{Type: ev.Type, Object: obj}:
				case <-done:
					w.Stop()
					return
				}
			}
		}
	}()
	cancel := func() {
		select {
		case <-done:
		default:
			close(done)
		}
		w.Stop()
	}
	return out, cancel
}

func (s *Store) ensure(gvk schema.GroupVersionKind) map[key]client.Object {
	byKey, ok := s.objects[gvk]
	if !ok {
		byKey = make(map[key]client.Object)
		s.objects[gvk] = byKey
	}
	return byKey
}

// recordLocked appends to the journal. Caller must hold s.mu.
func (s *Store) recordLocked(gvk schema.GroupVersionKind, seq int64, ev watch.Event) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	gw := s.ensureWatch(gvk)
	gw.journal = append(gw.journal, journalEntry{seq: seq, ev: ev})
}

func (s *Store) broadcast(gvk schema.GroupVersionKind, ev watch.Event) error {
	s.watchMu.Lock()
	gw := s.ensureWatch(gvk)
	b := gw.broadcaster
	s.watchMu.Unlock()
	return b.Action(ev.Type, ev.Object)
}

func copyInto(src, dst client.Object) error {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(src.DeepCopyObject())
	if err != nil {
		return err
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u, dst)
}
