// Package store is an in-memory Kubernetes-shaped object store with
// list/watch semantics. It is the POC analogue of postgres-controller-backend's
// internal reader/writer: controllers talk to it through controller-runtime
// Client/Cache interfaces, not through kube-apiserver.
package store

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// Store is a thread-safe, GVK-partitioned object store.
type Store struct {
	scheme *runtime.Scheme

	mu      sync.RWMutex
	objects map[schema.GroupVersionKind]map[key]client.Object
	seq     atomic.Int64

	watchMu  sync.Mutex
	watchers map[schema.GroupVersionKind]map[uint64]chan Event
	nextWID  uint64
}

// New returns an empty Store.
func New(scheme *runtime.Scheme) *Store {
	return &Store{
		scheme:   scheme,
		objects:  make(map[schema.GroupVersionKind]map[key]client.Object),
		watchers: make(map[schema.GroupVersionKind]map[uint64]chan Event),
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

// Create inserts a new object. Fails if it already exists.
func (s *Store) Create(obj client.Object) error {
	gvk, err := s.gvkOf(obj)
	if err != nil {
		return err
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.ensure(gvk)
	k := objectKey(obj)
	if _, exists := byKey[k]; exists {
		return alreadyExists(gvk, obj.GetName())
	}

	now := metav1.Now()
	if obj.GetCreationTimestamp().Time.IsZero() {
		obj.SetCreationTimestamp(now)
	}
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(fmt.Sprintf("%s-%d", obj.GetName(), s.seq.Load()+1)))
	}
	obj.SetResourceVersion(strconv.FormatInt(s.seq.Add(1), 10))
	if obj.GetGeneration() == 0 {
		obj.SetGeneration(1)
	}

	stored := obj.DeepCopyObject().(client.Object)
	byKey[k] = stored
	s.broadcast(gvk, Event{Type: watch.Added, Object: stored.DeepCopyObject().(client.Object)})
	return nil
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
	defer s.mu.Unlock()

	byKey := s.ensure(gvk)
	k := objectKey(obj)
	existing, ok := byKey[k]
	if !ok {
		return notFound(gvk, obj.GetName())
	}
	if rv := obj.GetResourceVersion(); rv != "" && rv != existing.GetResourceVersion() {
		return conflict(gvk, obj.GetName())
	}

	obj.SetUID(existing.GetUID())
	obj.SetCreationTimestamp(existing.GetCreationTimestamp())
	obj.SetResourceVersion(strconv.FormatInt(s.seq.Add(1), 10))
	if obj.GetGeneration() == 0 {
		obj.SetGeneration(existing.GetGeneration())
	}

	stored := obj.DeepCopyObject().(client.Object)
	byKey[k] = stored
	s.broadcast(gvk, Event{Type: watch.Modified, Object: stored.DeepCopyObject().(client.Object)})
	return nil
}

// Delete removes an object. If finalizers remain, sets deletionTimestamp
// and emits a Modified event instead of deleting.
func (s *Store) Delete(obj client.Object) error {
	gvk, err := s.gvkOf(obj)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	byKey, ok := s.objects[gvk]
	if !ok {
		return notFound(gvk, obj.GetName())
	}
	k := objectKey(obj)
	existing, ok := byKey[k]
	if !ok {
		return notFound(gvk, obj.GetName())
	}

	if len(existing.GetFinalizers()) > 0 {
		if existing.GetDeletionTimestamp() == nil {
			now := metav1.Now()
			updated := existing.DeepCopyObject().(client.Object)
			updated.SetDeletionTimestamp(&now)
			updated.SetResourceVersion(strconv.FormatInt(s.seq.Add(1), 10))
			byKey[k] = updated
			s.broadcast(gvk, Event{Type: watch.Modified, Object: updated.DeepCopyObject().(client.Object)})
		}
		return nil
	}

	delete(byKey, k)
	s.seq.Add(1)
	s.broadcast(gvk, Event{Type: watch.Deleted, Object: existing.DeepCopyObject().(client.Object)})
	return nil
}

// Watch returns a channel of events for the given GVK. Call cancel to
// unsubscribe.
func (s *Store) Watch(gvk schema.GroupVersionKind) (<-chan Event, func()) {
	ch := make(chan Event, 64)
	s.watchMu.Lock()
	s.nextWID++
	id := s.nextWID
	if s.watchers[gvk] == nil {
		s.watchers[gvk] = make(map[uint64]chan Event)
	}
	s.watchers[gvk][id] = ch
	s.watchMu.Unlock()

	cancel := func() {
		s.watchMu.Lock()
		if m := s.watchers[gvk]; m != nil {
			if c, ok := m[id]; ok {
				delete(m, id)
				close(c)
			}
		}
		s.watchMu.Unlock()
	}
	return ch, cancel
}

func (s *Store) ensure(gvk schema.GroupVersionKind) map[key]client.Object {
	byKey, ok := s.objects[gvk]
	if !ok {
		byKey = make(map[key]client.Object)
		s.objects[gvk] = byKey
	}
	return byKey
}

// broadcast must be called while holding s.mu so event order matches writes.
func (s *Store) broadcast(gvk schema.GroupVersionKind, ev Event) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, ch := range s.watchers[gvk] {
		select {
		case ch <- ev:
		default:
			// Drop if slow; POC buffers are sized for tests.
		}
	}
}

func copyInto(src, dst client.Object) error {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(src.DeepCopyObject())
	if err != nil {
		return err
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u, dst)
}
