package store

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// listerWatcher adapts Store ListMeta/WatchMeta to cache.ListerWatcher so a
// stock Reflector can drive informer cache population.
type listerWatcher struct {
	store *Store
	gvk   schema.GroupVersionKind
}

// NewListerWatcher returns a cache.ListerWatcher for the given GVK.
func NewListerWatcher(s *Store, gvk schema.GroupVersionKind) cache.ListerWatcher {
	return &listerWatcher{store: s, gvk: gvk}
}

func (lw *listerWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return lw.store.ListMeta(lw.gvk, options)
}

func (lw *listerWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return lw.store.WatchMeta(lw.gvk, options)
}

// IsWatchListSemanticsUnSupported reports that this ListerWatcher only
// implements classic List+Watch. Stock Reflector then skips streaming
// WatchList (bookmark) mode, which our in-memory store does not provide.
func (lw *listerWatcher) IsWatchListSemanticsUnSupported() bool { return true }
