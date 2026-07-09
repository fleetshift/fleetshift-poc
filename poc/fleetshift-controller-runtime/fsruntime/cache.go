package fsruntime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/store"
)

type fsCache struct {
	scheme     *runtime.Scheme
	store      *store.Store
	restMapper meta.RESTMapper
	logger     logr.Logger

	mu        sync.Mutex
	informers map[schema.GroupVersionKind]*fsInformer
	started   bool
	ctx       context.Context
}

var _ cache.Cache = (*fsCache)(nil)

func (c *fsCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	return c.store.Get(gvk, key.Namespace, key.Name, obj)
}

func (c *fsCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	cl := &fsClient{scheme: c.scheme, store: c.store, restMapper: c.restMapper}
	return cl.List(ctx, list, opts...)
}

func (c *fsCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return nil, err
	}
	return c.getOrCreateInformer(gvk)
}

func (c *fsCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	return c.getOrCreateInformer(gvk)
}

func (c *fsCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	return nil
}

func (c *fsCache) Start(ctx context.Context) error {
	c.mu.Lock()
	c.started = true
	c.ctx = ctx
	informers := make([]*fsInformer, 0, len(c.informers))
	for _, inf := range c.informers {
		informers = append(informers, inf)
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, inf := range informers {
		wg.Add(1)
		go func(inf *fsInformer) {
			defer wg.Done()
			inf.run(ctx)
		}(inf)
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (c *fsCache) WaitForCacheSync(ctx context.Context) bool {
	for {
		c.mu.Lock()
		all := true
		for _, inf := range c.informers {
			if !inf.HasSynced() {
				all = false
				break
			}
		}
		c.mu.Unlock()
		if all {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (c *fsCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return nil
}

func (c *fsCache) getOrCreateInformer(gvk schema.GroupVersionKind) (cache.Informer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if inf, ok := c.informers[gvk]; ok {
		return inf, nil
	}
	inf := &fsInformer{
		gvk:      gvk,
		store:    c.store,
		scheme:   c.scheme,
		logger:   c.logger.WithValues("gvk", gvk.String()),
		storeMap: make(map[types.NamespacedName]client.Object),
	}
	c.informers[gvk] = inf
	if c.started && c.ctx != nil {
		go inf.run(c.ctx)
	}
	return inf, nil
}

type handlerEntry struct {
	handler toolscache.ResourceEventHandler
	reg     *handlerReg
}

type fsInformer struct {
	gvk    schema.GroupVersionKind
	store  *store.Store
	scheme *runtime.Scheme
	logger logr.Logger

	synced  atomic.Bool
	stopped atomic.Bool

	mu       sync.Mutex
	handlers []handlerEntry
	storeMap map[types.NamespacedName]client.Object
}

var _ cache.Informer = (*fsInformer)(nil)

func (i *fsInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return i.AddEventHandlerWithResyncPeriod(handler, 0)
}

func (i *fsInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, _ time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	reg := &handlerReg{}
	i.mu.Lock()
	i.handlers = append(i.handlers, handlerEntry{handler: handler, reg: reg})
	alreadySynced := i.synced.Load()
	var snapshot []client.Object
	if alreadySynced {
		for _, obj := range i.storeMap {
			snapshot = append(snapshot, obj)
		}
	}
	i.mu.Unlock()

	if alreadySynced {
		for _, obj := range snapshot {
			handler.OnAdd(obj, true)
		}
		reg.synced.Store(true)
	}
	return reg, nil
}

func (i *fsInformer) AddEventHandlerWithOptions(handler toolscache.ResourceEventHandler, _ toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	return i.AddEventHandler(handler)
}

func (i *fsInformer) RemoveEventHandler(handle toolscache.ResourceEventHandlerRegistration) error {
	return nil
}

func (i *fsInformer) AddIndexers(indexers toolscache.Indexers) error { return nil }

func (i *fsInformer) HasSynced() bool { return i.synced.Load() }

func (i *fsInformer) HasSyncedChecker() toolscache.DoneChecker {
	return &doneChecker{name: "fsInformer:" + i.gvk.String(), flag: &i.synced}
}

func (i *fsInformer) IsStopped() bool { return i.stopped.Load() }

func (i *fsInformer) run(ctx context.Context) {
	defer i.stopped.Store(true)

	items, _, err := i.store.List(i.gvk, "")
	if err != nil {
		i.logger.Error(err, "initial list failed")
		return
	}

	i.mu.Lock()
	for _, obj := range items {
		nn := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
		i.storeMap[nn] = obj
		for _, h := range i.handlers {
			h.handler.OnAdd(obj, true)
		}
	}
	for _, h := range i.handlers {
		h.reg.synced.Store(true)
	}
	i.mu.Unlock()
	i.synced.Store(true)

	ch, cancel := i.store.Watch(i.gvk)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			i.handleEvent(ev)
		}
	}
}

func (i *fsInformer) handleEvent(ev store.Event) {
	i.mu.Lock()
	handlers := append([]handlerEntry(nil), i.handlers...)
	nn := types.NamespacedName{Namespace: ev.Object.GetNamespace(), Name: ev.Object.GetName()}
	old := i.storeMap[nn]
	switch ev.Type {
	case watch.Added, watch.Modified:
		i.storeMap[nn] = ev.Object
	case watch.Deleted:
		delete(i.storeMap, nn)
	}
	i.mu.Unlock()

	for _, h := range handlers {
		switch ev.Type {
		case watch.Added:
			h.handler.OnAdd(ev.Object, false)
		case watch.Modified:
			if old == nil {
				h.handler.OnAdd(ev.Object, false)
			} else {
				h.handler.OnUpdate(old, ev.Object)
			}
		case watch.Deleted:
			h.handler.OnDelete(ev.Object)
		}
	}
}

type handlerReg struct {
	synced atomic.Bool
}

var _ toolscache.ResourceEventHandlerRegistration = (*handlerReg)(nil)

func (r *handlerReg) HasSynced() bool { return r.synced.Load() }

func (r *handlerReg) HasSyncedChecker() toolscache.DoneChecker {
	return &doneChecker{name: "fsHandlerRegistration", flag: &r.synced}
}

type doneChecker struct {
	name string
	flag *atomic.Bool
}

func (d *doneChecker) Name() string { return d.name }

func (d *doneChecker) Done() <-chan struct{} {
	ch := make(chan struct{})
	if d.flag.Load() {
		close(ch)
	}
	return ch
}

func resolveGVK(scheme *runtime.Scheme, obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("fsruntime: no GVK for %T", obj)
	}
	return gvks[0], nil
}
