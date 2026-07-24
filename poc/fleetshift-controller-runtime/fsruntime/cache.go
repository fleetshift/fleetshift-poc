package fsruntime

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/store"
)

// fsCache is a controller-runtime cache.Cache backed by stock
// SharedIndexInformer instances. Each informer is driven by a
// store.ListerWatcher; Reflector owns relist, watch restart, and
// indexer population.
type fsCache struct {
	scheme     *runtime.Scheme
	store      *store.Store
	restMapper meta.RESTMapper
	logger     logr.Logger

	mu        sync.Mutex
	informers map[schema.GroupVersionKind]toolscache.SharedIndexInformer
	started   bool
	ctx       context.Context
}

var _ cache.Cache = (*fsCache)(nil)

func (c *fsCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	inf, err := c.getOrCreateInformer(gvk)
	if err != nil {
		return err
	}
	storeKey := types.NamespacedName{Namespace: key.Namespace, Name: key.Name}.String()
	item, exists, err := inf.GetStore().GetByKey(storeKey)
	if err != nil {
		return err
	}
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.Name)
	}
	return copyIntoObject(item.(runtime.Object), obj, gvk)
}

func (c *fsCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}
	listGVK, err := resolveGVK(c.scheme, list)
	if err != nil {
		return err
	}
	itemGVK := itemGVKFromListGVK(listGVK)
	inf, err := c.getOrCreateInformer(itemGVK)
	if err != nil {
		return err
	}

	var raw []interface{}
	if listOpts.Namespace != "" {
		raw, err = inf.GetIndexer().ByIndex(toolscache.NamespaceIndex, listOpts.Namespace)
	} else {
		raw = inf.GetStore().List()
	}
	if err != nil {
		return err
	}

	items := make([]client.Object, 0, len(raw))
	for _, item := range raw {
		obj, ok := item.(client.Object)
		if !ok {
			return fmt.Errorf("fsruntime: cache contained %T, not client.Object", item)
		}
		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}
		items = append(items, obj.DeepCopyObject().(client.Object))
	}
	if err := setListItems(list, items); err != nil {
		return err
	}
	return nil
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
	informers := make([]toolscache.SharedIndexInformer, 0, len(c.informers))
	for _, inf := range c.informers {
		informers = append(informers, inf)
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, inf := range informers {
		wg.Add(1)
		go func(inf toolscache.SharedIndexInformer) {
			defer wg.Done()
			inf.RunWithContext(ctx)
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

func (c *fsCache) getOrCreateInformer(gvk schema.GroupVersionKind) (toolscache.SharedIndexInformer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if inf, ok := c.informers[gvk]; ok {
		return inf, nil
	}

	example, err := c.scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("fsruntime: no type for %s: %w", gvk, err)
	}

	lw := store.NewListerWatcher(c.store, gvk)
	inf := toolscache.NewSharedIndexInformer(
		lw,
		example,
		0,
		toolscache.Indexers{toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc},
	)
	c.informers[gvk] = inf
	if c.started && c.ctx != nil {
		go inf.RunWithContext(c.ctx)
	}
	return inf, nil
}

func resolveGVK(scheme *runtime.Scheme, obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("fsruntime: no GVK for %T", obj)
	}
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

func copyIntoObject(src runtime.Object, dst client.Object, gvk schema.GroupVersionKind) error {
	copied := src.DeepCopyObject()
	outVal := reflect.ValueOf(dst)
	objVal := reflect.ValueOf(copied)
	if !objVal.Type().AssignableTo(outVal.Type()) {
		return fmt.Errorf("fsruntime: cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
	}
	reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
	dst.GetObjectKind().SetGroupVersionKind(gvk)
	return nil
}
