package rmruntime

import (
	"context"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stolostron/ocm-fleet/poc/resource-manager-controller-runtime/store"
)

// StatusHook runs on Status().Update before the store write. If it
// returns an error, the status is not persisted and Status().Update
// fails — controllers requeue, matching kube status write failure.
type StatusHook func(ctx context.Context, obj client.Object) error

type rmClient struct {
	scheme     *runtime.Scheme
	store      *store.Store
	restMapper meta.RESTMapper
	statusHook StatusHook
}

var _ client.Client = (*rmClient)(nil)

func (c *rmClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := c.resolveGVK(obj)
	if err != nil {
		return err
	}
	return c.store.Get(gvk, key.Namespace, key.Name, obj)
}

func (c *rmClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}
	listGVK, err := c.resolveGVK(list)
	if err != nil {
		return err
	}
	itemGVK := itemGVKFromListGVK(listGVK)
	items, rv, err := c.store.List(itemGVK, listOpts.Namespace)
	if err != nil {
		return err
	}
	filtered := items[:0]
	for _, obj := range items {
		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labelsOf(obj)) {
			continue
		}
		filtered = append(filtered, obj)
	}
	if err := setListItems(list, filtered); err != nil {
		return err
	}
	list.SetResourceVersion(rv)
	return nil
}

func (c *rmClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return c.store.Create(obj)
}

func (c *rmClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.store.Delete(obj)
}

func (c *rmClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.store.Update(obj)
}

func (c *rmClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "apply")
}

func (c *rmClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "patch")
}

func (c *rmClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "deletecollection")
}

func (c *rmClient) Status() client.SubResourceWriter {
	return &rmStatusWriter{client: c}
}

func (c *rmClient) SubResource(subResource string) client.SubResourceClient {
	if subResource == "status" {
		return &rmStatusWriter{client: c}
	}
	return &unsupportedSubResource{name: subResource}
}

func (c *rmClient) Scheme() *runtime.Scheme     { return c.scheme }
func (c *rmClient) RESTMapper() meta.RESTMapper { return c.restMapper }
func (c *rmClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.resolveGVK(obj)
}
func (c *rmClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return true, nil
}

func (c *rmClient) resolveGVK(obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("rmruntime: no GVK for %T", obj)
	}
	for _, gvk := range gvks {
		if gvk.Kind != "" {
			return gvk, nil
		}
	}
	return gvks[0], nil
}

type rmStatusWriter struct {
	client *rmClient
}

func (w *rmStatusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status create")
}

func (w *rmStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	// Hook runs before the store write so a failed report fails the status
	// update (controller requeues) and nothing is persisted. This matches
	// kube Status().Update failure semantics from the reconciler's POV,
	// with the platform as the durable sink for delivery outcomes.
	if w.client.statusHook != nil {
		if err := w.client.statusHook(ctx, obj); err != nil {
			return err
		}
	}
	// Spec/status split is a production concern (see pgruntime WriteStatus);
	// the POC treats the whole object as one document.
	return w.client.store.Update(obj)
}

func (w *rmStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status patch")
}

func (w *rmStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status apply")
}

func (w *rmStatusWriter) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status get")
}

type unsupportedSubResource struct{ name string }

func (u *unsupportedSubResource) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, u.name)
}
func (u *unsupportedSubResource) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, u.name)
}
func (u *unsupportedSubResource) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, u.name)
}
func (u *unsupportedSubResource) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, u.name)
}
func (u *unsupportedSubResource) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, u.name)
}

func itemGVKFromListGVK(listGVK schema.GroupVersionKind) schema.GroupVersionKind {
	kind := listGVK.Kind
	if len(kind) > 4 && kind[len(kind)-4:] == "List" {
		kind = kind[:len(kind)-4]
	}
	return schema.GroupVersionKind{Group: listGVK.Group, Version: listGVK.Version, Kind: kind}
}

func setListItems(list client.ObjectList, items []client.Object) error {
	listPtr := reflect.ValueOf(list)
	if listPtr.Kind() != reflect.Ptr {
		return fmt.Errorf("list must be a pointer")
	}
	listVal := listPtr.Elem()
	itemsField := listVal.FieldByName("Items")
	if !itemsField.IsValid() {
		return fmt.Errorf("list type %T has no Items field", list)
	}
	slice := reflect.MakeSlice(itemsField.Type(), 0, len(items))
	for _, obj := range items {
		slice = reflect.Append(slice, reflect.ValueOf(obj).Elem())
	}
	itemsField.Set(slice)
	return nil
}

func labelsOf(obj client.Object) labels.Set {
	return labels.Set(obj.GetLabels())
}
