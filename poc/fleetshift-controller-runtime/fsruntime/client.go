package fsruntime

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

	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/store"
)

type fsClient struct {
	scheme     *runtime.Scheme
	store      *store.Store
	restMapper meta.RESTMapper
}

var _ client.Client = (*fsClient)(nil)

func (c *fsClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := c.resolveGVK(obj)
	if err != nil {
		return err
	}
	return c.store.Get(gvk, key.Namespace, key.Name, obj)
}

func (c *fsClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
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

func (c *fsClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return c.store.Create(obj)
}

func (c *fsClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.store.Delete(obj)
}

func (c *fsClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.store.Update(obj)
}

func (c *fsClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "apply")
}

func (c *fsClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "patch")
}

func (c *fsClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "deletecollection")
}

func (c *fsClient) Status() client.SubResourceWriter {
	return &fsStatusWriter{client: c}
}

func (c *fsClient) SubResource(subResource string) client.SubResourceClient {
	if subResource == "status" {
		return &fsStatusWriter{client: c}
	}
	return &unsupportedSubResource{name: subResource}
}

func (c *fsClient) Scheme() *runtime.Scheme     { return c.scheme }
func (c *fsClient) RESTMapper() meta.RESTMapper { return c.restMapper }
func (c *fsClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.resolveGVK(obj)
}
func (c *fsClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return true, nil
}

func (c *fsClient) resolveGVK(obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("fsruntime: no GVK for %T", obj)
	}
	for _, gvk := range gvks {
		if gvk.Kind != "" {
			return gvk, nil
		}
	}
	return gvks[0], nil
}

type fsStatusWriter struct {
	client *fsClient
}

func (w *fsStatusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status create")
}

func (w *fsStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	// Status updates go through the same store Update path. Spec/status
	// split is a production concern (see pgruntime WriteStatus); the POC
	// treats the whole object as one document.
	return w.client.store.Update(obj)
}

func (w *fsStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status patch")
}

func (w *fsStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "status apply")
}

func (w *fsStatusWriter) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
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
