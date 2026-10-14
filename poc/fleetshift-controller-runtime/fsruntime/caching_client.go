package fsruntime

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cachingClient reads through the Reflector-populated cache and writes
// through to the underlying store — the same split as controller-runtime's
// default client against kube-apiserver.
type cachingClient struct {
	cache  client.Reader
	writer *fsClient
}

var _ client.Client = (*cachingClient)(nil)

func (c *cachingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.cache.Get(ctx, key, obj, opts...)
}

func (c *cachingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.cache.List(ctx, list, opts...)
}

func (c *cachingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return c.writer.Create(ctx, obj, opts...)
}

func (c *cachingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.writer.Delete(ctx, obj, opts...)
}

func (c *cachingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.writer.Update(ctx, obj, opts...)
}

func (c *cachingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return c.writer.Patch(ctx, obj, patch, opts...)
}

func (c *cachingClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return c.writer.Apply(ctx, obj, opts...)
}

func (c *cachingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return c.writer.DeleteAllOf(ctx, obj, opts...)
}

func (c *cachingClient) Status() client.SubResourceWriter {
	return c.writer.Status()
}

func (c *cachingClient) SubResource(subResource string) client.SubResourceClient {
	return c.writer.SubResource(subResource)
}

func (c *cachingClient) Scheme() *runtime.Scheme     { return c.writer.Scheme() }
func (c *cachingClient) RESTMapper() meta.RESTMapper { return c.writer.RESTMapper() }
func (c *cachingClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.writer.GroupVersionKindFor(obj)
}
func (c *cachingClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return c.writer.IsObjectNamespaced(obj)
}
