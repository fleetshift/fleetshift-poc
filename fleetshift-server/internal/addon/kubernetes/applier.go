package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const fieldManager = "fleetshift"

// applier wraps a dynamic client and REST mapper for SSA.
type applier struct {
	client dynamic.Interface
	mapper meta.RESTMapper
}

// newApplierFromConfig builds an applier from a REST config by creating
// a discovery client, dynamic client, and deferred REST mapper.
func newApplierFromConfig(cfg *rest.Config) (*applier, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))

	return &applier{client: dyn, mapper: mapper}, nil
}

// apply performs a server-side apply of the given raw JSON manifest.
// Namespaced resources default to the "default" namespace when unset.
func (a *applier) apply(ctx context.Context, raw json.RawMessage) error {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	gvk := obj.GroupVersionKind()
	mapping, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("resolve GVR for %s: %w", gvk, err)
	}

	var dr dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		dr = a.client.Resource(mapping.Resource).Namespace(ns)
	} else {
		dr = a.client.Resource(mapping.Resource)
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal object: %w", err)
	}

	_, err = dr.Patch(ctx, obj.GetName(), "application/apply-patch+yaml", data, metav1.PatchOptions{
		FieldManager: fieldManager,
	})
	if err != nil {
		return fmt.Errorf("apply %s %s/%s: %w", gvk.Kind, obj.GetNamespace(), obj.GetName(), err)
	}

	return nil
}

// delete removes the Kubernetes resource described by the raw JSON manifest.
// Returns nil if the resource is already gone (404).
func (a *applier) delete(ctx context.Context, raw json.RawMessage) error {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	gvk := obj.GroupVersionKind()
	mapping, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return fmt.Errorf("resolve GVR for %s: %w", gvk, err)
	}

	var dr dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "default"
		}
		dr = a.client.Resource(mapping.Resource).Namespace(ns)
	} else {
		dr = a.client.Resource(mapping.Resource)
	}

	if err := dr.Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete %s %s/%s: %w", gvk.Kind, obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

// deliveryStateForError returns [domain.DeliveryStateAuthFailed] for
// Kubernetes API authentication/authorization errors (401/403), and
// [domain.DeliveryStateFailed] for everything else.
func deliveryStateForError(err error) domain.DeliveryState {
	if apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err) {
		return domain.DeliveryStateAuthFailed
	}
	return domain.DeliveryStateFailed
}
