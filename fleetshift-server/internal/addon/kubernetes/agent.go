// Package kubernetes implements a [domain.DeliveryAgent] that applies
// Kubernetes manifests to a cluster via server-side apply (SSA). The
// agent authenticates using the caller's JWT (token passthrough) and
// reads cluster connection info from target properties.
package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"

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

// TargetType is the [domain.TargetType] for Kubernetes clusters
// managed by the direct delivery agent (token-passthrough, no fleetlet).
const TargetType domain.TargetType = "kubernetes"

// ManifestResourceType is the [domain.ResourceType] for generic
// Kubernetes manifests applied via server-side apply.
const ManifestResourceType domain.ResourceType = "kubernetes"

const fieldManager = "fleetshift"

// Agent implements [domain.DeliveryAgent] for Kubernetes clusters using
// token passthrough. It reads cluster connection info (API server URL,
// CA cert) from target properties and authenticates with the caller's
// JWT. No stored credentials are used.
type Agent struct{}

// NewAgent returns an Agent.
func NewAgent() *Agent {
	return &Agent{}
}

// Deliver validates the target and auth synchronously and returns
// [domain.DeliveryStateAccepted] immediately. The actual SSA apply
// runs in a background goroutine; on completion the goroutine calls
// [domain.DeliverySignaler.Done].
func (a *Agent) Deliver(ctx context.Context, target domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	if _, ok := target.Properties["api_server"]; !ok {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed},
			fmt.Errorf("%w: target %q missing api_server property", domain.ErrInvalidArgument, target.ID)
	}
	if auth.Token == "" {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed},
			fmt.Errorf("%w: delivery to target %q requires an authenticated caller token", domain.ErrInvalidArgument, target.ID)
	}

	go a.deliverAsync(ctx, target, manifests, auth, signaler)

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (a *Agent) deliverAsync(ctx context.Context, target domain.TargetInfo, manifests []domain.Manifest, auth domain.DeliveryAuth, signaler *domain.DeliverySignaler) {
	cfg, err := buildRESTConfig(target, auth.Token)
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build kubernetes client for target %q: %v", target.ID, err),
		})
		return
	}

	ap, err := newApplierFromConfig(cfg)
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("build kubernetes client for target %q: %v", target.ID, err),
		})
		return
	}

	for i, m := range manifests {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: fmt.Sprintf("Applying manifest %d/%d", i+1, len(manifests)),
		})

		if err := ap.apply(ctx, m.Raw); err != nil {
			signaler.Done(ctx, domain.DeliveryResult{
				State:   domain.DeliveryStateFailed,
				Message: fmt.Sprintf("apply manifest %d: %v", i+1, err),
			})
			return
		}
	}

	signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
}

// Remove is a no-op for now.
// TODO: implement resource pruning on removal
func (a *Agent) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

func buildRESTConfig(target domain.TargetInfo, token domain.RawToken) (*rest.Config, error) {
	apiServer := target.Properties["api_server"]
	if apiServer == "" {
		return nil, fmt.Errorf("target %q missing api_server property", target.ID)
	}
	cfg := &rest.Config{
		Host:        apiServer,
		BearerToken: string(token),
	}
	if ca := target.Properties["ca_cert"]; ca != "" {
		cfg.TLSClientConfig.CAData = []byte(ca)
	}
	return cfg, nil
}

// applier wraps a dynamic client and REST mapper for SSA.
type applier struct {
	client dynamic.Interface
	mapper meta.RESTMapper
}

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
