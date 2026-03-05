// Package kubernetes implements a [domain.DeliveryAgent] that applies
// Kubernetes manifests to a cluster via server-side apply (SSA). The
// kubeconfig is retrieved from a [domain.Vault] using a ref stored in
// the target's Properties.
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
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetType is the [domain.TargetType] for Kubernetes clusters
// managed by the direct delivery agent (kubeconfig-based, no fleetlet).
const TargetType domain.TargetType = "kubernetes"

const fieldManager = "fleetshift"

// Agent implements [domain.DeliveryAgent] for direct Kubernetes cluster
// access via kubeconfig. It retrieves the kubeconfig from a [domain.Vault]
// and applies manifests using server-side apply.
type Agent struct {
	vault domain.Vault
}

// NewAgent returns an Agent that retrieves kubeconfigs from the given vault.
func NewAgent(vault domain.Vault) *Agent {
	return &Agent{vault: vault}
}

// Deliver validates the target synchronously and returns
// [domain.DeliveryStateAccepted] immediately. The actual SSA apply
// runs in a background goroutine; on completion the goroutine calls
// [domain.DeliverySignaler.Done] so the workflow receives the terminal
// result via signal rather than activity return value.
func (a *Agent) Deliver(ctx context.Context, target domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	if _, ok := target.Properties["kubeconfig_ref"]; !ok {
		return domain.DeliveryResult{State: domain.DeliveryStateFailed},
			fmt.Errorf("%w: target %q missing kubeconfig_ref property", domain.ErrInvalidArgument, target.ID)
	}

	go a.deliverAsync(ctx, target, manifests, signaler)

	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (a *Agent) deliverAsync(ctx context.Context, target domain.TargetInfo, manifests []domain.Manifest, signaler *domain.DeliverySignaler) {
	ref := target.Properties["kubeconfig_ref"]

	kubeconfigBytes, err := a.vault.Get(ctx, domain.SecretRef(ref))
	if err != nil {
		signaler.Done(ctx, domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("retrieve kubeconfig for target %q: %v", target.ID, err),
		})
		return
	}

	ap, err := newApplier(kubeconfigBytes)
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

// applier wraps a dynamic client and REST mapper for SSA.
type applier struct {
	client dynamic.Interface
	mapper meta.RESTMapper
}

func newApplier(kubeconfig []byte) (*applier, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}

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
