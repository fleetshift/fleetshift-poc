package kubernetes

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventorySubtreeCleaner deletes every extension resource under an
// owner-validated inventory subtree. It is declared here, at the
// point of use, rather than imported from the application layer, so
// this addon package never depends on internal/application; it is
// satisfied by *application.TargetInventoryCleanupService without
// either package needing to know about the other.
type InventorySubtreeCleaner interface {
	// DeleteOwnedInventorySubtree deletes every extension resource under
	// ref after validating that ownerAddonID owns ref.ResourceType.
	DeleteOwnedInventorySubtree(ctx context.Context, ownerAddonID domain.AddonID, ref domain.InventorySubtreeRef) error
}

// KubernetesTargetIndexedInventoryCleaner is the platform-owned
// target-indexed-inventory cleaner for Kubernetes targets (see
// application.TargetIndexedInventoryCleaner). It deletes a
// terminating target's Kubernetes object subtree directly from the
// extension-resource store and never calls the Kubernetes API, so
// cleanup succeeds even with the Kubernetes addon disconnected, the
// target cluster gone, or no in-process watcher running for the target.
type KubernetesTargetIndexedInventoryCleaner struct {
	subtrees InventorySubtreeCleaner
}

// NewKubernetesTargetIndexedInventoryCleaner creates a cleaner backed
// by subtrees.
func NewKubernetesTargetIndexedInventoryCleaner(subtrees InventorySubtreeCleaner) *KubernetesTargetIndexedInventoryCleaner {
	return &KubernetesTargetIndexedInventoryCleaner{subtrees: subtrees}
}

// CleanupIndexedInventory deletes every Kubernetes object indexed
// under target. Non-Kubernetes target types are ignored: this cleaner
// is registered under the Kubernetes target type, but does not rely
// solely on that registration keying to keep it from acting on a
// TargetInfo of a different type.
func (c *KubernetesTargetIndexedInventoryCleaner) CleanupIndexedInventory(ctx context.Context, target domain.TargetInfo) error {
	if target.Type() != TargetType {
		return nil
	}
	parent, err := TargetObjectSubtree(target.ID())
	if err != nil {
		return fmt.Errorf("kubernetes target inventory cleanup (target %q): %w", target.ID(), err)
	}
	return c.subtrees.DeleteOwnedInventorySubtree(ctx, AddonID, domain.InventorySubtreeRef{
		ResourceType: ObjectResourceType,
		Parent:       parent,
	})
}
