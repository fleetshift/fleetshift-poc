package kubernetes_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// fakeSubtreeCleaner records DeleteOwnedInventorySubtree calls so
// KubernetesTargetIndexedInventoryCleaner's own dispatch logic --
// which target types it acts on and what ref it builds -- can be
// tested without a real store.
type fakeSubtreeCleaner struct {
	calls int
	owner domain.AddonID
	ref   domain.InventorySubtreeRef
	err   error
}

func (f *fakeSubtreeCleaner) DeleteOwnedInventorySubtree(_ context.Context, owner domain.AddonID, ref domain.InventorySubtreeRef) error {
	f.calls++
	f.owner = owner
	f.ref = ref
	return f.err
}

func newCleanupTargetInfo(id domain.TargetID, targetType domain.TargetType) domain.TargetInfo {
	return domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   id,
		Name: string(id),
		Type: targetType,
	})
}

func TestKubernetesTargetIndexedInventoryCleaner_IgnoresNonKubernetesTargetTypes(t *testing.T) {
	fake := &fakeSubtreeCleaner{}
	cleaner := kubernetes.NewKubernetesTargetIndexedInventoryCleaner(fake)

	err := cleaner.CleanupIndexedInventory(context.Background(), newCleanupTargetInfo("prod", "gcphcp"))
	if err != nil {
		t.Fatalf("CleanupIndexedInventory: %v", err)
	}
	if fake.calls != 0 {
		t.Fatalf("DeleteOwnedInventorySubtree calls = %d, want 0", fake.calls)
	}
}

func TestKubernetesTargetIndexedInventoryCleaner_DeletesOwnedSubtreeForKubernetesTarget(t *testing.T) {
	fake := &fakeSubtreeCleaner{}
	cleaner := kubernetes.NewKubernetesTargetIndexedInventoryCleaner(fake)

	err := cleaner.CleanupIndexedInventory(context.Background(), newCleanupTargetInfo("prod", kubernetes.TargetType))
	if err != nil {
		t.Fatalf("CleanupIndexedInventory: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("DeleteOwnedInventorySubtree calls = %d, want 1", fake.calls)
	}
	if fake.owner != kubernetes.AddonID {
		t.Errorf("owner = %q, want %q", fake.owner, kubernetes.AddonID)
	}
	wantRef := domain.InventorySubtreeRef{
		ResourceType: kubernetes.ObjectResourceType,
		Parent:       "clusters/prod",
	}
	if fake.ref != wantRef {
		t.Errorf("ref = %+v, want %+v", fake.ref, wantRef)
	}
}

func TestKubernetesTargetIndexedInventoryCleaner_PropagatesSubtreeDeleteError(t *testing.T) {
	wantErr := errors.New("delete failed")
	fake := &fakeSubtreeCleaner{err: wantErr}
	cleaner := kubernetes.NewKubernetesTargetIndexedInventoryCleaner(fake)

	err := cleaner.CleanupIndexedInventory(context.Background(), newCleanupTargetInfo("prod", kubernetes.TargetType))
	if !errors.Is(err, wantErr) {
		t.Fatalf("CleanupIndexedInventory error = %v, want wrapped %v", err, wantErr)
	}
}

func TestKubernetesTargetIndexedInventoryCleaner_RejectsEmptyTargetID(t *testing.T) {
	fake := &fakeSubtreeCleaner{}
	cleaner := kubernetes.NewKubernetesTargetIndexedInventoryCleaner(fake)

	err := cleaner.CleanupIndexedInventory(context.Background(), newCleanupTargetInfo("", kubernetes.TargetType))
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("CleanupIndexedInventory error = %v, want ErrInvalidArgument", err)
	}
	if fake.calls != 0 {
		t.Fatalf("DeleteOwnedInventorySubtree calls = %d, want 0", fake.calls)
	}
}

// seedKubernetesObjectType registers the real Kubernetes object
// extension resource type (the same shape kubernetes.Descriptor's
// InventoryResourceCapability and kubernetes.InventorySchema declare), so the
// composition test below exercises actual ownership and inventory
// metadata validation rather than a synthetic stand-in type.
func seedKubernetesObjectType(t *testing.T, store domain.Store) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	sch := kubernetes.InventorySchema()
	def := domain.NewExtensionResourceType(sch.ResourceType, domain.APIVersion(sch.Version), domain.CollectionID(sch.CollectionID), time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestKubernetesTargetIndexedInventoryCleaner_ComposedWithTargetOutputHookService
// registers the cleaner with TargetOutputHookService under the
// Kubernetes target type, backed by the real
// TargetInventoryCleanupService and a real store, the same way server
// composition will. No Kubernetes client, informer, or addon
// connection is constructed anywhere in this test, which is what
// proves cleanup works with the Kubernetes addon disconnected, with
// no live target cluster, and with no running in-process watcher.
func TestKubernetesTargetIndexedInventoryCleaner_ComposedWithTargetOutputHookService(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedKubernetesObjectType(t, store)

	ctx := context.Background()
	now := time.Now()
	reports := application.NewInventoryReportService(store)

	podsGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	svcGVR := schema.GroupVersionResource{Version: "v1", Resource: "services"}

	pod1, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{TargetID: "prod", GVR: podsGVR, Namespace: "default", Name: "web-1", UID: "uid-pod-1"})
	if err != nil {
		t.Fatalf("ObjectResourceName pod1: %v", err)
	}
	pod2, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{TargetID: "prod", GVR: podsGVR, Namespace: "default", Name: "web-2", UID: "uid-pod-2"})
	if err != nil {
		t.Fatalf("ObjectResourceName pod2: %v", err)
	}
	svc1, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{TargetID: "prod", GVR: svcGVR, Namespace: "default", Name: "web", UID: "uid-svc-1"})
	if err != nil {
		t.Fatalf("ObjectResourceName svc1: %v", err)
	}
	// A different target ("prod-old") sharing "prod" as a string
	// prefix: this is the same segment-boundary regression check
	// TargetInventoryCleanupServiceTest uses, now exercised through
	// the Kubernetes-specific naming contract.
	siblingPod, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{TargetID: "prod-old", GVR: podsGVR, Namespace: "default", Name: "web-1", UID: "uid-pod-3"})
	if err != nil {
		t.Fatalf("ObjectResourceName siblingPod: %v", err)
	}

	if err := reports.ReplaceCollection(ctx, application.InventoryCollectionReplacementInput{
		ResourceType: kubernetes.ObjectResourceType,
		Collection:   pod1.Collection(),
		Reports: []application.InventoryReplacementInput{
			{Name: &pod1, ObservedAt: now},
			{Name: &pod2, ObservedAt: now},
		},
	}); err != nil {
		t.Fatalf("seed pods collection: %v", err)
	}
	if err := reports.ReplaceCollection(ctx, application.InventoryCollectionReplacementInput{
		ResourceType: kubernetes.ObjectResourceType,
		Collection:   svc1.Collection(),
		Reports: []application.InventoryReplacementInput{
			{Name: &svc1, ObservedAt: now},
		},
	}); err != nil {
		t.Fatalf("seed services collection: %v", err)
	}
	if err := reports.ReplaceCollection(ctx, application.InventoryCollectionReplacementInput{
		ResourceType: kubernetes.ObjectResourceType,
		Collection:   siblingPod.Collection(),
		Reports: []application.InventoryReplacementInput{
			{Name: &siblingPod, ObservedAt: now},
		},
	}); err != nil {
		t.Fatalf("seed sibling target pods collection: %v", err)
	}

	cleaner := kubernetes.NewKubernetesTargetIndexedInventoryCleaner(application.NewTargetInventoryCleanupService(store))
	hooks := application.NewTargetOutputHookService(
		application.WithTargetIndexedInventoryCleaner(kubernetes.TargetType, cleaner),
	)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "prod", Name: "prod", Type: kubernetes.TargetType})
	if err := hooks.BeforeTargetDeleted(ctx, target); err != nil {
		t.Fatalf("BeforeTargetDeleted: %v", err)
	}

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer readTx.Rollback()

	for _, deleted := range []domain.ResourceName{pod1, pod2, svc1} {
		if _, err := readTx.ExtensionResources().Get(ctx, kubernetes.ObjectResourceType.FullName(deleted)); !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("expected %s to be deleted, got err=%v", deleted, err)
		}
	}
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetes.ObjectResourceType.FullName(siblingPod)); err != nil {
		t.Errorf("expected sibling target's object %s to survive, got err=%v", siblingPod, err)
	}
}
