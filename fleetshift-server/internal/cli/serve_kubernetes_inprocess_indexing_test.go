package cli

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kubernetesaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func seedKubernetesObjectType(t *testing.T, store domain.Store) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	sch := kubernetesaddon.InventorySchema()
	def := domain.NewExtensionResourceType(sch.ResourceType, domain.APIVersion(sch.Version), domain.CollectionID(sch.CollectionID), time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestStoreBackedTargetLister_ListsFromStore(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "listed",
		Type: "kubernetes",
		Name: "listed",
	})

	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Targets().Create(context.Background(), target); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := (storeTargetLister{store: store}).ListTargets(context.Background())
	if err != nil {
		t.Fatalf("ListTargets: %v", err)
	}
	if len(got) != 1 || got[0].ID() != "listed" {
		t.Fatalf("ListTargets = %v, want [listed]", got)
	}
}

func TestDirectInventoryReportBackend_RoundTrip(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedKubernetesObjectType(t, store)

	ctx := context.Background()
	now := time.Now()
	reports := application.NewInventoryReportService(store)
	subtrees := application.NewTargetInventoryCleanupService(store)
	backend := newDirectInventoryReportBackend(reports, subtrees)

	podsGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	pod1, err := kubernetesaddon.ObjectResourceName(kubernetesaddon.KubernetesObjectIdentity{
		TargetID: "prod", GVR: podsGVR, Namespace: "default", Name: "web-1", UID: "uid-pod-1",
	})
	if err != nil {
		t.Fatalf("ObjectResourceName: %v", err)
	}
	obs := json.RawMessage(`{"kind":"Pod"}`)

	if err := backend.ReplaceBatch(ctx, kubernetesaddon.ObjectResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name: pod1, Observation: &obs, ObservedAt: now,
	}}); err != nil {
		t.Fatalf("ReplaceBatch: %v", err)
	}

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetesaddon.ObjectResourceType.FullName(pod1)); err != nil {
		readTx.Rollback()
		t.Fatalf("Get after ReplaceBatch: %v", err)
	}
	readTx.Rollback()

	if err := backend.DeleteBatch(ctx, []domain.InventoryResourceRef{{
		ResourceType: kubernetesaddon.ObjectResourceType,
		Name:         pod1,
	}}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	readTx, err = store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetesaddon.ObjectResourceType.FullName(pod1)); !errors.Is(err, domain.ErrNotFound) {
		readTx.Rollback()
		t.Fatalf("Get after DeleteBatch err=%v, want ErrNotFound", err)
	}
	readTx.Rollback()

	collection := pod1.Collection()
	if err := backend.ReplaceCollection(ctx, kubernetesaddon.ObjectResourceType, collection, []kubernetesaddon.InventoryObjectReport{{
		Name: pod1, ObservedAt: now,
	}}); err != nil {
		t.Fatalf("ReplaceCollection: %v", err)
	}
	if err := backend.DeleteCollection(ctx, kubernetesaddon.ObjectResourceType, collection); err != nil {
		t.Fatalf("DeleteCollection: %v", err)
	}
	readTx, err = store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetesaddon.ObjectResourceType.FullName(pod1)); !errors.Is(err, domain.ErrNotFound) {
		readTx.Rollback()
		t.Fatalf("Get after DeleteCollection err=%v, want ErrNotFound", err)
	}
	readTx.Rollback()

	if err := backend.ReplaceCollection(ctx, kubernetesaddon.ObjectResourceType, collection, []kubernetesaddon.InventoryObjectReport{{
		Name: pod1, ObservedAt: now,
	}}); err != nil {
		t.Fatalf("re-seed ReplaceCollection: %v", err)
	}
	if err := backend.DeleteSubtree(ctx, domain.InventorySubtreeRef{
		ResourceType: kubernetesaddon.ObjectResourceType,
		Parent:       "clusters/prod",
	}); err != nil {
		t.Fatalf("DeleteSubtree: %v", err)
	}
	readTx, err = store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer readTx.Rollback()
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetesaddon.ObjectResourceType.FullName(pod1)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get after DeleteSubtree err=%v, want ErrNotFound", err)
	}
}

func TestNewKubernetesInProcessIndexing_WiresHooksAndController(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedKubernetesObjectType(t, store)

	// serve.go registers this schema on addon Connect; keep the wiring
	// contract explicit here so a schema drift fails without a full serve.
	sch := kubernetesaddon.InventorySchema()
	if sch.ResourceType != kubernetesaddon.ObjectResourceType {
		t.Fatalf("Schema.ResourceType = %q, want %q", sch.ResourceType, kubernetesaddon.ObjectResourceType)
	}
	if sch.Inventory == nil {
		t.Fatal("Schema must declare Inventory for Connect registration")
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	ctx := context.Background()
	now := time.Now()
	reports := application.NewInventoryReportService(store)

	podsGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	pod1, err := kubernetesaddon.ObjectResourceName(kubernetesaddon.KubernetesObjectIdentity{
		TargetID: "prod", GVR: podsGVR, Namespace: "default", Name: "web-1", UID: "uid-pod-1",
	})
	if err != nil {
		t.Fatalf("ObjectResourceName: %v", err)
	}
	if err := reports.ReplaceCollection(ctx, application.InventoryCollectionReplacementInput{
		ResourceType: kubernetesaddon.ObjectResourceType,
		Collection:   pod1.Collection(),
		Reports:      []application.InventoryReplacementInput{{Name: &pod1, ObservedAt: now}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	hooks, controller := newKubernetesInProcessIndexing(runCtx, store, nil, slog.New(slog.DiscardHandler))
	if hooks == nil {
		t.Fatal("expected non-nil hooks")
	}
	if controller == nil {
		t.Fatal("expected non-nil controller")
	}

	// Mirror serve.go: start the controller under the process context.
	done := make(chan struct{})
	go func() {
		defer close(done)
		controller.Run(runCtx)
	}()
	defer func() {
		cancelRun()
		<-done
	}()

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "prod",
		Name: "prod",
		Type: kubernetesaddon.TargetType,
	})
	hooks.AfterTargetRegistered(ctx, target)
	if err := hooks.BeforeTargetDeleted(ctx, target); err != nil {
		t.Fatalf("BeforeTargetDeleted: %v", err)
	}

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer readTx.Rollback()
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetesaddon.ObjectResourceType.FullName(pod1)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get after BeforeTargetDeleted err=%v, want ErrNotFound", err)
	}
}

func TestStoreBackedTargetLister_BeginError(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if _, err := (storeTargetLister{store: store}).ListTargets(context.Background()); err == nil {
		t.Fatal("expected ListTargets error on closed db")
	}
}

func TestStoreBackedTargetLister_ListError(t *testing.T) {
	store := &listFailStore{err: errors.New("list targets failed")}
	_, err := (storeTargetLister{store: store}).ListTargets(context.Background())
	if err == nil {
		t.Fatal("expected ListTargets error when Targets().List fails")
	}
	if !errors.Is(err, store.err) {
		t.Fatalf("ListTargets error = %v, want wrapped %v", err, store.err)
	}
}

// listFailStore / listFailTx exercise the Targets().List error path in
// storeTargetLister without standing up a real DB failure mode.
type listFailStore struct{ err error }

func (s *listFailStore) Begin(context.Context) (domain.Tx, error) {
	return nil, errors.New("Begin unused")
}

func (s *listFailStore) BeginReadOnly(context.Context) (domain.Tx, error) {
	return &listFailTx{err: s.err}, nil
}

type listFailTx struct{ err error }

func (t *listFailTx) Targets() domain.TargetRepository { return listFailTargets{err: t.err} }
func (t *listFailTx) Fulfillments() domain.FulfillmentRepository {
	panic("Fulfillments unused")
}
func (t *listFailTx) Deployments() domain.DeploymentRepository { panic("Deployments unused") }
func (t *listFailTx) Deliveries() domain.DeliveryRepository    { panic("Deliveries unused") }
func (t *listFailTx) Inventory() domain.InventoryRepository    { panic("Inventory unused") }
func (t *listFailTx) ExtensionResources() domain.ExtensionResourceRepository {
	panic("ExtensionResources unused")
}
func (t *listFailTx) SignerEnrollments() domain.SignerEnrollmentRepository {
	panic("SignerEnrollments unused")
}
func (t *listFailTx) ResourceIdentities() domain.ResourceIdentityRepository {
	panic("ResourceIdentities unused")
}
func (t *listFailTx) Commit() error   { return nil }
func (t *listFailTx) Rollback() error { return nil }

type listFailTargets struct{ err error }

func (listFailTargets) Create(context.Context, domain.TargetInfo) error { panic("Create unused") }
func (listFailTargets) CreateOrUpdate(context.Context, domain.TargetInfo) error {
	panic("CreateOrUpdate unused")
}
func (listFailTargets) Get(context.Context, domain.TargetID) (domain.TargetInfo, error) {
	panic("Get unused")
}
func (r listFailTargets) List(context.Context) ([]domain.TargetInfo, error) {
	return nil, r.err
}
func (listFailTargets) Delete(context.Context, domain.TargetID) error { panic("Delete unused") }

func TestDirectInventoryReportBackend_PropagatesServiceErrors(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	// Intentionally do not seed Object type so report writes fail validation.
	ctx := context.Background()
	now := time.Now()
	backend := newDirectInventoryReportBackend(
		application.NewInventoryReportService(store),
		application.NewTargetInventoryCleanupService(store),
	)

	name := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid-1")
	report := kubernetesaddon.InventoryObjectReport{Name: name, ObservedAt: now}

	if err := backend.ReplaceBatch(ctx, kubernetesaddon.ObjectResourceType, []kubernetesaddon.InventoryObjectReport{report}); err == nil {
		t.Fatal("expected ReplaceBatch error")
	}
	if err := backend.DeleteBatch(ctx, []domain.InventoryResourceRef{{
		ResourceType: kubernetesaddon.ObjectResourceType,
		Name:         name,
	}}); err == nil {
		t.Fatal("expected DeleteBatch error")
	}
	if err := backend.ReplaceCollection(ctx, kubernetesaddon.ObjectResourceType, name.Collection(), []kubernetesaddon.InventoryObjectReport{report}); err == nil {
		t.Fatal("expected ReplaceCollection error")
	}
	if err := backend.DeleteCollection(ctx, kubernetesaddon.ObjectResourceType, name.Collection()); err == nil {
		t.Fatal("expected DeleteCollection error")
	}
	if err := backend.DeleteSubtree(ctx, domain.InventorySubtreeRef{
		ResourceType: kubernetesaddon.ObjectResourceType,
		Parent:       "clusters/prod",
	}); err == nil {
		t.Fatal("expected DeleteSubtree error")
	}
}
