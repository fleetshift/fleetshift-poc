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

func TestKubernetesIndexStartupReplay_CancelsAndJoins(t *testing.T) {
	replayCtx, cancelReplay := context.WithCancel(context.Background())
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{}, 1)
	t.Cleanup(func() {
		cancelReplay()
		select {
		case release <- struct{}{}:
		default:
		}
	})

	done := startKubernetesIndexStartupReplay(replayCtx, func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replay start")
	}

	stopReturned := make(chan struct{})
	go func() {
		cancelReplay()
		<-done
		close(stopReturned)
	}()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replay cancellation")
	}
	select {
	case <-stopReturned:
		t.Fatal("join returned before replay release")
	default:
	}

	release <- struct{}{}
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replay join")
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
	backend := newDirectInventoryReportBackend(reports)

	podsGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	pod1, err := kubernetesaddon.ObjectResourceName(kubernetesaddon.KubernetesObjectIdentity{
		ClusterResourceName: "clusters/prod", GVR: podsGVR, Namespace: "default", Name: "web-1", UID: "uid-pod-1",
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

	if err := backend.ReplaceBatch(ctx, kubernetesaddon.ObjectResourceType, []kubernetesaddon.InventoryObjectReport{{
		Name: pod1, IsDelete: true,
	}}); err != nil {
		t.Fatalf("ReplaceBatch delete: %v", err)
	}
	readTx, err = store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	if _, err := readTx.ExtensionResources().Get(ctx, kubernetesaddon.ObjectResourceType.FullName(pod1)); !errors.Is(err, domain.ErrNotFound) {
		readTx.Rollback()
		t.Fatalf("Get after ReplaceBatch delete err=%v, want ErrNotFound", err)
	}
	readTx.Rollback()
}

func TestNewKubernetesInProcessIndexing_WiresIndexingRuntime(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedKubernetesObjectType(t, store)

	sch := kubernetesaddon.InventorySchema()
	if sch.ResourceType != kubernetesaddon.ObjectResourceType {
		t.Fatalf("Schema.ResourceType = %q, want %q", sch.ResourceType, kubernetesaddon.ObjectResourceType)
	}
	if sch.Inventory == nil {
		t.Fatal("Schema must declare Inventory for Connect registration")
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	indexing := newKubernetesInProcessIndexing(runCtx, store, nil, slog.New(slog.DiscardHandler))
	if indexing == nil || indexing.Runtime == nil || indexing.Host == nil {
		t.Fatal("expected wired indexing runtime")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := indexing.Runtime.StopAll(stopCtx); err != nil {
		t.Fatalf("StopAll: %v", err)
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
func (t *listFailTx) Queries() domain.QueryRepository { panic("Queries unused") }
func (t *listFailTx) Commit() error                   { return nil }
func (t *listFailTx) Rollback() error                 { return nil }

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
	ctx := context.Background()
	now := time.Now()
	backend := newDirectInventoryReportBackend(application.NewInventoryReportService(store))

	name := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid-1")
	report := kubernetesaddon.InventoryObjectReport{Name: name, ObservedAt: now}
	deleteReport := kubernetesaddon.InventoryObjectReport{Name: name, IsDelete: true}

	if err := backend.ReplaceBatch(ctx, kubernetesaddon.ObjectResourceType, []kubernetesaddon.InventoryObjectReport{report}); err == nil {
		t.Fatal("expected ReplaceBatch upsert error")
	}
	if err := backend.ReplaceBatch(ctx, kubernetesaddon.ObjectResourceType, []kubernetesaddon.InventoryObjectReport{deleteReport}); err == nil {
		t.Fatal("expected ReplaceBatch delete error")
	}
}
