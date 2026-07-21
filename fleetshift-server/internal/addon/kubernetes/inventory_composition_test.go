package kubernetes

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// Composition tests exercise the in-process indexing host and writer
// against a real SQLite extension-resource store with fake Kubernetes
// clients. They close gaps that unit tests (recording reporters) and
// kind e2e leave open, including Writer→store localLabels projection.

func configmapsGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
}

func makeConfigMap(uid, name, namespace, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"uid":             uid,
			"name":            name,
			"namespace":       namespace,
			"resourceVersion": rv,
		},
		"data": map[string]any{"k": "v"},
	}}
}

func seedObjectType(t *testing.T, store domain.Store) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	sch := InventorySchema()
	def := domain.NewExtensionResourceType(sch.ResourceType, domain.APIVersion(sch.Version), domain.CollectionID(sch.CollectionID), time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

type storeReportBackend struct {
	reports *application.InventoryReportService
}

func (b *storeReportBackend) ReplaceBatch(ctx context.Context, resourceType domain.ResourceType, reports []InventoryObjectReport) error {
	in := application.InventoryReplacementBatchInput{
		Reports: make([]application.InventoryReplacementInput, len(reports)),
	}
	for i, report := range reports {
		name := report.Name
		in.Reports[i] = application.InventoryReplacementInput{
			ResourceType: resourceType,
			Name:         &name,
			IsDelete:     report.IsDelete,
			Labels:       report.Labels,
			Observation:  report.Observation,
			Conditions:   report.Conditions,
			ObservedAt:   report.ObservedAt,
		}
	}
	return b.reports.ReplaceBatch(ctx, in)
}

func newStoreBackedReporter(store domain.Store) InventoryReporter {
	reports := application.NewInventoryReportService(store)
	return NewDirectInventoryReporter(&storeReportBackend{reports: reports})
}

func listObjectInventory(t *testing.T, store domain.Store) []*domain.ExtensionResource {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()
	objs, err := tx.ExtensionResources().ListByResourceType(context.Background(), ObjectResourceType)
	if err != nil {
		t.Fatalf("ListByResourceType: %v", err)
	}
	return objs
}

func objectsForCluster(objs []*domain.ExtensionResource, clusterID string) []*domain.ExtensionResource {
	prefix := "clusters/" + clusterID + "/"
	var out []*domain.ExtensionResource
	for _, obj := range objs {
		if strings.HasPrefix(string(obj.Name()), prefix) {
			out = append(out, obj)
		}
	}
	return out
}

func awaitStoreObjects(t *testing.T, store domain.Store, timeout time.Duration, pred func([]*domain.ExtensionResource) bool) []*domain.ExtensionResource {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		objs := listObjectInventory(t, store)
		if pred(objs) {
			return objs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for store inventory (%d objects)", len(objs))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func mustNamedObjectName(t *testing.T, clusterID string, gvr schema.GroupVersionResource, namespace, name, uid string) domain.ResourceName {
	t.Helper()
	scope := ObjectScopeNamespaced
	if namespace == "" {
		scope = ObjectScopeCluster
	}
	rn, err := ObjectResourceName(KubernetesObjectIdentity{
		ClusterResourceName: testClusterResourceName(clusterID),
		GVR:                 gvr,
		ScopeNamespace:      mustScopeNamespace(t, scope, namespace),
		Name:                name,
		UID:                 uid,
	})
	if err != nil {
		t.Fatalf("ObjectResourceName: %v", err)
	}
	return rn
}

func assertResourceName(t *testing.T, obj *domain.ExtensionResource, want domain.ResourceName) {
	t.Helper()
	if obj.Name() != want {
		t.Fatalf("resource name = %q, want %q", obj.Name(), want)
	}
	if obj.Name().Collection() != want.Collection() {
		t.Fatalf("collection = %q, want %q", obj.Name().Collection(), want.Collection())
	}
}

func compositionTarget(id domain.TargetID) domain.TargetInfo {
	return domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:    id,
		Type:  TargetType,
		Name:  string(id),
		State: domain.TargetStateReady,
		Properties: map[string]string{
			PropAPIServer:           "https://composition.example",
			PropCACert:              "unused",
			PropServiceAccountToken: "unused",
			PropClusterResourceName: string(testClusterResourceName(string(id))),
		},
	})
}

func fakeClientsForPodsAndConfigMaps(t *testing.T) (dynamic.Interface, discovery.DiscoveryInterface) {
	t.Helper()
	pods := podsGVR()
	cms := configmapsGVR()
	disc := newFakeDiscovery([]*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{
			{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
			{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
		},
	}})
	dyn := newFakeDynamicClient(pods, cms, crdGVR)
	return dyn, disc
}

func createPod(t *testing.T, dyn dynamic.Interface, uid, name string) {
	t.Helper()
	pod := makePod(uid, name, "default", "1")
	if _, err := dyn.Resource(podsGVR()).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod %s: %v", name, err)
	}
}

func newCompositionHost(
	ctx context.Context,
	store domain.Store,
	dyn dynamic.Interface,
	disc discovery.DiscoveryInterface,
	indexCfg IndexConfig,
) *KubernetesInProcessIndexHost {
	_ = indexCfg // applied via EnsureIndexer IndexRuntimeInput
	reporter := newStoreBackedReporter(store)
	return NewKubernetesInProcessIndexHost(
		ctx,
		nil,
		reporter,
		fakeIndexerClients{
			dynamic: func(*rest.Config) (dynamic.Interface, error) {
				return dyn, nil
			},
			discovery: func(*rest.Config) (discovery.DiscoveryInterface, error) {
				return disc, nil
			},
		},
		slog.New(slog.DiscardHandler),
	)
}

func ensureCompositionIndexer(t *testing.T, host *KubernetesInProcessIndexHost, target domain.TargetInfo, cfg IndexConfig) {
	t.Helper()
	props := target.Properties()
	rawCluster := props[PropClusterResourceName]
	if rawCluster == "" {
		t.Fatalf("target %q missing %s", target.ID(), PropClusterResourceName)
	}
	clusterResourceName, err := ParseClusterResourceName(rawCluster)
	if err != nil {
		t.Fatalf("target %q %s: %v", target.ID(), PropClusterResourceName, err)
	}
	input, err := NewIndexRuntimeInput(
		target.ID(),
		clusterResourceName,
		props[PropAPIServer],
		props[PropCACert],
		[]byte(props[PropServiceAccountToken]),
		"",
		1,
		cfg,
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := host.EnsureIndexer(context.Background(), input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}
}

// TestStopIndexer_LeavesInventory verifies StopIndexer stops the indexer
// and leaves previously written Object inventory rows in the store.
func TestStopIndexer_LeavesInventory(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	dyn, disc := fakeClientsForPodsAndConfigMaps(t)
	createPod(t, dyn, "uid-web", "web")

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	cfg := IndexConfig{
		Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
			pods: {GVR: pods},
		}},
		AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
		BatchInterval: 50 * time.Millisecond,
	}
	host := newCompositionHost(runCtx, store, dyn, disc, cfg)

	target := compositionTarget("prod")
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Targets().Create(context.Background(), target); err != nil {
		t.Fatalf("Create target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit target: %v", err)
	}

	ensureCompositionIndexer(t, host, target, cfg)
	wantName := mustNamedObjectName(t, "prod", pods, "default", "web", "uid-web")
	awaitStoreObjects(t, store, 5*time.Second, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objectsForCluster(objs, "prod") {
			if obj.Name() == wantName {
				return true
			}
		}
		return false
	})

	if err := host.StopIndexer(context.Background(), target.ID()); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
	if host.HasIndexer("prod") {
		t.Fatal("indexer should be stopped after StopIndexer")
	}

	got := objectsForCluster(listObjectInventory(t, store), "prod")
	if len(got) == 0 {
		t.Fatal("expected indexed inventory to remain after StopIndexer")
	}
	found := false
	for _, obj := range got {
		if obj.Name() == wantName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected object %q to remain after StopIndexer", wantName)
	}
}

// TestEnsureIndexer_IndexesTarget verifies EnsureIndexer starts indexing
// into the store for a target with resolvable credentials.
func TestEnsureIndexer_IndexesTarget(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	dyn, disc := fakeClientsForPodsAndConfigMaps(t)
	createPod(t, dyn, "uid-a", "pod-a")

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	cfg := IndexConfig{
		Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
			pods: {GVR: pods},
		}},
		AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
		BatchInterval: 50 * time.Millisecond,
	}
	host := newCompositionHost(runCtx, store, dyn, disc, cfg)
	local := compositionTarget("local")
	ensureCompositionIndexer(t, host, local, cfg)

	if !host.HasIndexer("local") {
		t.Fatal("expected local indexer running")
	}

	wantLocal := mustNamedObjectName(t, "local", pods, "default", "pod-a", "uid-a")
	awaitStoreObjects(t, store, 5*time.Second, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objectsForCluster(objs, "local") {
			if obj.Name() == wantLocal {
				return true
			}
		}
		return false
	})
}

// TestRemoveGVR_LeavesPersistedCollection verifies GVR removal is
// non-destructive to stored inventory while clearing the writer's
// in-memory state for that GVR.
func TestRemoveGVR_LeavesPersistedCollection(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	cms := configmapsGVR()
	reporter := newStoreBackedReporter(store)
	schema := IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
		pods: {GVR: pods},
		cms:  {GVR: cms},
	}}
	w := NewWriter("clusters/prod", reporter, NoopEdgeSink{}, schema.Entries, time.Hour, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	pod := makePod("uid-pod", "web", "default", "1")
	cm := makeConfigMap("uid-cm", "cfg", "default", "1")
	w.ResyncCh() <- ResyncEvent{GVR: pods, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{pod}}
	w.ResyncCh() <- ResyncEvent{GVR: cms, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{cm}}

	wantPod := mustNamedObjectName(t, "prod", pods, "default", "web", "uid-pod")
	wantCM := mustNamedObjectName(t, "prod", cms, "default", "cfg", "uid-cm")
	awaitStoreObjects(t, store, 3*time.Second, func(objs []*domain.ExtensionResource) bool {
		havePod, haveCM := false, false
		for _, obj := range objs {
			switch obj.Name() {
			case wantPod:
				havePod = true
			case wantCM:
				haveCM = true
			}
		}
		return havePod && haveCM
	})

	w.RemoveCh() <- RemoveGVREvent{GVR: cms}

	objs := listObjectInventory(t, store)
	var foundPod, foundCM bool
	for _, obj := range objs {
		if obj.Name() == wantCM {
			foundCM = true
			assertResourceName(t, obj, wantCM)
		}
		if obj.Name() == wantPod {
			foundPod = true
			assertResourceName(t, obj, wantPod)
		}
	}
	if !foundPod {
		t.Fatal("pod collection must survive configmap GVR removal")
	}
	if !foundCM {
		t.Fatal("configmap rows must remain after non-destructive GVR removal")
	}
}

// TestResync_RemovesAbsentReportedNamesFromStore verifies a same-process
// LIST/resync deletes store rows for UIDs previously acknowledged in
// ReportedNames but absent from the LIST, without requiring a watch
// DELETE event. This is process-generation omission reconciliation, not
// durable DB collection wipe of unknown rows.
func TestResync_RemovesAbsentReportedNamesFromStore(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	reporter := newStoreBackedReporter(store)
	schema := IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
		pods: {GVR: pods},
	}}
	w := NewWriter("clusters/prod", reporter, NoopEdgeSink{}, schema.Entries, time.Hour, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	pod1 := makePod("uid-1", "keep", "default", "1")
	pod2 := makePod("uid-2", "drop", "default", "1")
	w.ResyncCh() <- ResyncEvent{GVR: pods, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{pod1, pod2}}

	wantKeep := mustNamedObjectName(t, "prod", pods, "default", "keep", "uid-1")
	wantDrop := mustNamedObjectName(t, "prod", pods, "default", "drop", "uid-2")
	awaitStoreObjects(t, store, 3*time.Second, func(objs []*domain.ExtensionResource) bool {
		haveKeep, haveDrop := false, false
		for _, obj := range objs {
			switch obj.Name() {
			case wantKeep:
				haveKeep = true
			case wantDrop:
				haveDrop = true
			}
		}
		return haveKeep && haveDrop
	})

	w.ResyncCh() <- ResyncEvent{GVR: pods, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{pod1}}
	awaitStoreObjects(t, store, 3*time.Second, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objs {
			if obj.Name() == wantDrop {
				return false
			}
		}
		return true
	})

	objs := listObjectInventory(t, store)
	var foundKeep bool
	for _, obj := range objs {
		if obj.Name() == wantDrop {
			t.Fatalf("omitted ReportedUID still present: %s", obj.Name())
		}
		if obj.Name() == wantKeep {
			foundKeep = true
			assertResourceName(t, obj, wantKeep)
		}
	}
	if !foundKeep {
		t.Fatal("sibling object must survive ReportedNames-diff resync")
	}
}

// TestWriter_PersistsKubeLabelsAsLocalLabels verifies Writer → store
// projects metadata.labels onto inventory localLabels, omits synthetic
// identity labels, drops observation.metadata.labels, and replaces
// localLabels on a later resync when kube labels change.
func TestWriter_PersistsKubeLabelsAsLocalLabels(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	reporter := newStoreBackedReporter(store)
	schema := IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
		pods: {GVR: pods},
	}}
	w := NewWriter("clusters/prod", reporter, NoopEdgeSink{}, schema.Entries, time.Hour, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	labeled := makePod("uid-labeled", "web", "default", "1")
	labeled.SetLabels(map[string]string{
		"app":                    "web",
		"kubernetes.io/hostname": "node-1",
	})
	unlabeled := makePod("uid-plain", "plain", "default", "1")
	w.ResyncCh() <- ResyncEvent{GVR: pods, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{labeled, unlabeled}}

	wantLabeled := mustNamedObjectName(t, "prod", pods, "default", "web", "uid-labeled")
	wantPlain := mustNamedObjectName(t, "prod", pods, "default", "plain", "uid-plain")
	objs := awaitStoreObjects(t, store, 3*time.Second, func(objs []*domain.ExtensionResource) bool {
		haveLabeled, havePlain := false, false
		for _, obj := range objs {
			switch obj.Name() {
			case wantLabeled:
				haveLabeled = true
			case wantPlain:
				havePlain = true
			}
		}
		return haveLabeled && havePlain
	})

	var labeledInv, plainInv *domain.InventoryResource
	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		switch obj.Name() {
		case wantLabeled:
			labeledInv = inv
		case wantPlain:
			plainInv = inv
		}
	}
	if labeledInv == nil || plainInv == nil {
		t.Fatalf("missing inventory: labeled=%v plain=%v", labeledInv != nil, plainInv != nil)
	}

	if got := labeledInv.Labels()["app"]; got != "web" {
		t.Errorf("labeled localLabels[app] = %q, want web", got)
	}
	if got := labeledInv.Labels()["kubernetes.io/hostname"]; got != "node-1" {
		t.Errorf("labeled localLabels[kubernetes.io/hostname] = %q, want node-1", got)
	}
	for _, synthetic := range []string{"k8s.kind", "k8s.gvr", "k8s.name", "k8s.uid", "k8s.group", "k8s.scope"} {
		if _, ok := labeledInv.Labels()[synthetic]; ok {
			t.Errorf("labeled localLabels must not include synthetic %q, got %#v", synthetic, labeledInv.Labels())
		}
	}
	if len(plainInv.Labels()) != 0 {
		t.Errorf("unlabeled localLabels = %#v, want empty", plainInv.Labels())
	}

	assertObservationOmitsMetadataLabels := func(t *testing.T, inv *domain.InventoryResource) {
		t.Helper()
		obs := inv.Observation()
		if obs == nil {
			t.Fatal("nil observation")
		}
		var top map[string]any
		if err := json.Unmarshal(*obs, &top); err != nil {
			t.Fatalf("unmarshal observation: %v", err)
		}
		meta, _ := top["metadata"].(map[string]any)
		if _, ok := meta["labels"]; ok {
			t.Errorf("observation.metadata.labels = %#v, want omitted", meta["labels"])
		}
		if top["kind"] != "Pod" {
			t.Errorf("observation.kind = %v, want Pod", top["kind"])
		}
	}
	assertObservationOmitsMetadataLabels(t, labeledInv)
	assertObservationOmitsMetadataLabels(t, plainInv)

	labeled.SetLabels(map[string]string{"app": "api", "tier": "frontend"})
	labeled.SetResourceVersion("2")
	w.ResyncCh() <- ResyncEvent{GVR: pods, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{labeled, unlabeled}}
	awaitStoreObjects(t, store, 3*time.Second, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objs {
			if obj.Name() != wantLabeled {
				continue
			}
			inv := obj.Inventory()
			return inv != nil && inv.Labels()["app"] == "api" && inv.Labels()["tier"] == "frontend"
		}
		return false
	})

	for _, obj := range listObjectInventory(t, store) {
		if obj.Name() != wantLabeled {
			continue
		}
		inv := obj.Inventory()
		if inv.Labels()["app"] != "api" || inv.Labels()["tier"] != "frontend" {
			t.Fatalf("replaced localLabels = %#v, want app=api tier=frontend", inv.Labels())
		}
		if _, ok := inv.Labels()["kubernetes.io/hostname"]; ok {
			t.Fatalf("replaced localLabels still has removed key kubernetes.io/hostname: %#v", inv.Labels())
		}
		assertObservationOmitsMetadataLabels(t, inv)
		return
	}
	t.Fatal("labeled object missing after label replace resync")
}

// TestStartupList_DoesNotDeleteDBOnlyRows verifies a new process/GVR
// generation's initial LIST upserts current objects but does not remove
// persisted rows that were never acknowledged in this generation
// (Accepted gap: cross-process stale inventory).
func TestStartupList_DoesNotDeleteDBOnlyRows(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	reporter := newStoreBackedReporter(store)
	schema := IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
		pods: {GVR: pods},
	}}

	// Pre-seed a DB-only row as if an earlier process wrote it.
	staleName := mustNamedObjectName(t, "prod", pods, "default", "stale", "uid-stale")
	ctx := context.Background()
	obs := json.RawMessage(`{"kind":"Pod"}`)
	if err := application.NewInventoryReportService(store).ReplaceBatch(ctx, application.InventoryReplacementBatchInput{
		Reports: []application.InventoryReplacementInput{{
			ResourceType: ObjectResourceType,
			Name:         &staleName,
			Labels:       map[string]string{"k8s.name": "stale"},
			Observation:  &obs,
			ObservedAt:   time.Now(),
		}},
	}); err != nil {
		t.Fatalf("seed stale row: %v", err)
	}

	w := NewWriter("clusters/prod", reporter, NoopEdgeSink{}, schema.Entries, time.Hour, slog.New(slog.DiscardHandler))
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(runCtx)

	live := makePod("uid-live", "live", "default", "1")
	w.ResyncCh() <- ResyncEvent{GVR: pods, Scope: ObjectScopeNamespaced, Resources: []*unstructured.Unstructured{live}}

	wantLive := mustNamedObjectName(t, "prod", pods, "default", "live", "uid-live")
	awaitStoreObjects(t, store, 3*time.Second, func(objs []*domain.ExtensionResource) bool {
		haveLive, haveStale := false, false
		for _, obj := range objs {
			switch obj.Name() {
			case wantLive:
				haveLive = true
			case staleName:
				haveStale = true
			}
		}
		return haveLive && haveStale
	})
}

// TestServeStyleComposition_EnsureIndexerIndexesTarget mirrors serve
// wiring (host + EnsureIndexer) and asserts indexing writes
// ObjectResourceName-shaped rows.
func TestServeStyleComposition_EnsureIndexerIndexesTarget(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedObjectType(t, store)

	pods := podsGVR()
	dyn, disc := fakeClientsForPodsAndConfigMaps(t)
	createPod(t, dyn, "uid-node-standin", "web")

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	cfg := IndexConfig{
		Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
			pods: {GVR: pods},
		}},
		AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
		BatchInterval: 50 * time.Millisecond,
	}
	host := newCompositionHost(runCtx, store, dyn, disc, cfg)

	target := compositionTarget("serve-smoke")
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Targets().Create(context.Background(), target); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	ensureCompositionIndexer(t, host, target, cfg)

	want := mustNamedObjectName(t, "serve-smoke", pods, "default", "web", "uid-node-standin")
	objs := awaitStoreObjects(t, store, 5*time.Second, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objectsForCluster(objs, "serve-smoke") {
			if obj.Name() == want {
				return true
			}
		}
		return false
	})
	for _, obj := range objectsForCluster(objs, "serve-smoke") {
		if obj.Name() == want {
			assertResourceName(t, obj, want)
		}
	}
	if !host.HasIndexer("serve-smoke") {
		t.Fatal("expected EnsureIndexer to start indexer")
	}
}
