package kubernetes

import (
	"context"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
)

// fakeDiscoveryWithPreferred wraps FakeDiscovery and overrides
// ServerPreferredResources to return the Resources field, since the upstream
// fake always returns nil.
type fakeDiscoveryWithPreferred struct {
	*fakediscovery.FakeDiscovery
}

func (f *fakeDiscoveryWithPreferred) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return f.Resources, nil
}

func newFakeDiscovery(resources []*metav1.APIResourceList) *fakeDiscoveryWithPreferred {
	fd := &fakediscovery.FakeDiscovery{
		Fake: &fakeclientset.NewSimpleClientset().Fake,
	}
	fd.Resources = resources
	return &fakeDiscoveryWithPreferred{FakeDiscovery: fd}
}

// newFakeDynamicClient creates a fake dynamic client with a scheme that knows
// about unstructured lists for the given GVRs.
func newFakeDynamicClient(gvrs ...schema.GroupVersionResource) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	gvrToListKind := make(map[schema.GroupVersionResource]string)
	for _, gvr := range gvrs {
		// Register a list kind so the fake can return empty lists.
		gvrToListKind[gvr] = gvr.Resource + "List"
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)
}

// --- SupportedResources tests ---

func TestSupportedResources_WatchableOnly(t *testing.T) {
	disc := newFakeDiscovery([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "bindings", Namespaced: false, Verbs: metav1.Verbs{"create"}}, // not watchable
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch", "create"}},
				{Name: "controllerrevisions", Namespaced: true, Verbs: metav1.Verbs{"get", "list"}}, // not watchable
			},
		},
	})

	result, err := SupportedResources(disc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := map[schema.GroupVersionResource]struct{}{
		{Group: "", Version: "v1", Resource: "pods"}:            {},
		{Group: "apps", Version: "v1", Resource: "deployments"}: {},
	}

	if len(result) != len(expected) {
		t.Fatalf("expected %d GVRs, got %d: %v", len(expected), len(result), result)
	}
	for gvr := range expected {
		desc, ok := result[gvr]
		if !ok {
			t.Errorf("expected GVR %s not found in result", gvr)
			continue
		}
		if desc.Scope != ObjectScopeNamespaced {
			t.Errorf("GVR %s scope = %q, want namespaced", gvr, desc.Scope)
		}
	}
}

func TestSupportedResources_ClusterScopedWatchable(t *testing.T) {
	disc := newFakeDiscovery([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "nodes", Namespaced: false, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "pods", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		},
	})

	result, err := SupportedResources(disc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	nodes := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	pods := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	if desc, ok := result[nodes]; !ok {
		t.Fatal("expected nodes in result")
	} else if desc.Scope != ObjectScopeCluster {
		t.Fatalf("nodes scope = %q, want cluster", desc.Scope)
	}
	if desc, ok := result[pods]; !ok {
		t.Fatal("expected pods in result")
	} else if desc.Scope != ObjectScopeNamespaced {
		t.Fatalf("pods scope = %q, want namespaced", desc.Scope)
	}
}

func TestSupportedResources_EmptyList(t *testing.T) {
	disc := newFakeDiscovery([]*metav1.APIResourceList{})

	result, err := SupportedResources(disc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 GVRs, got %d", len(result))
	}
}

func TestSupportedResources_NoWatchVerb(t *testing.T) {
	disc := newFakeDiscovery([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "bindings", Namespaced: false, Verbs: metav1.Verbs{"create"}},
				{Name: "componentstatuses", Namespaced: false, Verbs: metav1.Verbs{"get", "list"}},
			},
		},
	})

	result, err := SupportedResources(disc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 GVRs, got %d", len(result))
	}
}

// --- IsResourceAllowed tests ---

func TestIsResourceAllowed_EmptyLists(t *testing.T) {
	if !IsResourceAllowed("apps", "deployments", nil, nil, nil, slog.Default()) {
		t.Error("expected allowed with empty lists")
	}
}

func TestIsResourceAllowed_UserDenyWinsOverAllow(t *testing.T) {
	allow := []Resource{{ApiGroups: []string{"*"}, Resources: []string{"*"}}}
	deny := []Resource{{ApiGroups: []string{"apps"}, Resources: []string{"deployments"}}}

	if IsResourceAllowed("apps", "deployments", allow, deny, nil, slog.Default()) {
		t.Error("expected denied: resource in both allow and user deny")
	}
}

func TestIsResourceAllowed_DenyOnly(t *testing.T) {
	deny := []Resource{{ApiGroups: []string{""}, Resources: []string{"secrets"}}}

	if IsResourceAllowed("", "secrets", nil, deny, nil, slog.Default()) {
		t.Error("expected denied: secrets in user deny list")
	}
	if !IsResourceAllowed("", "pods", nil, deny, nil, slog.Default()) {
		t.Error("expected allowed: pods not in deny list")
	}
}

func TestIsResourceAllowed_AllowOnly(t *testing.T) {
	allow := []Resource{{ApiGroups: []string{"apps"}, Resources: []string{"deployments"}}}

	if !IsResourceAllowed("apps", "deployments", allow, nil, nil, slog.Default()) {
		t.Error("expected allowed: deployments in allow list")
	}
	if IsResourceAllowed("apps", "statefulsets", allow, nil, nil, slog.Default()) {
		t.Error("expected denied: statefulsets not in allow list")
	}
}

func TestIsResourceAllowed_WildcardAllow(t *testing.T) {
	allow := []Resource{{ApiGroups: []string{"*"}, Resources: []string{"*"}}}

	if !IsResourceAllowed("anything", "anything", allow, nil, nil, slog.Default()) {
		t.Error("expected allowed: wildcard allow")
	}
}

func TestIsResourceMatchingList_NoMatch(t *testing.T) {
	list := []Resource{{ApiGroups: []string{"apps"}, Resources: []string{"deployments"}}}

	_, _, matched := IsResourceMatchingList(list, "batch", "jobs")
	if matched {
		t.Error("expected no match")
	}
}

func TestIsResourceMatchingList_WildcardGroup(t *testing.T) {
	list := []Resource{{ApiGroups: []string{"*"}, Resources: []string{"pods"}}}

	g, k, matched := IsResourceMatchingList(list, "anything", "pods")
	if !matched {
		t.Error("expected match with wildcard group")
	}
	if g != "*" || k != "pods" {
		t.Errorf("unexpected match values: group=%s, resource=%s", g, k)
	}
}

// --- InformerManager.Reconcile tests ---

func TestInformerManager_Reconcile_StartNew(t *testing.T) {
	eventCh := make(chan ResourceEvent, 100)
	resyncCh := make(chan ResyncEvent, 100)
	logger := slog.Default()

	podsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	svcsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}

	disc := newFakeDiscovery([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "services", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		},
	})

	dynClient := newFakeDynamicClient(podsGVR, svcsGVR)
	mgr := NewInformerManager(dynClient, disc, eventCh, resyncCh, nil, nil, logger)
	stopAck := ackAllResyncs(resyncCh)
	defer stopAck()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr.Reconcile(ctx, testDiscoveredList(testDiscovered(podsGVR, ObjectScopeNamespaced), testDiscovered(svcsGVR, ObjectScopeNamespaced)))

	// After reconcile, both GVRs should have stoppers.
	if len(mgr.stoppers) != 2 {
		t.Errorf("expected 2 stoppers, got %d", len(mgr.stoppers))
	}
	if _, ok := mgr.stoppers[podsGVR]; !ok {
		t.Error("expected stopper for pods")
	}
	if _, ok := mgr.stoppers[svcsGVR]; !ok {
		t.Error("expected stopper for services")
	}
}

func TestInformerManager_Reconcile_StopRemoved(t *testing.T) {
	eventCh := make(chan ResourceEvent, 100)
	resyncCh := make(chan ResyncEvent, 100)
	logger := slog.Default()

	podsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	svcsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}

	disc := newFakeDiscovery([]*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		},
	})

	mgr := NewInformerManager(nil, disc, eventCh, resyncCh, nil, nil, logger)

	// Pre-populate stoppers to simulate previously-running informers.
	stopped := false
	mgr.stoppers[svcsGVR] = func() { stopped = true }
	mgr.stoppers[podsGVR] = func() { t.Error("pods stopper should not have been called") }

	// Desired only includes pods; services should be stopped.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	mgr.Reconcile(ctx, testDiscoveredList(testDiscovered(podsGVR, ObjectScopeNamespaced)))

	if !stopped {
		t.Error("expected services informer to be stopped")
	}
	if _, ok := mgr.stoppers[svcsGVR]; ok {
		t.Error("services should have been removed from stoppers")
	}
	// Pods should still be there (it was already running and still desired).
	if _, ok := mgr.stoppers[podsGVR]; !ok {
		t.Error("pods stopper should still exist")
	}
}

func TestInformerManager_Reconcile_SendsRemoveGVREvent(t *testing.T) {
	eventCh := make(chan ResourceEvent, 100)
	resyncCh := make(chan ResyncEvent, 100)
	removeCh := make(chan RemoveGVREvent, 100)
	logger := slog.Default()

	podsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	svcsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}

	disc := newFakeDiscovery(nil)
	mgr := NewInformerManager(nil, disc, eventCh, resyncCh, removeCh, nil, logger)

	mgr.stoppers[svcsGVR] = func() {}
	mgr.generations[svcsGVR] = 7
	mgr.stoppers[podsGVR] = func() {}
	mgr.generations[podsGVR] = 3

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	mgr.Reconcile(ctx, testDiscoveredList(testDiscovered(podsGVR, ObjectScopeNamespaced)))

	select {
	case got := <-removeCh:
		if got.GVR != svcsGVR {
			t.Fatalf("RemoveGVREvent GVR = %v, want %v", got.GVR, svcsGVR)
		}
		if got.Generation != 7 {
			t.Fatalf("RemoveGVREvent Generation = %d, want 7", got.Generation)
		}
	default:
		t.Fatal("expected RemoveGVREvent for stopped services GVR")
	}
	select {
	case got := <-removeCh:
		t.Fatalf("unexpected extra RemoveGVREvent: %v", got.GVR)
	default:
	}
}

func TestInformerManager_StopAll_DoesNotSendRemoveGVREvent(t *testing.T) {
	eventCh := make(chan ResourceEvent, 100)
	resyncCh := make(chan ResyncEvent, 100)
	removeCh := make(chan RemoveGVREvent, 100)
	logger := slog.Default()

	disc := newFakeDiscovery(nil)
	mgr := NewInformerManager(nil, disc, eventCh, resyncCh, removeCh, nil, logger)
	mgr.stoppers[schema.GroupVersionResource{Resource: "a"}] = func() {}
	mgr.stoppers[schema.GroupVersionResource{Resource: "b"}] = func() {}

	if err := mgr.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll: %v", err)
	}

	select {
	case got := <-removeCh:
		t.Fatalf("StopAll must not emit RemoveGVREvent, got %v", got.GVR)
	default:
	}
}

func TestInformerManager_StopAll(t *testing.T) {
	eventCh := make(chan ResourceEvent, 100)
	resyncCh := make(chan ResyncEvent, 100)
	logger := slog.Default()

	disc := newFakeDiscovery(nil)
	mgr := NewInformerManager(nil, disc, eventCh, resyncCh, nil, nil, logger)

	called := 0
	mgr.stoppers[schema.GroupVersionResource{Resource: "a"}] = func() { called++ }
	mgr.stoppers[schema.GroupVersionResource{Resource: "b"}] = func() { called++ }

	if err := mgr.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll: %v", err)
	}

	if called != 2 {
		t.Errorf("expected 2 stoppers called, got %d", called)
	}
	if len(mgr.stoppers) != 0 {
		t.Errorf("expected empty stoppers map, got %d entries", len(mgr.stoppers))
	}
}

// --- GenericInformer namespace filter tests ---

func TestGenericInformer_ClusterScopedPassesThroughNamespaceFilter(t *testing.T) {
	// Cluster-scoped resources (empty namespace) should always pass the
	// namespace filter, even when include patterns are set.
	nodesGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	dynClient := newFakeDynamicClient(nodesGVR)

	node := newUnstructured("nodes", "node-uid-1")
	node.SetName("worker-1")
	node.SetResourceVersion("100")
	_, err := dynClient.Resource(nodesGVR).Create(context.Background(), node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create fake node: %v", err)
	}

	nsFilter := mustNamespaceFilter(t, NamespaceFilterConfig{
		IncludePatterns: []string{"prod-*"},
	})

	eventCh := make(chan ResourceEvent, 100)
	resyncCh := make(chan ResyncEvent, 100)
	stopAck := ackAllResyncs(resyncCh)
	defer stopAck()

	informer := NewInformer(dynClient, nodesGVR, ObjectScopeCluster, eventCh, resyncCh, nsFilter, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go informer.Run(ctx)

	select {
	case ev := <-eventCh:
		if ev.Op != EventAdd {
			t.Errorf("expected EventAdd, got %v", ev.Op)
		}
		if ev.Resource.GetName() != "worker-1" {
			t.Errorf("expected resource name worker-1, got %s", ev.Resource.GetName())
		}
	case <-time.After(1 * time.Second):
		t.Error("timed out waiting for node event — cluster-scoped should pass namespace filter")
	}
	cancel()
}

// --- FilterSupportedResources tests ---

func TestFilterSupportedResources_DefaultDenyApplied(t *testing.T) {
	pod := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	events := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}
	deploy := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	leases := schema.GroupVersionResource{Group: "coordination.k8s.io", Version: "v1", Resource: "leases"}
	evK8s := schema.GroupVersionResource{Group: "events.k8s.io", Version: "v1", Resource: "events"}
	eps := schema.GroupVersionResource{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}
	supported := map[schema.GroupVersionResource]DiscoveredAPIResource{
		pod:    testDiscovered(pod, ObjectScopeNamespaced),
		events: testDiscovered(events, ObjectScopeNamespaced),
		deploy: testDiscovered(deploy, ObjectScopeNamespaced),
		leases: testDiscovered(leases, ObjectScopeNamespaced),
		evK8s:  testDiscovered(evK8s, ObjectScopeNamespaced),
		eps:    testDiscovered(eps, ObjectScopeNamespaced),
	}

	result := FilterSupportedResources(supported, nil, nil, slog.Default())

	resultMap := make(map[schema.GroupVersionResource]struct{})
	for _, desc := range result {
		resultMap[desc.GVR] = struct{}{}
	}

	if _, ok := resultMap[pod]; !ok {
		t.Error("expected pods to pass filter")
	}
	if _, ok := resultMap[deploy]; !ok {
		t.Error("expected deployments to pass filter")
	}
	if _, ok := resultMap[events]; ok {
		t.Error("expected core events to be denied")
	}
	if _, ok := resultMap[leases]; ok {
		t.Error("expected leases to be denied")
	}
	if len(result) != 2 {
		t.Errorf("expected 2 results, got %d", len(result))
	}
}

func TestFilterSupportedResources_UserDenyMergedWithDefault(t *testing.T) {
	pod := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	secrets := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	deploy := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	supported := map[schema.GroupVersionResource]DiscoveredAPIResource{
		pod:     testDiscovered(pod, ObjectScopeNamespaced),
		secrets: testDiscovered(secrets, ObjectScopeNamespaced),
		deploy:  testDiscovered(deploy, ObjectScopeNamespaced),
	}

	userDeny := []Resource{{ApiGroups: []string{""}, Resources: []string{"secrets"}}}
	result := FilterSupportedResources(supported, userDeny, nil, slog.Default())

	resultMap := make(map[schema.GroupVersionResource]struct{})
	for _, desc := range result {
		resultMap[desc.GVR] = struct{}{}
	}

	if _, ok := resultMap[secrets]; ok {
		t.Error("expected secrets to be denied by user deny list")
	}
	if len(result) != 2 {
		t.Errorf("expected 2 results (pods, deployments), got %d", len(result))
	}
}

func TestFilterSupportedResources_AllowListRestricts(t *testing.T) {
	pod := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	cms := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	deploy := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	supported := map[schema.GroupVersionResource]DiscoveredAPIResource{
		pod:    testDiscovered(pod, ObjectScopeNamespaced),
		cms:    testDiscovered(cms, ObjectScopeNamespaced),
		deploy: testDiscovered(deploy, ObjectScopeNamespaced),
	}

	allow := []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}}
	result := FilterSupportedResources(supported, nil, allow, slog.Default())

	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].GVR.Resource != "pods" {
		t.Errorf("expected pods, got %s", result[0].GVR.Resource)
	}
}
