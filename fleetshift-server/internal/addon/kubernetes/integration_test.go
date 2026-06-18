//go:build integration

package kubernetes_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kind/pkg/cluster"
	kindlog "sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubeaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/testharness"
)

const clusterName = "fleetshift-k8s-e2e"

var fixture *clusterFixture

type clusterFixture struct {
	apiServer      string
	caCert         string
	saToken        string
	adminRestCfg   *rest.Config
	adminDynClient dynamic.Interface
	adminK8s       *kubernetes.Clientset
}

func TestMain(m *testing.M) {
	provider := cluster.NewProvider()
	if _, err := provider.List(); err != nil {
		fmt.Fprintf(os.Stderr, "container runtime not available, skipping: %v\n", err)
		os.Exit(0)
	}

	// Clean up stale cluster from previous runs.
	_ = provider.Delete(clusterName, "")

	// Create cluster via the kind addon's delivery pipeline.
	reporter := newChannelReporter()
	kindAgent := kindaddon.NewAgent(reporter, func(logger kindlog.Logger) kindaddon.ClusterProvider {
		return cluster.NewProvider(cluster.ProviderWithLogger(logger))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   "kind-e2e-setup",
		Type: kindaddon.TargetType,
	})
	manifests := []domain.Manifest{{
		ResourceType: kindaddon.ClusterResourceType,
		Raw:          json.RawMessage(`{"name":"` + clusterName + `"}`),
	}}

	if err := kindAgent.Deliver(ctx, target, "e2e-setup", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		fmt.Fprintf(os.Stderr, "kind Deliver: %v\n", err)
		os.Exit(1)
	}

	var result domain.DeliveryResult
	select {
	case result = <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			fmt.Fprintf(os.Stderr, "kind delivery state = %q: %s\n", result.State, result.Message)
			_ = provider.Delete(clusterName, "")
			os.Exit(1)
		}
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "timed out waiting for kind cluster")
		_ = provider.Delete(clusterName, "")
		os.Exit(1)
	}

	// Extract target properties from provisioned target.
	if len(result.ProvisionedTargets) == 0 || len(result.ProducedSecrets) == 0 {
		fmt.Fprintln(os.Stderr, "kind delivery missing provisioned targets or secrets")
		_ = provider.Delete(clusterName, "")
		os.Exit(1)
	}
	pt := result.ProvisionedTargets[0]

	// Get admin kubeconfig for direct cluster access.
	kcStr, err := provider.KubeConfig(clusterName, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "KubeConfig: %v\n", err)
		_ = provider.Delete(clusterName, "")
		os.Exit(1)
	}
	adminCfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(kcStr))
	if err != nil {
		fmt.Fprintf(os.Stderr, "RESTConfigFromKubeConfig: %v\n", err)
		_ = provider.Delete(clusterName, "")
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(adminCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dynamic client: %v\n", err)
		_ = provider.Delete(clusterName, "")
		os.Exit(1)
	}

	adminK8s, err := kubernetes.NewForConfig(adminCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kubernetes client: %v\n", err)
		_ = provider.Delete(clusterName, "")
		os.Exit(1)
	}

	fixture = &clusterFixture{
		apiServer:      pt.Properties["api_server"],
		caCert:         pt.Properties["ca_cert"],
		saToken:        string(result.ProducedSecrets[0].Value),
		adminRestCfg:   adminCfg,
		adminDynClient: dynClient,
		adminK8s:       adminK8s,
	}

	code := m.Run()
	_ = provider.Delete(clusterName, "")
	os.Exit(code)
}

type e2eFixture struct {
	harness   *testharness.Harness
	k8sMgr    *kubeaddon.Manager
	dynClient dynamic.Interface
	typedK8s  *kubernetes.Clientset
	namespace string
	targetID  domain.TargetID
	auth      domain.DeliveryAuth
}

func setupE2E(t *testing.T) *e2eFixture {
	t.Helper()

	h := testharness.New(t)
	inventoryWriter := application.NewInventoryWriteService(h.Store)

	k8sMgr := kubeaddon.NewManager(
		h.Store,
		nil, // vault — SA token is passed directly
		inventoryWriter,
		h.Reporter,
		nil, // keyResolver
		nil, // httpClient
		slog.Default(),
	)

	ctx := context.Background()
	targetID := domain.TargetID("k8s-e2e")

	if err := h.AddonMgr.Enable(ctx, kubeaddon.Descriptor()); err != nil {
		t.Fatalf("Enable kubernetes addon: %v", err)
	}
	if err := h.AddonMgr.Connect(ctx, "kubernetes", application.ConnectInput{
		Agent: k8sMgr,
	}); err != nil {
		t.Fatalf("Connect kubernetes addon: %v", err)
	}

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   targetID,
		Type: kubeaddon.TargetType,
		Name: "E2E Kind Cluster",
		Properties: map[string]string{
			"api_server":            fixture.apiServer,
			"ca_cert":               fixture.caCert,
			"service_account_token": fixture.saToken,
		},
	})

	if err := h.Targets.Register(ctx, target); err != nil {
		t.Fatalf("Register target: %v", err)
	}
	if err := k8sMgr.HandleTargetReady(ctx, target); err != nil {
		t.Fatalf("HandleTargetReady: %v", err)
	}

	// Create a test-specific namespace.
	ns := "e2e-" + strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(ns) > 63 {
		ns = ns[:63]
	}
	nsObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata":   map[string]any{"name": ns},
		},
	}
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	_, err := fixture.adminDynClient.Resource(nsGVR).Create(ctx, nsObj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create namespace %s: %v", ns, err)
	}

	t.Cleanup(func() {
		k8sMgr.StopAll()
		_ = fixture.adminDynClient.Resource(nsGVR).Delete(context.Background(), ns, metav1.DeleteOptions{})
	})

	return &e2eFixture{
		harness:   h,
		k8sMgr:    k8sMgr,
		dynClient: fixture.adminDynClient,
		typedK8s:  fixture.adminK8s,
		namespace: ns,
		targetID:  targetID,
		auth:      domain.DeliveryAuth{Token: domain.RawToken(fixture.saToken)},
	}
}

// awaitInventoryMatch polls until the predicate returns true.
func awaitInventoryMatch(t *testing.T, store domain.Store, predicate func([]domain.InventoryItem) bool, timeout time.Duration) []domain.InventoryItem {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		tx, err := store.BeginReadOnly(context.Background())
		if err != nil {
			t.Fatalf("BeginReadOnly: %v", err)
		}
		items, err := tx.Inventory().List(context.Background())
		tx.Rollback()
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if predicate(items) {
			return items
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for inventory match (%d items)", len(items))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// awaitNoInventoryItem polls until the item no longer exists.
func awaitNoInventoryItem(t *testing.T, store domain.Store, id domain.InventoryItemID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		tx, err := store.BeginReadOnly(context.Background())
		if err != nil {
			t.Fatalf("BeginReadOnly: %v", err)
		}
		_, err = tx.Inventory().Get(context.Background(), id)
		tx.Rollback()
		if err != nil {
			return // item gone
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for item %s to disappear", id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// awaitEdgeFrom polls until at least one outgoing edge from sourceUID
// matches the predicate, returning all edges from that source.
func awaitEdgeFrom(t *testing.T, store domain.Store, targetID domain.TargetID, sourceUID string, predicate func([]domain.InventoryEdge) bool, timeout time.Duration) []domain.InventoryEdge {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		edges := queryEdgesFrom(t, store, targetID, sourceUID)
		if predicate(edges) {
			return edges
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for edge match from source %s (%d edges)", sourceUID, len(edges))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// queryEdgesFrom returns outgoing edges from a source UID.
func queryEdgesFrom(t *testing.T, store domain.Store, targetID domain.TargetID, sourceUID string) []domain.InventoryEdge {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()
	edges, err := tx.Edges().ListBySourceUID(context.Background(), targetID, sourceUID)
	if err != nil {
		t.Fatalf("ListBySourceUID: %v", err)
	}
	return edges
}

// queryEdgesTo returns incoming edges to a destination UID.
func queryEdgesTo(t *testing.T, store domain.Store, targetID domain.TargetID, destUID string) []domain.InventoryEdge {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()
	edges, err := tx.Edges().ListByDestUID(context.Background(), targetID, destUID)
	if err != nil {
		t.Fatalf("ListByDestUID: %v", err)
	}
	return edges
}

// parseObserved unmarshals the Observed field of an inventory item.
func parseObserved(t *testing.T, item domain.InventoryItem) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(item.Observed(), &m); err != nil {
		t.Fatalf("unmarshal observed for %s: %v", item.ID(), err)
	}
	return m
}

// uidFromItemID extracts the UID from an inventory item ID of the form "targetID/UID".
func uidFromItemID(id domain.InventoryItemID) string {
	parts := strings.SplitN(string(id), "/", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return string(id)
}

// findUID returns the UID for an item matching the given type and name.
func findUID(t *testing.T, items []domain.InventoryItem, invType domain.InventoryType, name string) string {
	t.Helper()
	for _, item := range items {
		if item.Type() == invType && item.Name() == name {
			return uidFromItemID(item.ID())
		}
	}
	t.Fatalf("item not found: type=%s name=%s", invType, name)
	return ""
}

// awaitBootstrap waits for v1/Node to appear in inventory.
func awaitBootstrap(t *testing.T, f *e2eFixture) {
	t.Helper()
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Node" {
				return true
			}
		}
		return false
	}, 60*time.Second)
}

// TestE2E_ClusterBootstrap verifies that cluster bootstrap inventory is indexed.
func TestE2E_ClusterBootstrap(t *testing.T) {
	f := setupE2E(t)

	// Wait for nodes and core namespaces to appear in inventory.
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		var hasNode bool
		nsNames := map[string]bool{}
		for _, item := range items {
			if item.Type() == "v1/Node" {
				hasNode = true
			}
			if item.Type() == "v1/Namespace" {
				nsNames[item.Name()] = true
			}
		}
		return hasNode && nsNames["default"] && nsNames["kube-system"] && nsNames["kube-public"]
	}, 60*time.Second)

	// Verify nodes have enriched fields.
	var foundNode bool
	for _, item := range items {
		if item.Type() != "v1/Node" {
			continue
		}
		foundNode = true
		observed := parseObserved(t, item)
		if _, ok := observed["kubeletVersion"]; !ok {
			t.Error("node missing kubeletVersion")
		}
		if _, ok := observed["role"]; !ok {
			t.Error("node missing role")
		}
		if len(item.Conditions()) == 0 {
			t.Error("node missing conditions")
		}
	}
	if !foundNode {
		t.Fatal("no Node inventory items found")
	}
}

// TestE2E_DeployWorkload verifies deploying a workload via the platform pipeline.
func TestE2E_DeployWorkload(t *testing.T) {
	f := setupE2E(t)

	// Wait for initial indexing to settle.
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Node" {
				return true
			}
		}
		return false
	}, 60*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Deploy nginx via the platform pipeline.
	manifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {"name": "nginx-e2e", "namespace": "%s"},
		"spec": {
			"replicas": 1,
			"selector": {"matchLabels": {"app": "nginx-e2e"}},
			"template": {
				"metadata": {"labels": {"app": "nginx-e2e"}},
				"spec": {"containers": [{"name": "nginx", "image": "nginx:alpine"}]}
			}
		}
	}`, f.namespace))

	_, err := f.harness.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:   "nginx-e2e-deploy",
		Auth: f.auth,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ResourceType: kubeaddon.ManifestResourceType,
				Raw:          manifest,
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{f.targetID},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	// Wait for Pod to appear (proves Deployment → ReplicaSet → Pod chain was indexed).
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Pod" && item.Name() != "" && strings.HasPrefix(item.Name(), "nginx-e2e-") {
				observed := map[string]any{}
				_ = json.Unmarshal(item.Observed(), &observed)
				if phase, _ := observed["phase"].(string); phase == "Running" {
					return true
				}
			}
		}
		return false
	}, 90*time.Second)

	// Find the Pod, ReplicaSet, Deployment UIDs.
	var podUID, rsUID, deployUID, nodeUID string
	for _, item := range items {
		switch {
		case item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "nginx-e2e-"):
			podUID = uidFromItemID(item.ID())
		case item.Type() == "apps/v1/ReplicaSet" && strings.HasPrefix(item.Name(), "nginx-e2e-"):
			rsUID = uidFromItemID(item.ID())
		case item.Type() == "apps/v1/Deployment" && item.Name() == "nginx-e2e":
			deployUID = uidFromItemID(item.ID())
		}
	}

	if podUID == "" || rsUID == "" || deployUID == "" {
		t.Fatalf("missing resources: pod=%q rs=%q deploy=%q", podUID, rsUID, deployUID)
	}

	// Wait for ownedBy edges (edges arrive via flush, may lag items).
	podEdges := awaitEdgeFrom(t, f.harness.Store, f.targetID, podUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == "ownedBy" && e.DestUID == rsUID {
				return true
			}
		}
		return false
	}, 30*time.Second)

	for _, e := range podEdges {
		if e.EdgeType == "runsOn" {
			nodeUID = e.DestUID
		}
	}
	if nodeUID == "" {
		t.Error("missing runsOn edge: Pod→Node")
	}

	awaitEdgeFrom(t, f.harness.Store, f.targetID, rsUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == "ownedBy" && e.DestUID == deployUID {
				return true
			}
		}
		return false
	}, 30*time.Second)
}

// TestE2E_PodAttachmentEdges verifies pod attachment edges (ConfigMap, Secret).
func TestE2E_PodAttachmentEdges(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmManifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": {"name": "e2e-cm", "namespace": "%s"},
		"data": {"key": "value"}
	}`, f.namespace))

	secretManifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": {"name": "e2e-secret", "namespace": "%s"},
		"stringData": {"password": "test"}
	}`, f.namespace))

	podManifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": {"name": "e2e-attach-pod", "namespace": "%s"},
		"spec": {
			"containers": [{
				"name": "busybox", "image": "busybox:latest",
				"command": ["sleep", "3600"],
				"env": [{"name": "SECRET_VAL", "valueFrom": {"secretKeyRef": {"name": "e2e-secret", "key": "password"}}}]
			}],
			"volumes": [{"name": "cm-vol", "configMap": {"name": "e2e-cm"}}]
		}
	}`, f.namespace))

	_, err := f.harness.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:   "attach-deploy",
		Auth: f.auth,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{ResourceType: kubeaddon.ManifestResourceType, Raw: cmManifest},
				{ResourceType: kubeaddon.ManifestResourceType, Raw: secretManifest},
				{ResourceType: kubeaddon.ManifestResourceType, Raw: podManifest},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{f.targetID},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	// Wait for the pod, configmap, and secret to all appear.
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		var pod, cm, secret bool
		for _, item := range items {
			switch {
			case item.Type() == "v1/Pod" && item.Name() == "e2e-attach-pod":
				pod = true
			case item.Type() == "v1/ConfigMap" && item.Name() == "e2e-cm":
				cm = true
			case item.Type() == "v1/Secret" && item.Name() == "e2e-secret":
				secret = true
			}
		}
		return pod && cm && secret
	}, 90*time.Second)

	podUID := findUID(t, items, "v1/Pod", "e2e-attach-pod")
	cmUID := findUID(t, items, "v1/ConfigMap", "e2e-cm")
	secretUID := findUID(t, items, "v1/Secret", "e2e-secret")

	// Wait for attachment edges (edges arrive via flush, may lag items).
	edges := awaitEdgeFrom(t, f.harness.Store, f.targetID, podUID, func(edges []domain.InventoryEdge) bool {
		var hasCM, hasSecret bool
		for _, e := range edges {
			if e.EdgeType == "attachedTo" && e.DestUID == cmUID {
				hasCM = true
			}
			if e.EdgeType == "attachedTo" && e.DestUID == secretUID {
				hasSecret = true
			}
		}
		return hasCM && hasSecret
	}, 30*time.Second)

	var hasCM, hasSecret bool
	for _, e := range edges {
		if e.EdgeType == "attachedTo" && e.DestUID == cmUID {
			hasCM = true
		}
		if e.EdgeType == "attachedTo" && e.DestUID == secretUID {
			hasSecret = true
		}
	}
	if !hasCM {
		t.Error("missing attachedTo edge: Pod→ConfigMap")
	}
	if !hasSecret {
		t.Error("missing attachedTo edge: Pod→Secret")
	}
}

// TestE2E_ServiceSelectorEdges verifies service selector edges to pods.
func TestE2E_ServiceSelectorEdges(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	deployManifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": {"name": "e2e-svc-deploy", "namespace": "%s"},
		"spec": {
			"replicas": 1,
			"selector": {"matchLabels": {"app": "e2e-svc"}},
			"template": {
				"metadata": {"labels": {"app": "e2e-svc"}},
				"spec": {"containers": [{"name": "nginx", "image": "nginx:alpine"}]}
			}
		}
	}`, f.namespace))

	svcManifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "v1", "kind": "Service",
		"metadata": {"name": "e2e-svc", "namespace": "%s"},
		"spec": {
			"selector": {"app": "e2e-svc"},
			"ports": [{"port": 80, "targetPort": 80}]
		}
	}`, f.namespace))

	_, err := f.harness.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:   "svc-deploy",
		Auth: f.auth,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{ResourceType: kubeaddon.ManifestResourceType, Raw: deployManifest},
				{ResourceType: kubeaddon.ManifestResourceType, Raw: svcManifest},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{f.targetID},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	// Wait for pod to be running AND service to appear.
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		var podRunning, svcFound bool
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-svc-deploy-") {
				observed := map[string]any{}
				_ = json.Unmarshal(item.Observed(), &observed)
				if phase, _ := observed["phase"].(string); phase == "Running" {
					podRunning = true
				}
			}
			if item.Type() == "v1/Service" && item.Name() == "e2e-svc" {
				svcFound = true
			}
		}
		return podRunning && svcFound
	}, 90*time.Second)

	svcUID := findUID(t, items, "v1/Service", "e2e-svc")
	awaitEdgeFrom(t, f.harness.Store, f.targetID, svcUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == "selects" && e.DestKind == "Pod" {
				return true
			}
		}
		return false
	}, 30*time.Second)
}

// TestE2E_PVCPVEdge verifies PVC→PV attachment edge.
func TestE2E_PVCPVEdge(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	ctx := context.Background()
	pvGVR := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	pvcGVR := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}

	pv := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolume",
		"metadata": map[string]any{"name": "e2e-pv"},
		"spec": map[string]any{
			"capacity":         map[string]any{"storage": "1Gi"},
			"accessModes":      []any{"ReadWriteOnce"},
			"hostPath":         map[string]any{"path": "/tmp/e2e-pv"},
			"storageClassName": "manual",
		},
	}}
	_, err := f.dynClient.Resource(pvGVR).Create(ctx, pv, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create PV: %v", err)
	}
	t.Cleanup(func() {
		_ = f.dynClient.Resource(pvGVR).Delete(context.Background(), "e2e-pv", metav1.DeleteOptions{})
	})

	pvc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "e2e-pvc", "namespace": f.namespace},
		"spec": map[string]any{
			"accessModes":      []any{"ReadWriteOnce"},
			"resources":        map[string]any{"requests": map[string]any{"storage": "1Gi"}},
			"volumeName":       "e2e-pv",
			"storageClassName": "manual",
		},
	}}
	_, err = f.dynClient.Resource(pvcGVR).Namespace(f.namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create PVC: %v", err)
	}

	// Wait for both PVC and PV to appear in inventory.
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		var pvc, pv bool
		for _, item := range items {
			if item.Type() == "v1/PersistentVolumeClaim" && item.Name() == "e2e-pvc" {
				pvc = true
			}
			if item.Type() == "v1/PersistentVolume" && item.Name() == "e2e-pv" {
				pv = true
			}
		}
		return pvc && pv
	}, 60*time.Second)

	pvcUID := findUID(t, items, "v1/PersistentVolumeClaim", "e2e-pvc")
	pvUID := findUID(t, items, "v1/PersistentVolume", "e2e-pv")

	// Wait for attachment edge (edges arrive via flush, may lag items).
	awaitEdgeFrom(t, f.harness.Store, f.targetID, pvcUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == "attachedTo" && e.DestUID == pvUID {
				return true
			}
		}
		return false
	}, 30*time.Second)
}

// TestE2E_UpdateReindex verifies that resource updates trigger re-indexing.
func TestE2E_UpdateReindex(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	manifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": {"name": "e2e-scale", "namespace": "%s"},
		"spec": {
			"replicas": 1,
			"selector": {"matchLabels": {"app": "e2e-scale"}},
			"template": {
				"metadata": {"labels": {"app": "e2e-scale"}},
				"spec": {"containers": [{"name": "nginx", "image": "nginx:alpine"}]}
			}
		}
	}`, f.namespace))

	_, err := f.harness.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:   "scale-deploy",
		Auth: f.auth,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{ResourceType: kubeaddon.ManifestResourceType, Raw: manifest}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{f.targetID},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	// Wait for 1 pod.
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		count := 0
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-scale-") {
				count++
			}
		}
		return count >= 1
	}, 90*time.Second)

	// Scale to 2 via dynamic client.
	deployGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	dep, err := f.dynClient.Resource(deployGVR).Namespace(f.namespace).Get(ctx, "e2e-scale", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	_ = unstructured.SetNestedField(dep.Object, int64(2), "spec", "replicas")
	_, err = f.dynClient.Resource(deployGVR).Namespace(f.namespace).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update deployment: %v", err)
	}

	// Wait for 2 pods.
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		count := 0
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-scale-") {
				count++
			}
		}
		return count >= 2
	}, 90*time.Second)

	// Wait for both pods to have ownedBy edges (edges arrive via flush, may lag items).
	for _, item := range items {
		if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-scale-") {
			uid := uidFromItemID(item.ID())
			name := item.Name()
			awaitEdgeFrom(t, f.harness.Store, f.targetID, uid, func(edges []domain.InventoryEdge) bool {
				for _, e := range edges {
					if e.EdgeType == "ownedBy" {
						return true
					}
				}
				return false
			}, 30*time.Second)
			_ = name // used for debug context in awaitEdgeFrom timeout
		}
	}
}

// TestE2E_RemoveCleanup verifies that resource deletions are detected and cleaned up.
func TestE2E_RemoveCleanup(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	manifest := json.RawMessage(fmt.Sprintf(`{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": {"name": "e2e-remove", "namespace": "%s"},
		"spec": {
			"replicas": 1,
			"selector": {"matchLabels": {"app": "e2e-remove"}},
			"template": {
				"metadata": {"labels": {"app": "e2e-remove"}},
				"spec": {"containers": [{"name": "nginx", "image": "nginx:alpine"}]}
			}
		}
	}`, f.namespace))

	_, err := f.harness.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:   "remove-deploy",
		Auth: f.auth,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{ResourceType: kubeaddon.ManifestResourceType, Raw: manifest}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{f.targetID},
		},
	})
	if err != nil {
		t.Fatalf("Create deployment: %v", err)
	}

	// Wait for pod to appear.
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-remove-") {
				return true
			}
		}
		return false
	}, 90*time.Second)

	// Delete via dynamic client (the indexer should detect the deletion).
	deployGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	err = f.dynClient.Resource(deployGVR).Namespace(f.namespace).Delete(ctx, "e2e-remove", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete deployment: %v", err)
	}

	// Wait for deployment to disappear from inventory.
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "apps/v1/Deployment" && item.Name() == "e2e-remove" {
				return false
			}
		}
		return true
	}, 90*time.Second)

	// Verify pods are also gone.
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-remove-") {
				return false
			}
		}
		return true
	}, 30*time.Second)
}

// TestE2E_CRDLifecycle verifies CRD and custom resource indexing.
func TestE2E_CRDLifecycle(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	ctx := context.Background()
	crdGVR := schema.GroupVersionResource{
		Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
	}

	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "widgets.test.fleetshift.io"},
		"spec": map[string]any{
			"group": "test.fleetshift.io",
			"names": map[string]any{
				"kind":     "Widget",
				"plural":   "widgets",
				"singular": "widget",
			},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name":    "v1",
				"served":  true,
				"storage": true,
				"schema": map[string]any{
					"openAPIV3Schema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"spec": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"color": map[string]any{"type": "string"},
								},
							},
						},
					},
				},
			}},
		},
	}}

	_, err := f.dynClient.Resource(crdGVR).Create(ctx, crd, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create CRD: %v", err)
	}
	t.Cleanup(func() {
		_ = f.dynClient.Resource(crdGVR).Delete(context.Background(), "widgets.test.fleetshift.io", metav1.DeleteOptions{})
	})

	// Wait for CRD to be established.
	time.Sleep(5 * time.Second)

	// Create a Widget CR.
	widgetGVR := schema.GroupVersionResource{
		Group: "test.fleetshift.io", Version: "v1", Resource: "widgets",
	}
	widget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "test.fleetshift.io/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "test-widget", "namespace": f.namespace},
		"spec":       map[string]any{"color": "blue"},
	}}
	_, err = f.dynClient.Resource(widgetGVR).Namespace(f.namespace).Create(ctx, widget, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create Widget: %v", err)
	}

	// Wait for Widget to appear in inventory.
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "test.fleetshift.io/v1/Widget" && item.Name() == "test-widget" {
				return true
			}
		}
		return false
	}, 60*time.Second)
}

// TestE2E_DefaultDenyList verifies default deny list filters out unwanted resources.
func TestE2E_DefaultDenyList(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	// Give the indexer time to process all resources.
	time.Sleep(5 * time.Second)

	tx, err := f.harness.Store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	items, err := tx.Inventory().List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	deniedTypes := map[domain.InventoryType]bool{
		"v1/Event":                          true,
		"events.k8s.io/v1/Event":            true,
		"coordination.k8s.io/v1/Lease":      true,
		"v1/Endpoints":                      true,
		"discovery.k8s.io/v1/EndpointSlice": true,
	}

	for _, item := range items {
		if deniedTypes[item.Type()] {
			t.Errorf("denied resource type found in inventory: %s (name: %s)", item.Type(), item.Name())
		}
	}
}

// TestE2E_TargetTermination verifies that target termination cleans up all inventory.
func TestE2E_TargetTermination(t *testing.T) {
	f := setupE2E(t)
	awaitBootstrap(t, f)

	// Verify we have inventory.
	tx, _ := f.harness.Store.BeginReadOnly(context.Background())
	items, _ := tx.Inventory().List(context.Background())
	tx.Rollback()
	if len(items) == 0 {
		t.Fatal("expected inventory items before termination")
	}

	ctx := context.Background()
	if err := f.k8sMgr.HandleTargetTerminated(ctx, f.targetID); err != nil {
		t.Fatalf("HandleTargetTerminated: %v", err)
	}

	// Verify all inventory is gone.
	tx, _ = f.harness.Store.BeginReadOnly(context.Background())
	items, _ = tx.Inventory().List(context.Background())
	tx.Rollback()

	for _, item := range items {
		if item.TargetID() == f.targetID {
			t.Errorf("inventory item still present after termination: %s", item.ID())
		}
	}
}
