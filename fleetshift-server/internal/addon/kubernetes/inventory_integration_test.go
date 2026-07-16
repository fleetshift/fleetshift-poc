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
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kind/pkg/cluster"
	kindlog "sigs.k8s.io/kind/pkg/log"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	kubeaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

const clusterName = "fleetshift-k8s-e2e"

// kindName is the ownership-encoded kind/docker cluster name (fs--{id}).
const kindName = "fs--" + clusterName

var fixture *clusterFixture

func TestMain(m *testing.M) {
	provider := cluster.NewProvider()
	if _, err := provider.List(); err != nil {
		fmt.Fprintf(os.Stderr, "container runtime not available, skipping: %v\n", err)
		os.Exit(0)
	}

	_ = provider.Delete(kindName, "")
	_ = provider.Delete(clusterName, "") // pre-ownership bare name from older runs

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
		ManifestType: kindaddon.ClusterManifestType,
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
			_ = provider.Delete(kindName, "")
			os.Exit(1)
		}
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "timed out waiting for kind cluster")
		_ = provider.Delete(kindName, "")
		os.Exit(1)
	}

	if len(result.ProvisionedTargets) == 0 || len(result.ProducedSecrets) == 0 {
		fmt.Fprintln(os.Stderr, "kind delivery missing provisioned targets or secrets")
		_ = provider.Delete(kindName, "")
		os.Exit(1)
	}
	pt := result.ProvisionedTargets[0]

	kcStr, err := provider.KubeConfig(kindName, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "KubeConfig: %v\n", err)
		_ = provider.Delete(kindName, "")
		os.Exit(1)
	}
	adminCfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(kcStr))
	if err != nil {
		fmt.Fprintf(os.Stderr, "RESTConfigFromKubeConfig: %v\n", err)
		_ = provider.Delete(kindName, "")
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(adminCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dynamic client: %v\n", err)
		_ = provider.Delete(kindName, "")
		os.Exit(1)
	}

	fixture = &clusterFixture{
		apiServer:      pt.Properties[kubeaddon.PropAPIServer],
		caCert:         pt.Properties[kubeaddon.PropCACert],
		saToken:        string(result.ProducedSecrets[0].Value),
		adminDynClient: dynClient,
	}

	code := m.Run()
	_ = provider.Delete(kindName, "")
	os.Exit(code)
}

// Test_ClusterBootstrap verifies initial cluster indexing produces core
// inventory under ObjectResourceType: nodes with enriched fields, system
// namespaces, identity labels, and a diversity of resource kinds.
func Test_ClusterBootstrap(t *testing.T) {
	f := setupE2E(t, skipBootstrapWait())

	expectedKinds := []string{"Node", "Namespace", "ServiceAccount", "ConfigMap", "Secret", "Service"}
	objs := awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		kindSet := map[string]bool{}
		nsNames := map[string]bool{}
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			kind := invKind(inv)
			kindSet[kind] = true
			if kind == "Namespace" {
				nsNames[invMetaName(inv)] = true
			}
		}
		for _, k := range expectedKinds {
			if !kindSet[k] {
				return false
			}
		}
		return nsNames["default"] && nsNames["kube-system"] && nsNames["kube-public"]
	}, 60*time.Second)

	var foundNode bool
	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil || invKind(inv) != "Node" {
			continue
		}
		foundNode = true
		assertObjectIdentity(t, obj, f.clusterResourceName)
		extracted := parseExtracted(t, inv)
		if _, ok := extracted["kubeletVersion"]; !ok {
			t.Error("node missing extracted.kubeletVersion")
		}
		if _, ok := extracted["role"]; !ok {
			t.Error("node missing extracted.role")
		}
		if len(inv.Conditions()) == 0 {
			t.Error("node missing conditions")
		}
		if len(inv.Labels()) == 0 {
			t.Error("node missing localLabels")
		}
	}
	if !foundNode {
		t.Fatal("no Node inventory objects found")
	}
}

// Test_DeployWorkload deploys via the delivery agent and checks that the
// ownership chain (Deployment → ReplicaSet → Pod) is indexed as inventory.
// Topology edge persistence is out of scope; edge computation is covered
// by unit tests with a recording edge sink.
func Test_DeployWorkload(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "nginx-e2e-deploy", nginxDeployment("nginx-e2e", f.namespace, 1))

	objs := f.awaitRunningWorkload(t, "nginx-e2e", "nginx-e2e-")
	if findByKindNamePrefix(objs, "Pod", "nginx-e2e-") == nil {
		t.Fatal("missing Pod inventory")
	}
	if findByKindNamePrefix(objs, "ReplicaSet", "nginx-e2e-") == nil {
		t.Fatal("missing ReplicaSet inventory")
	}
	if findByKindName(objs, "Deployment", "nginx-e2e") == nil {
		t.Fatal("missing Deployment inventory")
	}
}

// Test_PodAttachmentResources verifies pods referencing ConfigMaps and
// Secrets are indexed alongside those resources.
func Test_PodAttachmentResources(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "attach-deploy",
		configMapManifest("e2e-cm", f.namespace, map[string]string{"key": "value"}),
		secretManifest("e2e-secret", f.namespace, map[string]string{"password": "test"}),
		attachmentPodManifest("e2e-attach-pod", f.namespace, "e2e-cm", "e2e-secret"),
	)

	f.awaitObjects(t,
		objectSpec{Kind: "Pod", Name: "e2e-attach-pod"},
		objectSpec{Kind: "ConfigMap", Name: "e2e-cm"},
		objectSpec{Kind: "Secret", Name: "e2e-secret"},
	)
}

// Test_ServiceAndSelectedPods verifies a Service and its selected Pods
// are both indexed.
func Test_ServiceAndSelectedPods(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "svc-deploy",
		nginxDeployment("e2e-svc", f.namespace, 1),
		serviceManifest("e2e-svc", f.namespace, map[string]string{"app": "e2e-svc"}, 80),
	)

	f.awaitRunningPod(t, "e2e-svc-")
	f.awaitObjects(t, objectSpec{Kind: "Service", Name: "e2e-svc"})
}

// Test_PVCPVIndexed verifies a PVC and PV are indexed when created.
func Test_PVCPVIndexed(t *testing.T) {
	f := setupE2E(t)

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

	f.awaitObjects(t,
		objectSpec{Kind: "PersistentVolumeClaim", Name: "e2e-pvc"},
		objectSpec{Kind: "PersistentVolume", Name: "e2e-pv"},
	)
}

// Test_UpdateReindex verifies WATCH detects mutations: scaling a
// deployment from 1 to 2 replicas indexes the new pod.
func Test_UpdateReindex(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "scale-deploy", nginxDeployment("e2e-scale", f.namespace, 1))
	f.awaitPodCount(t, "e2e-scale-", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

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

	f.awaitPodCount(t, "e2e-scale-", 2)
}

// Test_RemoveCleanup verifies the indexer detects external deletions.
func Test_RemoveCleanup(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "remove-deploy", nginxDeployment("e2e-remove", f.namespace, 1))
	f.awaitPodCount(t, "e2e-remove-", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	deployGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	err := f.dynClient.Resource(deployGVR).Namespace(f.namespace).Delete(ctx, "e2e-remove", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete deployment: %v", err)
	}

	f.awaitObjectGone(t, "Deployment", "e2e-remove")
	f.awaitObjectGoneByPrefix(t, "Pod", "e2e-remove-")
}

// Test_CRDLifecycle verifies CRD watcher reconciliation indexes custom
// resources with base-tier extraction.
func Test_CRDLifecycle(t *testing.T) {
	f := setupE2E(t)

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
				"kind": "Widget", "plural": "widgets", "singular": "widget",
			},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name": "v1", "served": true, "storage": true,
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

	time.Sleep(5 * time.Second)

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

	f.awaitObjects(t, objectSpec{Kind: "Widget", Name: "test-widget"})
}

// Test_DefaultDenyList verifies high-volume/transient types are not indexed.
func Test_DefaultDenyList(t *testing.T) {
	f := setupE2E(t)

	time.Sleep(5 * time.Second)

	objs := listInventory(t, f.store)
	deniedGVRs := map[string]bool{
		"core~v1~events":                     true,
		"events.k8s.io~v1~events":            true,
		"coordination.k8s.io~v1~leases":      true,
		"core~v1~endpoints":                  true,
		"discovery.k8s.io~v1~endpointslices": true,
	}

	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		gvr := invGVRKey(inv)
		if deniedGVRs[gvr] {
			t.Errorf("denied GVR found in inventory: %s (name: %s)", gvr, invMetaName(inv))
		}
	}
}

// Test_StopIndexerLeavesInventory verifies StopIndexer stops watching
// without deleting source-owned indexed inventory.
func Test_StopIndexerLeavesInventory(t *testing.T) {
	f := setupE2E(t)

	objs := listInventory(t, f.store)
	if len(objs) == 0 {
		t.Fatal("expected inventory objects before stop")
	}

	ctx := context.Background()
	if err := f.host.StopIndexer(ctx, f.target.ID()); err != nil {
		t.Fatalf("StopIndexer: %v", err)
	}
	if f.host.HasIndexer(f.targetID) {
		t.Fatal("expected indexer to be stopped")
	}

	remaining := 0
	prefix := string(f.clusterResourceName) + "/"
	for _, obj := range listInventory(t, f.store) {
		if strings.HasPrefix(string(obj.Name()), prefix) {
			remaining++
		}
	}
	if remaining == 0 {
		t.Fatal("expected indexed inventory to remain after StopIndexer")
	}
}

// Test_DeliveryRemoval verifies DeliveryAgent.Remove deletes applied
// manifests and the indexer drops them from inventory.
func Test_DeliveryRemoval(t *testing.T) {
	f := setupE2E(t)

	manifest := nginxDeployment("e2e-removal", f.namespace, 1)
	f.deliver(t, "removal-deploy", manifest)
	f.awaitRunningPod(t, "e2e-removal-")

	f.remove(t, manifest)

	f.awaitObjectGone(t, "Deployment", "e2e-removal")
	f.awaitObjectGoneByPrefix(t, "Pod", "e2e-removal-")
}

// Test_CRDDeletion verifies CRD delete behavior while indexing is
// running: deleting a custom resource removes it via watch DELETE /
// IsDelete. Deleting the CRD afterwards stops that GVR without a
// synthesized collection wipe — other GVRs remain indexed.
func Test_CRDDeletion(t *testing.T) {
	f := setupE2E(t)

	ctx := context.Background()
	crdGVR := schema.GroupVersionResource{
		Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
	}

	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "gadgets.test.fleetshift.io"},
		"spec": map[string]any{
			"group": "test.fleetshift.io",
			"names": map[string]any{
				"kind": "Gadget", "plural": "gadgets", "singular": "gadget",
			},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name": "v1", "served": true, "storage": true,
				"schema": map[string]any{
					"openAPIV3Schema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"spec": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"size": map[string]any{"type": "string"},
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

	time.Sleep(5 * time.Second)

	gadgetGVR := schema.GroupVersionResource{
		Group: "test.fleetshift.io", Version: "v1", Resource: "gadgets",
	}
	gadget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "test.fleetshift.io/v1",
		"kind":       "Gadget",
		"metadata":   map[string]any{"name": "test-gadget", "namespace": f.namespace},
		"spec":       map[string]any{"size": "large"},
	}}
	_, err = f.dynClient.Resource(gadgetGVR).Namespace(f.namespace).Create(ctx, gadget, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create Gadget: %v", err)
	}

	f.awaitObjects(t, objectSpec{Kind: "Gadget", Name: "test-gadget"})

	// Watch DELETE / IsDelete while the GVR is still being indexed.
	err = f.dynClient.Resource(gadgetGVR).Namespace(f.namespace).Delete(ctx, "test-gadget", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete Gadget: %v", err)
	}
	f.awaitObjectGone(t, "Gadget", "test-gadget")

	// GVR disappearance is non-destructive: stop the informer, leave
	// other collections alone (no DeleteCollection / collection wipe).
	err = f.dynClient.Resource(crdGVR).Delete(ctx, "gadgets.test.fleetshift.io", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete CRD: %v", err)
	}

	foundNode := false
	for _, obj := range listInventory(t, f.store) {
		inv := obj.Inventory()
		if inv != nil && invKind(inv) == "Node" {
			foundNode = true
			break
		}
	}
	if !foundNode {
		t.Fatal("expected Node inventory to remain after non-destructive CRD/GVR removal")
	}
}

// Test_EnrichedFields verifies schema-driven extracted fields on real
// Deployments, ReplicaSets, and Pods.
func Test_EnrichedFields(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "enriched-deploy", nginxDeployment("e2e-enriched", f.namespace, 1))
	objs := f.awaitRunningWorkload(t, "e2e-enriched", "e2e-enriched-")

	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		if invKind(inv) == "Deployment" && invMetaName(inv) == "e2e-enriched" {
			extracted := parseExtracted(t, inv)
			if v, _ := extracted["replicas"].(float64); v != 1 {
				t.Errorf("deployment replicas = %v, want 1", extracted["replicas"])
			}
			if _, ok := extracted["availableReplicas"]; !ok {
				t.Error("deployment missing availableReplicas")
			}
			if len(inv.Conditions()) == 0 {
				t.Error("deployment missing conditions")
			}
		}
	}

	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		if invKind(inv) == "ReplicaSet" && strings.HasPrefix(invMetaName(inv), "e2e-enriched-") {
			extracted := parseExtracted(t, inv)
			if v, _ := extracted["replicas"].(float64); v != 1 {
				t.Errorf("replicaset replicas = %v, want 1", extracted["replicas"])
			}
			if _, ok := extracted["readyReplicas"]; !ok {
				t.Error("replicaset missing readyReplicas")
			}
			break
		}
	}

	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		if invKind(inv) == "Pod" && strings.HasPrefix(invMetaName(inv), "e2e-enriched-") {
			extracted := parseExtracted(t, inv)
			if phase, _ := extracted["phase"].(string); phase != "Running" {
				t.Errorf("pod phase = %q, want Running", phase)
			}
			if _, ok := extracted["podIP"]; !ok {
				t.Error("pod missing podIP")
			}
			if _, ok := extracted["containerImages"]; !ok {
				t.Error("pod missing containerImages")
			}
			if _, ok := extracted["status"]; !ok {
				t.Error("pod missing computed status")
			}
			if len(inv.Conditions()) == 0 {
				t.Error("pod missing conditions")
			}
			break
		}
	}
}

// Test_LabelIndexing verifies Kubernetes object metadata.labels are
// projected onto inventory localLabels through the indexing pipeline.
func Test_LabelIndexing(t *testing.T) {
	f := setupE2E(t)

	f.deliver(t, "label-deploy", nginxDeployment("e2e-labels", f.namespace, 1))
	objs := f.awaitRunningPod(t, "e2e-labels-")

	var foundPod bool
	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		if invKind(inv) == "Pod" && strings.HasPrefix(invMetaName(inv), "e2e-labels-") {
			foundPod = true
			assertObjectIdentity(t, obj, f.clusterResourceName)
			if invMetaName(inv) == "" {
				t.Fatal("pod missing observation metadata.name")
			}
			if inv.Labels()["app"] != "e2e-labels" {
				t.Errorf("pod localLabels[app] = %q, want e2e-labels", inv.Labels()["app"])
			}
			break
		}
	}
	if !foundPod {
		t.Fatal("no pod found with prefix e2e-labels-")
	}

	for _, obj := range objs {
		inv := obj.Inventory()
		if inv == nil {
			continue
		}
		if invKind(inv) == "Node" {
			if _, ok := inv.Labels()["kubernetes.io/os"]; !ok {
				t.Error("node missing kubernetes.io/os localLabel")
			}
			return
		}
	}
	t.Fatal("no Node found to assert kubernetes.io/os localLabel")
}

// Test_EnsureIndexerIndexesTarget wires EnsureIndexer against the kind
// cluster and asserts Node inventory under the canonical
// ObjectResourceName shape.
func Test_EnsureIndexerIndexesTarget(t *testing.T) {
	if fixture == nil {
		t.Fatal("kind fixture not initialized")
	}

	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedKubernetesObjectType(t, store)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	reports := application.NewInventoryReportService(store)
	reporter := kubeaddon.NewDirectInventoryReporter(newE2EInventoryBackend(reports))
	host := kubeaddon.NewKubernetesInProcessIndexHost(
		runCtx,
		nil,
		reporter,
		kubeaddon.DefaultIndexerClients{},
		slog.Default(),
	)

	targetID := domain.TargetID("k8s-ensure-e2e")
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:    targetID,
		Type:  kubeaddon.TargetType,
		Name:  "EnsureIndexer E2E",
		State: domain.TargetStateReady,
		Properties: map[string]string{
			kubeaddon.PropAPIServer:           fixture.apiServer,
			kubeaddon.PropCACert:              fixture.caCert,
			kubeaddon.PropServiceAccountToken: fixture.saToken,
		},
	})

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

	cfg := kubeaddon.DefaultIndexConfig()
	cfg.BatchInterval = 200 * time.Millisecond
	clusterResourceName := domain.ResourceName("clusters/ensure-e2e")
	input, err := kubeaddon.NewIndexRuntimeInput(
		target.ID(),
		clusterResourceName,
		fixture.apiServer,
		fixture.caCert,
		[]byte(fixture.saToken),
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
	t.Cleanup(func() {
		_ = host.StopIndexer(context.Background(), targetID)
	})

	objs := awaitInventoryMatch(t, store, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv != nil && invKind(inv) == "Node" && strings.HasPrefix(string(obj.Name()), string(clusterResourceName)+"/") {
				return true
			}
		}
		return false
	}, 60*time.Second)

	var found bool
	for _, obj := range objs {
		inv := obj.Inventory()
		if inv != nil && invKind(inv) == "Node" && strings.HasPrefix(string(obj.Name()), string(clusterResourceName)+"/") {
			found = true
			assertObjectIdentity(t, obj, clusterResourceName)
		}
	}
	if !found {
		t.Fatal("expected Node inventory after EnsureIndexer")
	}
	if !host.HasIndexer(targetID) {
		t.Fatal("expected running indexer after EnsureIndexer")
	}
}

// storeTargetLister mirrors the serve composition adapter for kind e2e.
type storeTargetLister struct {
	store domain.Store
}

func (l storeTargetLister) ListTargets(ctx context.Context) ([]domain.TargetInfo, error) {
	tx, err := l.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return tx.Targets().List(ctx)
}

// ── Fixtures & setup ────────────────────────────────────────────────────

type clusterFixture struct {
	apiServer      string
	caCert         string
	saToken        string
	adminDynClient dynamic.Interface
}

type e2eFixture struct {
	store               domain.Store
	host                *kubeaddon.KubernetesInProcessIndexHost
	dynClient           dynamic.Interface
	namespace           string
	target              domain.TargetInfo
	targetID            domain.TargetID
	clusterResourceName domain.ResourceName
	auth                domain.DeliveryAuth
}

type setupOption func(*setupConfig)
type setupConfig struct{ skipBootstrap bool }

func skipBootstrapWait() setupOption {
	return func(c *setupConfig) { c.skipBootstrap = true }
}

// seedKubernetesObjectType registers the Kubernetes Object extension
// resource type so inventory writes succeed against a fresh test store.
// Previously lived in inventory_cleaner_test.go; restored here after
// that cleanup path was removed.
func seedKubernetesObjectType(t *testing.T, store domain.Store) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	sch := kubeaddon.InventorySchema()
	def := domain.NewExtensionResourceType(sch.ResourceType, domain.APIVersion(sch.Version), domain.CollectionID(sch.CollectionID), time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func setupE2E(t *testing.T, opts ...setupOption) *e2eFixture {
	t.Helper()

	setup := &setupConfig{}
	for _, o := range opts {
		o(setup)
	}

	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	seedKubernetesObjectType(t, store)

	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)

	reports := application.NewInventoryReportService(store)
	reporter := kubeaddon.NewDirectInventoryReporter(newE2EInventoryBackend(reports))

	host := kubeaddon.NewKubernetesInProcessIndexHost(
		runCtx,
		nil,
		reporter,
		kubeaddon.DefaultIndexerClients{},
		slog.Default(),
	)

	ctx := context.Background()
	targetID := domain.TargetID("k8s-e2e")
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   targetID,
		Type: kubeaddon.TargetType,
		Name: "E2E Kind Cluster",
		Properties: map[string]string{
			kubeaddon.PropAPIServer:           fixture.apiServer,
			kubeaddon.PropCACert:              fixture.caCert,
			kubeaddon.PropServiceAccountToken: fixture.saToken,
		},
	})

	indexCfg := kubeaddon.DefaultIndexConfig()
	indexCfg.BatchInterval = 200 * time.Millisecond
	clusterResourceName := domain.ResourceName("clusters/e2e")
	input, err := kubeaddon.NewIndexRuntimeInput(
		target.ID(),
		clusterResourceName,
		fixture.apiServer,
		fixture.caCert,
		[]byte(fixture.saToken),
		"",
		1,
		indexCfg,
	)
	if err != nil {
		t.Fatalf("NewIndexRuntimeInput: %v", err)
	}
	if err := host.EnsureIndexer(ctx, input); err != nil {
		t.Fatalf("EnsureIndexer: %v", err)
	}

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
	_, err = fixture.adminDynClient.Resource(nsGVR).Create(ctx, nsObj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create namespace %s: %v", ns, err)
	}

	t.Cleanup(func() {
		_ = host.StopIndexer(context.Background(), target.ID())
		_ = fixture.adminDynClient.Resource(nsGVR).Delete(context.Background(), ns, metav1.DeleteOptions{})
	})

	f := &e2eFixture{
		store:               store,
		host:                host,
		dynClient:           fixture.adminDynClient,
		namespace:           ns,
		target:              target,
		targetID:            targetID,
		clusterResourceName: clusterResourceName,
		auth:                domain.DeliveryAuth{Token: domain.RawToken(fixture.saToken)},
	}

	if !setup.skipBootstrap {
		awaitInventoryMatch(t, store, func(objs []*domain.ExtensionResource) bool {
			for _, obj := range objs {
				inv := obj.Inventory()
				if inv != nil && invKind(inv) == "Node" {
					return true
				}
			}
			return false
		}, 60*time.Second)
	}

	return f
}

// ── Delivery helpers ────────────────────────────────────────────────────

func (f *e2eFixture) deliver(t *testing.T, id string, manifests ...json.RawMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kubeManifests := make([]domain.Manifest, len(manifests))
	for i, m := range manifests {
		kubeManifests[i] = domain.Manifest{ManifestType: kubeaddon.ManifestManifestType, Raw: m}
	}

	reporter := newChannelReporter()
	agent := kubeaddon.NewDeliveryAgent(reporter)
	if err := agent.Deliver(ctx, f.target, domain.DeliveryID(id), kubeManifests, f.auth, nil, 1); err != nil {
		t.Fatalf("deliver %s: %v", id, err)
	}
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("deliver %s state = %q: %s", id, result.State, result.Message)
		}
	case <-ctx.Done():
		t.Fatalf("deliver %s: timed out", id)
	}
}

func (f *e2eFixture) remove(t *testing.T, manifests ...json.RawMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kubeManifests := make([]domain.Manifest, len(manifests))
	for i, m := range manifests {
		kubeManifests[i] = domain.Manifest{ManifestType: kubeaddon.ManifestManifestType, Raw: m}
	}
	reporter := newChannelReporter()
	agent := kubeaddon.NewDeliveryAgent(reporter)
	if err := agent.Remove(ctx, f.target, "e2e-removal", kubeManifests, f.auth, nil, 1); err != nil {
		t.Fatalf("remove: %v", err)
	}
	select {
	case result := <-reporter.done:
		if result.State != domain.DeliveryStateDelivered {
			t.Fatalf("remove state = %q: %s", result.State, result.Message)
		}
	case <-ctx.Done():
		t.Fatalf("remove: timed out")
	}
}

func nginxDeployment(name, ns string, replicas int) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"replicas": replicas,
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}},
				"spec": map[string]any{
					"containers": []any{map[string]any{
						"name": "nginx", "image": "nginx:alpine",
					}},
				},
			},
		},
	}
	b, _ := json.Marshal(obj)
	return b
}

func configMapManifest(name, ns string, data map[string]string) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"data":       data,
	}
	b, _ := json.Marshal(obj)
	return b
}

func secretManifest(name, ns string, stringData map[string]string) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"stringData": stringData,
	}
	b, _ := json.Marshal(obj)
	return b
}

func serviceManifest(name, ns string, selector map[string]string, port int) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"selector": selector,
			"ports":    []any{map[string]any{"port": port, "targetPort": port}},
		},
	}
	b, _ := json.Marshal(obj)
	return b
}

func attachmentPodManifest(name, ns, cmName, secretName string) json.RawMessage {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"containers": []any{map[string]any{
				"name": "busybox", "image": "busybox:latest",
				"command": []any{"sleep", "3600"},
				"env": []any{map[string]any{
					"name": "SECRET_VAL",
					"valueFrom": map[string]any{
						"secretKeyRef": map[string]any{"name": secretName, "key": "password"},
					},
				}},
			}},
			"volumes": []any{map[string]any{
				"name":      "cm-vol",
				"configMap": map[string]any{"name": cmName},
			}},
		},
	}
	b, _ := json.Marshal(obj)
	return b
}

// ── Await helpers ───────────────────────────────────────────────────────

type objectSpec struct {
	Kind string
	Name string
}

func invObservation(t *testing.T, inv *domain.InventoryResource) map[string]any {
	t.Helper()
	if inv == nil {
		t.Fatal("nil inventory")
	}
	obs := inv.Observation()
	if obs == nil {
		t.Fatal("nil observation")
	}
	var top map[string]any
	if err := json.Unmarshal(*obs, &top); err != nil {
		t.Fatalf("unmarshal observation: %v", err)
	}
	return top
}

func invKind(inv *domain.InventoryResource) string {
	if inv == nil || inv.Observation() == nil {
		return ""
	}
	var top map[string]any
	if err := json.Unmarshal(*inv.Observation(), &top); err != nil {
		return ""
	}
	kind, _ := top["kind"].(string)
	return kind
}

func invMetaName(inv *domain.InventoryResource) string {
	if inv == nil || inv.Observation() == nil {
		return ""
	}
	var top map[string]any
	if err := json.Unmarshal(*inv.Observation(), &top); err != nil {
		return ""
	}
	meta, _ := top["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	return name
}

func invGVRKey(inv *domain.InventoryResource) string {
	if inv == nil || inv.Observation() == nil {
		return ""
	}
	var top map[string]any
	if err := json.Unmarshal(*inv.Observation(), &top); err != nil {
		return ""
	}
	gvrMap, _ := top["gvr"].(map[string]any)
	group, _ := gvrMap["group"].(string)
	version, _ := gvrMap["version"].(string)
	resource, _ := gvrMap["resource"].(string)
	return kubeaddon.GVRKey(schema.GroupVersionResource{
		Group: group, Version: version, Resource: resource,
	})
}

func assertObjectIdentity(t *testing.T, obj *domain.ExtensionResource, clusterResourceName domain.ResourceName) {
	t.Helper()
	inv := obj.Inventory()
	if inv == nil {
		t.Fatalf("object %s missing inventory", obj.Name())
	}
	obs := invObservation(t, inv)
	meta, _ := obs["metadata"].(map[string]any)
	gvrMap, _ := obs["gvr"].(map[string]any)
	group, _ := gvrMap["group"].(string)
	version, _ := gvrMap["version"].(string)
	resource, _ := gvrMap["resource"].(string)
	kind, _ := obs["kind"].(string)
	namespace, _ := meta["namespace"].(string)
	name, _ := meta["name"].(string)
	uid, _ := meta["uid"].(string)
	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
	want, err := kubeaddon.ObjectResourceName(kubeaddon.KubernetesObjectIdentity{
		ClusterResourceName: clusterResourceName,
		GVR:                 gvr,
		Kind:                kind,
		Namespace:           namespace,
		Name:                name,
		UID:                 uid,
	})
	if err != nil {
		t.Fatalf("ObjectResourceName: %v", err)
	}
	if obj.Name() != want {
		t.Fatalf("resource name = %q, want %q", obj.Name(), want)
	}
	if obj.Name().Collection() != want.Collection() {
		t.Fatalf("collection = %q, want %q", obj.Name().Collection(), want.Collection())
	}
}

func (f *e2eFixture) awaitObjects(t *testing.T, specs ...objectSpec) []*domain.ExtensionResource {
	t.Helper()
	objs := awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		for _, spec := range specs {
			found := false
			for _, obj := range objs {
				inv := obj.Inventory()
				if inv == nil {
					continue
				}
				if invKind(inv) == spec.Kind && invMetaName(inv) == spec.Name {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}, 90*time.Second)
	for _, spec := range specs {
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			if invKind(inv) == spec.Kind && invMetaName(inv) == spec.Name {
				assertObjectIdentity(t, obj, f.clusterResourceName)
			}
		}
	}
	return objs
}

func (f *e2eFixture) awaitRunningPod(t *testing.T, namePrefix string) []*domain.ExtensionResource {
	t.Helper()
	return awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			if invKind(inv) == "Pod" && strings.HasPrefix(invMetaName(inv), namePrefix) {
				extracted := parseExtracted(t, inv)
				if phase, _ := extracted["phase"].(string); phase == "Running" {
					return true
				}
			}
		}
		return false
	}, 90*time.Second)
}

// awaitRunningWorkload waits until Deployment, ReplicaSet, and a Running
// Pod are all present. A Running Pod alone can race ahead of RS/Deploy
// indexing on slower CI.
func (f *e2eFixture) awaitRunningWorkload(t *testing.T, deployName, namePrefix string) []*domain.ExtensionResource {
	t.Helper()
	return awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		if findByKindName(objs, "Deployment", deployName) == nil {
			return false
		}
		if findByKindNamePrefix(objs, "ReplicaSet", namePrefix) == nil {
			return false
		}
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			if invKind(inv) == "Pod" && strings.HasPrefix(invMetaName(inv), namePrefix) {
				extracted := parseExtracted(t, inv)
				if phase, _ := extracted["phase"].(string); phase == "Running" {
					return true
				}
			}
		}
		return false
	}, 90*time.Second)
}

func (f *e2eFixture) awaitPodCount(t *testing.T, namePrefix string, n int) []*domain.ExtensionResource {
	t.Helper()
	return awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		count := 0
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			if invKind(inv) == "Pod" && strings.HasPrefix(invMetaName(inv), namePrefix) {
				count++
			}
		}
		return count >= n
	}, 90*time.Second)
}

func (f *e2eFixture) awaitObjectGone(t *testing.T, kind, name string) {
	t.Helper()
	awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			if invKind(inv) == kind && invMetaName(inv) == name {
				return false
			}
		}
		return true
	}, 60*time.Second)
}

func (f *e2eFixture) awaitObjectGoneByPrefix(t *testing.T, kind, prefix string) {
	t.Helper()
	awaitInventoryMatch(t, f.store, func(objs []*domain.ExtensionResource) bool {
		for _, obj := range objs {
			inv := obj.Inventory()
			if inv == nil {
				continue
			}
			if invKind(inv) == kind && strings.HasPrefix(invMetaName(inv), prefix) {
				return false
			}
		}
		return true
	}, 60*time.Second)
}

func awaitInventoryMatch(t *testing.T, store domain.Store, predicate func([]*domain.ExtensionResource) bool, timeout time.Duration) []*domain.ExtensionResource {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		objs := listInventory(t, store)
		if predicate(objs) {
			return objs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for inventory match (%d objects)", len(objs))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func listInventory(t *testing.T, store domain.Store) []*domain.ExtensionResource {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()
	objs, err := tx.ExtensionResources().ListByResourceType(context.Background(), kubeaddon.ObjectResourceType)
	if err != nil {
		t.Fatalf("ListByResourceType: %v", err)
	}
	return objs
}

func parseExtracted(t *testing.T, inv *domain.InventoryResource) map[string]any {
	t.Helper()
	obs := inv.Observation()
	if obs == nil {
		t.Fatal("nil observation")
	}
	var top map[string]any
	if err := json.Unmarshal(*obs, &top); err != nil {
		t.Fatalf("unmarshal observation: %v", err)
	}
	extracted, _ := top["extracted"].(map[string]any)
	if extracted == nil {
		return map[string]any{}
	}
	return extracted
}

func findByKindName(objs []*domain.ExtensionResource, kind, name string) *domain.ExtensionResource {
	for _, obj := range objs {
		inv := obj.Inventory()
		if inv != nil && invKind(inv) == kind && invMetaName(inv) == name {
			return obj
		}
	}
	return nil
}

func findByKindNamePrefix(objs []*domain.ExtensionResource, kind, prefix string) *domain.ExtensionResource {
	for _, obj := range objs {
		inv := obj.Inventory()
		if inv != nil && invKind(inv) == kind && strings.HasPrefix(invMetaName(inv), prefix) {
			return obj
		}
	}
	return nil
}

// ── Inventory report backend ────────────────────────────────────────────

// e2eInventoryBackend adapts application inventory services onto
// InventoryReportBackend for the in-process e2e path (same shape as
// server composition).
type e2eInventoryBackend struct {
	reports *application.InventoryReportService
}

func newE2EInventoryBackend(
	reports *application.InventoryReportService,
) *e2eInventoryBackend {
	return &e2eInventoryBackend{reports: reports}
}

func (b *e2eInventoryBackend) ReplaceBatch(ctx context.Context, resourceType domain.ResourceType, reports []kubeaddon.InventoryObjectReport) error {
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
