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

// ── Tests ───────────────────────────────────────────────────────────────

// Test_ClusterBootstrap verifies that initial cluster indexing produces
// core inventory: nodes with enriched fields, system namespaces, labels,
// and a diversity of resource types from watch-all mode.
func Test_ClusterBootstrap(t *testing.T) {
	f := setupE2E(t, skipBootstrapWait())

	// Wait for nodes, core namespaces, and a diverse set of resource types.
	expectedTypes := []domain.InventoryType{
		"v1/Node", "v1/Namespace", "v1/ServiceAccount",
		"v1/ConfigMap", "v1/Secret", "v1/Service",
	}
	items := awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		typeSet := map[domain.InventoryType]bool{}
		nsNames := map[string]bool{}
		for _, item := range items {
			typeSet[item.Type()] = true
			if item.Type() == "v1/Namespace" {
				nsNames[item.Name()] = true
			}
		}
		for _, et := range expectedTypes {
			if !typeSet[et] {
				return false
			}
		}
		return nsNames["default"] && nsNames["kube-system"] && nsNames["kube-public"]
	}, 60*time.Second)

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
		if labels := item.Labels(); len(labels) == 0 {
			t.Error("node missing labels")
		}
	}
	if !foundNode {
		t.Fatal("no Node inventory items found")
	}
}

// Test_DeployWorkload verifies deploying a workload via the platform
// delivery pipeline and checks that the full ownership chain
// (Deployment → ReplicaSet → Pod) and runsOn edge (Pod → Node) are indexed.
func Test_DeployWorkload(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "nginx-e2e-deploy", nginxDeployment("nginx-e2e", f.namespace, 1))

	items := f.awaitRunningPod(t, "nginx-e2e-")
	podUID := findUIDByPrefix(t, items, "v1/Pod", "nginx-e2e-")
	rsUID := findUIDByPrefix(t, items, "apps/v1/ReplicaSet", "nginx-e2e-")
	deployUID := findUID(t, items, "apps/v1/Deployment", "nginx-e2e")

	podEdges := f.awaitEdge(t, podUID, "ownedBy", rsUID)

	var nodeUID string
	for _, e := range podEdges {
		if e.EdgeType == "runsOn" {
			nodeUID = e.DestUID
		}
	}
	if nodeUID == "" {
		t.Error("missing runsOn edge: Pod→Node")
	}

	f.awaitEdge(t, rsUID, "ownedBy", deployUID)
}

// Test_PodAttachmentEdges verifies that pods referencing ConfigMaps
// (via volumes) and Secrets (via env valueFrom) produce attachedTo edges
// to those resources.
func Test_PodAttachmentEdges(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "attach-deploy",
		configMapManifest("e2e-cm", f.namespace, map[string]string{"key": "value"}),
		secretManifest("e2e-secret", f.namespace, map[string]string{"password": "test"}),
		attachmentPodManifest("e2e-attach-pod", f.namespace, "e2e-cm", "e2e-secret"),
	)

	items := f.awaitItems(t,
		itemSpec{"v1/Pod", "e2e-attach-pod"},
		itemSpec{"v1/ConfigMap", "e2e-cm"},
		itemSpec{"v1/Secret", "e2e-secret"},
	)

	podUID := findUID(t, items, "v1/Pod", "e2e-attach-pod")
	cmUID := findUID(t, items, "v1/ConfigMap", "e2e-cm")
	secretUID := findUID(t, items, "v1/Secret", "e2e-secret")

	f.awaitEdge(t, podUID, "attachedTo", cmUID)
	f.awaitEdge(t, podUID, "attachedTo", secretUID)
}

// Test_ServiceSelectorEdges verifies that a Service with a label
// selector produces a selects edge pointing to matching Pods.
func Test_ServiceSelectorEdges(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "svc-deploy",
		nginxDeployment("e2e-svc", f.namespace, 1),
		serviceManifest("e2e-svc", f.namespace, map[string]string{"app": "e2e-svc"}, 80),
	)

	f.awaitRunningPod(t, "e2e-svc-")

	items := f.awaitItems(t, itemSpec{"v1/Service", "e2e-svc"})
	svcUID := findUID(t, items, "v1/Service", "e2e-svc")

	f.awaitEdgeByKind(t, svcUID, "selects", "Pod")
}

// Test_PVCPVEdge verifies that a PersistentVolumeClaim bound to a
// PersistentVolume produces an attachedTo edge from PVC to PV.
func Test_PVCPVEdge(t *testing.T) {
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

	items := f.awaitItems(t,
		itemSpec{"v1/PersistentVolumeClaim", "e2e-pvc"},
		itemSpec{"v1/PersistentVolume", "e2e-pv"},
	)

	pvcUID := findUID(t, items, "v1/PersistentVolumeClaim", "e2e-pvc")
	pvUID := findUID(t, items, "v1/PersistentVolume", "e2e-pv")

	f.awaitEdge(t, pvcUID, "attachedTo", pvUID)
}

// Test_UpdateReindex verifies that the WATCH phase detects resource
// mutations. Scales a deployment from 1 to 2 replicas and confirms the
// new pod appears in inventory with an ownedBy edge.
func Test_UpdateReindex(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "scale-deploy", nginxDeployment("e2e-scale", f.namespace, 1))
	f.awaitPodCount(t, "e2e-scale-", 1)

	// Scale to 2 via dynamic client.
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

	items := f.awaitPodCount(t, "e2e-scale-", 2)

	for _, item := range items {
		if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-scale-") {
			uid := uidFromItemID(item.ID())
			awaitEdgeFrom(t, f.harness.Store, f.targetID, uid, func(edges []domain.InventoryEdge) bool {
				for _, e := range edges {
					if e.EdgeType == "ownedBy" {
						return true
					}
				}
				return false
			}, 30*time.Second)
		}
	}
}

// Test_RemoveCleanup verifies that the indexer detects external resource
// deletions. Deletes a deployment via the Kubernetes API and confirms the
// deployment and its pods are removed from inventory.
func Test_RemoveCleanup(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "remove-deploy", nginxDeployment("e2e-remove", f.namespace, 1))
	f.awaitPodCount(t, "e2e-remove-", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	deployGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	err := f.dynClient.Resource(deployGVR).Namespace(f.namespace).Delete(ctx, "e2e-remove", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete deployment: %v", err)
	}

	f.awaitItemGone(t, "apps/v1/Deployment", "e2e-remove")
	f.awaitItemGoneByPrefix(t, "v1/Pod", "e2e-remove-")
}

// Test_CRDLifecycle verifies that the CRD watcher triggers informer
// reconciliation when a new CRD is created, and that custom resources
// of that type are indexed with base-tier extraction.
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

	f.awaitItems(t, itemSpec{"test.fleetshift.io/v1/Widget", "test-widget"})
}

// Test_DefaultDenyList verifies that the default deny list correctly
// filters out high-volume and transient resource types (Events, Leases,
// Endpoints, EndpointSlices) from inventory.
func Test_DefaultDenyList(t *testing.T) {
	f := setupE2E(t)

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

// Test_TargetTermination verifies that StopIndexing stops the agent and
// that platform-side inventory cleanup removes all items and edges.
func Test_TargetTermination(t *testing.T) {
	f := setupE2E(t)

	tx, _ := f.harness.Store.BeginReadOnly(context.Background())
	items, _ := tx.Inventory().List(context.Background())
	tx.Rollback()
	if len(items) == 0 {
		t.Fatal("expected inventory items before termination")
	}

	// Wait for at least one pod to have edges (edges flush asynchronously).
	var edgeSourceUID string
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Pod" {
				uid := uidFromItemID(item.ID())
				edges := queryEdgesFrom(t, f.harness.Store, f.targetID, uid)
				if len(edges) > 0 {
					edgeSourceUID = uid
					return true
				}
			}
		}
		return false
	}, 30*time.Second)

	ctx := context.Background()
	if err := f.k8sPool.StopIndexing(ctx, f.target); err != nil {
		t.Fatalf("StopIndexing: %v", err)
	}

	// Platform-side cleanup (mirrors what CleanupDeliveryData does).
	cleanup := &application.InventoryCleanupService{Store: f.harness.Store}
	if err := cleanup.DeleteByTarget(ctx, f.targetID); err != nil {
		t.Fatalf("inventory cleanup: %v", err)
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

	// Verify edges are also gone.
	edges := queryEdgesFrom(t, f.harness.Store, f.targetID, edgeSourceUID)
	if len(edges) > 0 {
		t.Errorf("edges still present after termination: %d edges from %s", len(edges), edgeSourceUID)
	}
}

// Test_DeliveryRemoval verifies the delivery Remove() path: deploying a
// workload via the platform pipeline, then removing it through the delivery
// delegate's delete path, and confirming the indexer detects the deletion.
func Test_DeliveryRemoval(t *testing.T) {
	f := setupE2E(t)

	manifest := nginxDeployment("e2e-removal", f.namespace, 1)
	f.deploy(t, "removal-deploy", manifest)
	f.awaitRunningPod(t, "e2e-removal-")

	f.remove(t, manifest)

	f.awaitItemGone(t, "apps/v1/Deployment", "e2e-removal")
	f.awaitItemGoneByPrefix(t, "v1/Pod", "e2e-removal-")
}

// Test_CRDDeletion verifies the second half of the CRD lifecycle: when
// a CRD is deleted, the CRD watcher triggers informer reconciliation, which
// stops the informer for that GVR. Kubernetes cascades the CRD deletion to
// all custom resources, and the indexer removes them from inventory.
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
				"kind":     "Gadget",
				"plural":   "gadgets",
				"singular": "gadget",
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

	f.awaitItems(t, itemSpec{"test.fleetshift.io/v1/Gadget", "test-gadget"})

	// Delete the CRD — Kubernetes cascades to all CRs, and the CRD watcher
	// triggers informer reconciliation.
	err = f.dynClient.Resource(crdGVR).Delete(ctx, "gadgets.test.fleetshift.io", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete CRD: %v", err)
	}

	f.awaitItemGone(t, "test.fleetshift.io/v1/Gadget", "test-gadget")
}

// Test_EnrichedFields verifies that the two-tier extraction pipeline
// produces correct enriched fields from real Kubernetes resources. Deploys
// a workload and checks schema-driven fields on Deployments, ReplicaSets,
// and Pods — defense in depth for the extraction + schema hook pipeline.
func Test_EnrichedFields(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "enriched-deploy", nginxDeployment("e2e-enriched", f.namespace, 1))
	items := f.awaitRunningPod(t, "e2e-enriched-")

	// Verify Deployment enriched fields.
	for _, item := range items {
		if item.Type() == "apps/v1/Deployment" && item.Name() == "e2e-enriched" {
			observed := parseObserved(t, item)
			if v, _ := observed["replicas"].(float64); v != 1 {
				t.Errorf("deployment replicas = %v, want 1", observed["replicas"])
			}
			if _, ok := observed["availableReplicas"]; !ok {
				t.Error("deployment missing availableReplicas")
			}
			if len(item.Conditions()) == 0 {
				t.Error("deployment missing conditions")
			}
		}
	}

	// Verify ReplicaSet enriched fields.
	for _, item := range items {
		if item.Type() == "apps/v1/ReplicaSet" && strings.HasPrefix(item.Name(), "e2e-enriched-") {
			observed := parseObserved(t, item)
			if v, _ := observed["replicas"].(float64); v != 1 {
				t.Errorf("replicaset replicas = %v, want 1", observed["replicas"])
			}
			if _, ok := observed["readyReplicas"]; !ok {
				t.Error("replicaset missing readyReplicas")
			}
			break
		}
	}

	// Verify Pod enriched fields.
	for _, item := range items {
		if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-enriched-") {
			observed := parseObserved(t, item)
			if phase, _ := observed["phase"].(string); phase != "Running" {
				t.Errorf("pod phase = %q, want Running", phase)
			}
			if _, ok := observed["podIP"]; !ok {
				t.Error("pod missing podIP")
			}
			if _, ok := observed["containerImages"]; !ok {
				t.Error("pod missing containerImages")
			}
			if _, ok := observed["status"]; !ok {
				t.Error("pod missing computed status")
			}
			if len(item.Conditions()) == 0 {
				t.Error("pod missing conditions")
			}
			break
		}
	}
}

// Test_LabelIndexing verifies that Kubernetes labels flow through the
// full indexing pipeline (informer → writer → extraction → inventory) and
// are queryable on inventory items.
func Test_LabelIndexing(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "label-deploy", nginxDeployment("e2e-labels", f.namespace, 1))
	items := f.awaitRunningPod(t, "e2e-labels-")

	// Verify labels on the Pod (template labels propagate from the deployment spec).
	var foundPod bool
	for _, item := range items {
		if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), "e2e-labels-") {
			foundPod = true
			labels := item.Labels()
			if labels == nil {
				t.Fatal("pod has nil labels")
			}
			if labels["app"] != "e2e-labels" {
				t.Errorf("pod label app = %q, want e2e-labels", labels["app"])
			}
			break
		}
	}
	if !foundPod {
		t.Fatal("no pod found with prefix e2e-labels-")
	}

	// Verify labels on a Node (bootstrap resources carry Kubernetes-set labels).
	for _, item := range items {
		if item.Type() == "v1/Node" {
			labels := item.Labels()
			if labels == nil {
				t.Fatal("node has nil labels")
			}
			if _, ok := labels["kubernetes.io/os"]; !ok {
				t.Error("node missing kubernetes.io/os label")
			}
			break
		}
	}
}

// Test_IncomingEdges verifies that edges can be queried by destination
// UID (incoming direction). Deploys a workload and queries all edges
// pointing to a Node, which should include runsOn edges from pods.
func Test_IncomingEdges(t *testing.T) {
	f := setupE2E(t)

	f.deploy(t, "incoming-deploy", nginxDeployment("e2e-incoming", f.namespace, 1))
	items := f.awaitRunningPod(t, "e2e-incoming-")

	podUID := findUIDByPrefix(t, items, "v1/Pod", "e2e-incoming-")

	// Wait for the pod's runsOn edge to appear (edges flush asynchronously).
	podEdges := awaitEdgeFrom(t, f.harness.Store, f.targetID, podUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == "runsOn" {
				return true
			}
		}
		return false
	}, 30*time.Second)

	var nodeUID string
	for _, e := range podEdges {
		if e.EdgeType == "runsOn" {
			nodeUID = e.DestUID
		}
	}

	// Query incoming edges to the node — should include our pod's runsOn.
	tx, err := f.harness.Store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	incomingEdges, err := tx.Edges().ListByDestUID(context.Background(), f.targetID, nodeUID)
	if err != nil {
		t.Fatalf("ListByDestUID: %v", err)
	}

	var foundRunsOn bool
	for _, e := range incomingEdges {
		if e.EdgeType == "runsOn" && e.SourceUID == podUID {
			foundRunsOn = true
			break
		}
	}
	if !foundRunsOn {
		t.Errorf("incoming edges to node %s missing runsOn from pod %s (%d edges total)", nodeUID, podUID, len(incomingEdges))
	}
}

// ── Fixtures & setup ────────────────────────────────────────────────────

type clusterFixture struct {
	apiServer      string
	caCert         string
	saToken        string
	adminRestCfg   *rest.Config
	adminDynClient dynamic.Interface
	adminK8s       *kubernetes.Clientset
}

type e2eFixture struct {
	harness   *testharness.Harness
	k8sPool   *kubeaddon.AgentPool
	dynClient dynamic.Interface
	typedK8s  *kubernetes.Clientset
	namespace string
	target    domain.TargetInfo
	targetID  domain.TargetID
	auth      domain.DeliveryAuth
}

type setupOption func(*setupConfig)
type setupConfig struct{ skipBootstrap bool }

func skipBootstrapWait() setupOption {
	return func(c *setupConfig) { c.skipBootstrap = true }
}

func setupE2E(t *testing.T, opts ...setupOption) *e2eFixture {
	t.Helper()

	cfg := &setupConfig{}
	for _, o := range opts {
		o(cfg)
	}

	h := testharness.New(t)
	inventoryWriter := application.NewInventoryWriteService(h.Store)

	k8sPool := kubeaddon.NewAgentPool(
		context.Background(),
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
		DeliveryAgent: k8sPool,
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
	if err := k8sPool.StartIndexing(ctx, target); err != nil {
		t.Fatalf("StartIndexing: %v", err)
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
		k8sPool.StopAll()
		_ = fixture.adminDynClient.Resource(nsGVR).Delete(context.Background(), ns, metav1.DeleteOptions{})
	})

	f := &e2eFixture{
		harness:   h,
		k8sPool:   k8sPool,
		dynClient: fixture.adminDynClient,
		typedK8s:  fixture.adminK8s,
		namespace: ns,
		target:    target,
		targetID:  targetID,
		auth:      domain.DeliveryAuth{Token: domain.RawToken(fixture.saToken)},
	}

	if !cfg.skipBootstrap {
		awaitInventoryMatch(t, h.Store, func(items []domain.InventoryItem) bool {
			for _, item := range items {
				if item.Type() == "v1/Node" {
					return true
				}
			}
			return false
		}, 60*time.Second)
	}

	return f
}

// ── Deploy & manifest helpers ───────────────────────────────────────────

func (f *e2eFixture) deploy(t *testing.T, id string, manifests ...json.RawMessage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	kubeManifests := make([]domain.Manifest, len(manifests))
	for i, m := range manifests {
		kubeManifests[i] = domain.Manifest{ResourceType: kubeaddon.ManifestResourceType, Raw: m}
	}

	_, err := f.harness.Deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:   domain.DeploymentID(id),
		Auth: f.auth,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: kubeManifests,
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{f.targetID},
		},
	})
	if err != nil {
		t.Fatalf("deploy %s: %v", id, err)
	}
}

func (f *e2eFixture) remove(t *testing.T, manifests ...json.RawMessage) {
	t.Helper()
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:   f.targetID,
		Type: kubeaddon.TargetType,
		Properties: map[string]string{
			"api_server":            fixture.apiServer,
			"ca_cert":               fixture.caCert,
			"service_account_token": fixture.saToken,
		},
	})
	kubeManifests := make([]domain.Manifest, len(manifests))
	for i, m := range manifests {
		kubeManifests[i] = domain.Manifest{ResourceType: kubeaddon.ManifestResourceType, Raw: m}
	}
	err := f.k8sPool.Remove(context.Background(), target, "e2e-removal", kubeManifests, f.auth, nil, 1)
	if err != nil {
		t.Fatalf("remove: %v", err)
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

type itemSpec struct {
	Type domain.InventoryType
	Name string
}

func (f *e2eFixture) awaitItems(t *testing.T, specs ...itemSpec) []domain.InventoryItem {
	t.Helper()
	return awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, spec := range specs {
			found := false
			for _, item := range items {
				if item.Type() == spec.Type && item.Name() == spec.Name {
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
}

func (f *e2eFixture) awaitRunningPod(t *testing.T, namePrefix string) []domain.InventoryItem {
	t.Helper()
	return awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), namePrefix) {
				observed := map[string]any{}
				_ = json.Unmarshal(item.Observed(), &observed)
				if phase, _ := observed["phase"].(string); phase == "Running" {
					return true
				}
			}
		}
		return false
	}, 90*time.Second)
}

func (f *e2eFixture) awaitPodCount(t *testing.T, namePrefix string, n int) []domain.InventoryItem {
	t.Helper()
	return awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		count := 0
		for _, item := range items {
			if item.Type() == "v1/Pod" && strings.HasPrefix(item.Name(), namePrefix) {
				count++
			}
		}
		return count >= n
	}, 90*time.Second)
}

func (f *e2eFixture) awaitItemGone(t *testing.T, invType domain.InventoryType, name string) {
	t.Helper()
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == invType && item.Name() == name {
				return false
			}
		}
		return true
	}, 60*time.Second)
}

func (f *e2eFixture) awaitItemGoneByPrefix(t *testing.T, invType domain.InventoryType, prefix string) {
	t.Helper()
	awaitInventoryMatch(t, f.harness.Store, func(items []domain.InventoryItem) bool {
		for _, item := range items {
			if item.Type() == invType && strings.HasPrefix(item.Name(), prefix) {
				return false
			}
		}
		return true
	}, 60*time.Second)
}

func (f *e2eFixture) awaitEdge(t *testing.T, sourceUID, edgeType, destUID string) []domain.InventoryEdge {
	t.Helper()
	return awaitEdgeFrom(t, f.harness.Store, f.targetID, sourceUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == edgeType && e.DestUID == destUID {
				return true
			}
		}
		return false
	}, 30*time.Second)
}

func (f *e2eFixture) awaitEdgeByKind(t *testing.T, sourceUID, edgeType, destKind string) []domain.InventoryEdge {
	t.Helper()
	return awaitEdgeFrom(t, f.harness.Store, f.targetID, sourceUID, func(edges []domain.InventoryEdge) bool {
		for _, e := range edges {
			if e.EdgeType == edgeType && e.DestKind == destKind {
				return true
			}
		}
		return false
	}, 30*time.Second)
}

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

// ── Query & parse helpers ───────────────────────────────────────────────

func parseObserved(t *testing.T, item domain.InventoryItem) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(item.Observed(), &m); err != nil {
		t.Fatalf("unmarshal observed for %s: %v", item.ID(), err)
	}
	return m
}

func uidFromItemID(id domain.InventoryItemID) string {
	parts := strings.SplitN(string(id), "/", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return string(id)
}

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

func findUIDByPrefix(t *testing.T, items []domain.InventoryItem, invType domain.InventoryType, prefix string) string {
	t.Helper()
	for _, item := range items {
		if item.Type() == invType && strings.HasPrefix(item.Name(), prefix) {
			return uidFromItemID(item.ID())
		}
	}
	t.Fatalf("item not found: type=%s prefix=%s", invType, prefix)
	return ""
}
