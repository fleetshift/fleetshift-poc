package kubernetes

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func mustExtract(t *testing.T, r *unstructured.Unstructured, entry SchemaEntry, clusterResourceName domain.ResourceName) (InventoryObjectReport, inventoryNode) {
	t.Helper()
	report, node, err := ExtractObservedResource(r, entry, clusterResourceName)
	if err != nil {
		t.Fatalf("ExtractObservedResource: %v", err)
	}
	return report, node
}

func mustObservation(t *testing.T, report InventoryObjectReport) map[string]any {
	t.Helper()
	if report.Observation == nil {
		t.Fatal("Observation is nil")
	}
	var out map[string]any
	if err := json.Unmarshal(*report.Observation, &out); err != nil {
		t.Fatalf("unmarshal Observation: %v", err)
	}
	return out
}

func extractedFields(t *testing.T, report InventoryObjectReport) map[string]any {
	t.Helper()
	obs := mustObservation(t, report)
	extracted, ok := obs["extracted"].(map[string]any)
	if !ok {
		t.Fatalf("extracted is not a map: %#v", obs["extracted"])
	}
	return extracted
}

func TestExtractIdentityFields(t *testing.T) {
	ts := metav1.NewTime(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "abc-123",
				"name":              "my-deploy",
				"namespace":         "default",
				"creationTimestamp": ts.Format(time.RFC3339),
				"labels": map[string]any{
					"app": "web",
				},
				"annotations": map[string]any{
					"note": "hello",
				},
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "ReplicaSet",
						"name":       "my-rs",
						"uid":        "owner-uid-1",
					},
					map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"name":       "my-cm",
						"uid":        "owner-uid-2",
					},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
	}

	report, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	wantName := "clusters/target-1/apiResources/apps~v1~deployments/objects/abc-123"
	if string(report.Name) != wantName {
		t.Errorf("Name = %q, want %q", report.Name, wantName)
	}
	if report.Labels["k8s.gvr"] != "apps~v1~deployments" {
		t.Errorf("Labels[k8s.gvr] = %q, want %q", report.Labels["k8s.gvr"], "apps~v1~deployments")
	}
	if report.Labels["k8s.kind"] != "Deployment" {
		t.Errorf("Labels[k8s.kind] = %q, want %q", report.Labels["k8s.kind"], "Deployment")
	}
	if report.Labels["k8s.name"] != "my-deploy" {
		t.Errorf("Labels[k8s.name] = %q, want %q", report.Labels["k8s.name"], "my-deploy")
	}
	if report.Labels["k8s.namespace"] != "default" {
		t.Errorf("Labels[k8s.namespace] = %q, want %q", report.Labels["k8s.namespace"], "default")
	}
	if report.Labels["k8s.uid"] != "abc-123" {
		t.Errorf("Labels[k8s.uid] = %q, want %q", report.Labels["k8s.uid"], "abc-123")
	}
	// Kubernetes object labels stay on the node for selector matching,
	// not on the FleetShift report labels.
	if report.Labels["app"] != "" {
		t.Errorf("report Labels should not copy Kubernetes object labels, got app=%q", report.Labels["app"])
	}
	if node.Labels["app"] != "web" {
		t.Errorf("node.Labels[app] = %q, want %q", node.Labels["app"], "web")
	}

	obs := mustObservation(t, report)
	if obs["apiVersion"] != "apps/v1" || obs["kind"] != "Deployment" {
		t.Errorf("apiVersion/kind = %v/%v, want apps/v1/Deployment", obs["apiVersion"], obs["kind"])
	}
	gvr, _ := obs["gvr"].(map[string]any)
	if gvr["group"] != "apps" || gvr["version"] != "v1" || gvr["resource"] != "deployments" || gvr["scope"] != "namespaced" {
		t.Errorf("gvr = %#v, want apps/v1/deployments namespaced", gvr)
	}
	meta, _ := obs["metadata"].(map[string]any)
	if meta["uid"] != "abc-123" || meta["namespace"] != "default" || meta["name"] != "my-deploy" {
		t.Errorf("metadata identity = %#v", meta)
	}
	labels, _ := meta["labels"].(map[string]any)
	if labels["app"] != "web" {
		t.Errorf("metadata.labels[app] = %v, want web", labels["app"])
	}
}

func TestExtractCreationTimestamp_InObservation(t *testing.T) {
	k8sCreationTime := time.Date(2024, 3, 15, 8, 30, 0, 0, time.UTC)
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "uid-ts",
				"name":              "ts-deploy",
				"namespace":         "default",
				"creationTimestamp": k8sCreationTime.Format(time.RFC3339),
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	meta, _ := mustObservation(t, report)["metadata"].(map[string]any)
	if meta["creationTimestamp"] != k8sCreationTime.Format(time.RFC3339) {
		t.Errorf("creationTimestamp = %v, want %v", meta["creationTimestamp"], k8sCreationTime.Format(time.RFC3339))
	}
}

func TestExtractIdentityFields_CoreAPIGroup(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               string(types.UID("pod-uid")),
				"name":              "my-pod",
				"namespace":         "kube-system",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if report.Labels["k8s.gvr"] != "core~v1~pods" {
		t.Errorf("Labels[k8s.gvr] = %q, want %q", report.Labels["k8s.gvr"], "core~v1~pods")
	}
	if report.Labels["k8s.group"] != "" {
		t.Errorf("Labels[k8s.group] = %q, want empty for core", report.Labels["k8s.group"])
	}
	obs := mustObservation(t, report)
	if obs["apiVersion"] != "v1" || obs["kind"] != "Pod" {
		t.Errorf("apiVersion/kind = %v/%v, want v1/Pod", obs["apiVersion"], obs["kind"])
	}
	gvr, _ := obs["gvr"].(map[string]any)
	if gvr["group"] != "" || gvr["version"] != "v1" || gvr["resource"] != "pods" {
		t.Errorf("gvr = %#v, want core v1 pods", gvr)
	}
	wantName := "clusters/target-1/apiResources/core~v1~pods/objects/pod-uid"
	if string(report.Name) != wantName {
		t.Errorf("Name = %q, want %q", report.Name, wantName)
	}
}

func TestExtractConditions(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "cond-uid",
				"name":              "cond-deploy",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"conditions": []any{
					map[string]any{
						"type":               "Available",
						"status":             "True",
						"reason":             "MinimumReplicasAvailable",
						"message":            "Deployment has minimum availability.",
						"lastTransitionTime": "2025-06-01T10:00:00Z",
					},
					map[string]any{
						"type":   "Progressing",
						"status": "True",
						"reason": "NewReplicaSetAvailable",
					},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	conds := report.Conditions
	if len(conds) != 2 {
		t.Fatalf("Conditions len = %d, want 2", len(conds))
	}

	c0 := conds[0]
	if c0.Type() != "Available" {
		t.Errorf("Conditions[0].Type = %q, want %q", c0.Type(), "Available")
	}
	if c0.Status() != domain.ConditionTrue {
		t.Errorf("Conditions[0].Status = %q, want %q", c0.Status(), domain.ConditionTrue)
	}
	if c0.Reason() != "MinimumReplicasAvailable" {
		t.Errorf("Conditions[0].Reason = %q, want %q", c0.Reason(), "MinimumReplicasAvailable")
	}
	if c0.Message() != "Deployment has minimum availability." {
		t.Errorf("Conditions[0].Message = %q, want %q", c0.Message(), "Deployment has minimum availability.")
	}
	want := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	if !c0.LastTransitionTime().Equal(want) {
		t.Errorf("Conditions[0].LastTransitionTime = %v, want %v", c0.LastTransitionTime(), want)
	}

	c1 := conds[1]
	if c1.Type() != "Progressing" {
		t.Errorf("Conditions[1].Type = %q, want %q", c1.Type(), "Progressing")
	}
	// Missing lastTransitionTime falls back to the report's ObservedAt.
	if !c1.LastTransitionTime().Equal(report.ObservedAt) {
		t.Errorf("Conditions[1].LastTransitionTime = %v, want ObservedAt %v", c1.LastTransitionTime(), report.ObservedAt)
	}
}

func TestExtractConditions_ContractRules(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "cond-rules-uid",
				"name":              "cond-rules",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"conditions": []any{
					map[string]any{
						"type":   "",
						"status": "True",
					},
					map[string]any{
						"type":   "Health",
						"status": "Healthy",
					},
					map[string]any{
						"type":   "Ready",
						"status": "False",
						"reason": "Initializing",
					},
					map[string]any{
						"type":               "Ready",
						"status":             "True",
						"reason":             "AllGood",
						"lastTransitionTime": "not-a-timestamp",
					},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if len(report.Conditions) != 1 {
		t.Fatalf("Conditions = %#v, want exactly one Ready after dropping empty type and nonstandard status", report.Conditions)
	}
	c := report.Conditions[0]
	if c.Type() != "Ready" || c.Status() != domain.ConditionTrue || c.Reason() != "AllGood" {
		t.Fatalf("condition = type=%s status=%s reason=%s, want Ready/True/AllGood (last entry wins)", c.Type(), c.Status(), c.Reason())
	}
	if !c.LastTransitionTime().Equal(report.ObservedAt) {
		t.Errorf("malformed lastTransitionTime should fall back to ObservedAt; got %v want %v", c.LastTransitionTime(), report.ObservedAt)
	}
}

func TestExtractObservedFields_NumberType(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "fields-uid",
				"name":              "fields-deploy",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"replicas": int64(3),
			},
			"status": map[string]any{
				"readyReplicas": int64(2),
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
			{Name: "readyReplicas", JSONPath: ".status.readyReplicas", DataType: DataTypeNumber},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	replVal, ok := fields["replicas"]
	if !ok {
		t.Fatal("extracted missing 'replicas'")
	}
	if replVal.(float64) != 3 {
		t.Errorf("replicas = %v, want 3", replVal)
	}

	readyVal, ok := fields["readyReplicas"]
	if !ok {
		t.Fatal("extracted missing 'readyReplicas'")
	}
	if readyVal.(float64) != 2 {
		t.Errorf("readyReplicas = %v, want 2", readyVal)
	}
}

func TestExtractObservedFields_BytesType(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]any{
				"uid":               "node-uid",
				"name":              "worker-1",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"allocatable": map[string]any{
					"memory": "128Mi",
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
		Kind: "Node",
		Fields: []FieldExtraction{
			{Name: "memoryAllocatable", JSONPath: ".status.allocatable.memory", DataType: DataTypeBytes},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	memVal, ok := fields["memoryAllocatable"]
	if !ok {
		t.Fatal("extracted missing 'memoryAllocatable'")
	}
	// 128Mi = 128 * 1024 * 1024 = 134217728
	want := float64(134217728)
	if memVal.(float64) != want {
		t.Errorf("memoryAllocatable = %v, want %v", memVal, want)
	}
}

func TestExtractObservedFields_StringType(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "str-uid",
				"name":              "str-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"phase": "Running",
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "phase", JSONPath: ".status.phase"},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	v, ok := fields["phase"]
	if !ok {
		t.Fatal("extracted missing 'phase'")
	}
	if v.(string) != "Running" {
		t.Errorf("phase = %q, want %q", v, "Running")
	}
}

func TestExtractObservedFields_SliceType(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "slice-uid",
				"name":              "slice-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"containerStatuses": []any{
					map[string]any{"image": "nginx:1.25"},
					map[string]any{"image": "sidecar:latest"},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "containerImages", JSONPath: ".status.containerStatuses[*].image", DataType: DataTypeSlice},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	v, ok := fields["containerImages"]
	if !ok {
		t.Fatal("extracted missing 'containerImages'")
	}
	listVal, ok := v.([]any)
	if !ok {
		t.Fatal("containerImages is not a list")
	}
	if len(listVal) != 2 {
		t.Fatalf("containerImages len = %d, want 2", len(listVal))
	}
	if listVal[0].(string) != "nginx:1.25" {
		t.Errorf("containerImages[0] = %q, want %q", listVal[0], "nginx:1.25")
	}
	if listVal[1].(string) != "sidecar:latest" {
		t.Errorf("containerImages[1] = %q, want %q", listVal[1], "sidecar:latest")
	}
}

func TestExtractObservedFields_BoolNativeNotString(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "bool-uid",
				"name":              "bool-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"hostNetwork": true,
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "hostNetwork", JSONPath: ".spec.hostNetwork"},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	v, ok := fields["hostNetwork"]
	if !ok {
		t.Fatal("extracted missing 'hostNetwork'")
	}
	if v.(bool) != true {
		t.Error("hostNetwork should be true (native bool), not a string")
	}
}

func TestExtractObservedFields_JSONPathNormalization(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "jp-uid",
				"name":              "jp-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"phase": "Pending",
			},
		},
	}

	// JSONPath with braces should also work
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "phase", JSONPath: "{.status.phase}"},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	v, ok := fields["phase"]
	if !ok {
		t.Fatal("extracted missing 'phase'")
	}
	if v.(string) != "Pending" {
		t.Errorf("phase = %q, want %q", v, "Pending")
	}
}

func TestExtractObservedFields_MissingFieldIsSkipped(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "miss-uid",
				"name":              "miss-deploy",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"replicas": int64(3),
			},
			// No status at all
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
			{Name: "readyReplicas", JSONPath: ".status.readyReplicas", DataType: DataTypeNumber},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	if _, ok := fields["replicas"]; !ok {
		t.Error("should have 'replicas'")
	}
	if _, ok := fields["readyReplicas"]; ok {
		t.Error("should NOT have 'readyReplicas' when status is missing")
	}
}

func TestExtractAnnotations_StripsInternalKeys(t *testing.T) {
	// Extraction must succeed when annotations with internal keys are
	// present; filtered annotations are only written into extracted
	// when ExtractAnnotations is enabled.
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "strip-uid",
				"name":              "strip-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"annotations": map[string]any{
					"keep": "yes",
					"apps.open-cluster-management.io/hosting-subscription": "sub/foo",
					"apps.open-cluster-management.io/hosting-deployable":   "dep/bar",
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if report.Labels["k8s.name"] != "strip-pod" {
		t.Errorf("Labels[k8s.name] = %q, want %q", report.Labels["k8s.name"], "strip-pod")
	}
	meta, _ := mustObservation(t, report)["metadata"].(map[string]any)
	annotations, _ := meta["annotations"].(map[string]any)
	if annotations["keep"] != "yes" {
		t.Errorf("metadata.annotations should retain source annotations, got %#v", annotations)
	}
}

func TestExtractObservedFields_MapStringType(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "map-uid",
				"name":              "map-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"nodeSelector": map[string]any{
					"disktype": "ssd",
					"region":   "us-east",
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "nodeSelector", JSONPath: ".spec.nodeSelector", DataType: DataTypeMapString},
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	v, ok := fields["nodeSelector"]
	if !ok {
		t.Fatal("extracted missing 'nodeSelector'")
	}
	sv, ok := v.(map[string]any)
	if !ok {
		t.Fatal("nodeSelector is not a map")
	}
	if sv["disktype"].(string) != "ssd" {
		t.Errorf("nodeSelector.disktype = %q, want %q", sv["disktype"], "ssd")
	}
	if sv["region"].(string) != "us-east" {
		t.Errorf("nodeSelector.region = %q, want %q", sv["region"], "us-east")
	}
}

func TestExtractOwnerReferences_ControllerOwner(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "rs-uid",
				"name":              "my-rs",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "Deployment",
						"name":       "my-deploy",
						"uid":        "deploy-uid",
						"controller": true,
					},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"},
		Kind: "ReplicaSet",
	}
	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if node.OwnerUID != "deploy-uid" {
		t.Errorf("OwnerUID = %q, want %q", node.OwnerUID, "deploy-uid")
	}
}

func TestExtractOwnerReferences_MultipleOwnersSelectsController(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "pod-uid",
				"name":              "my-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"name":       "my-cm",
						"uid":        "cm-uid",
					},
					map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "ReplicaSet",
						"name":       "my-rs",
						"uid":        "rs-uid",
						"controller": true,
					},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if node.OwnerUID != "rs-uid" {
		t.Errorf("OwnerUID = %q, want %q (should select controller)", node.OwnerUID, "rs-uid")
	}
}

func TestExtractOwnerReferences_NoController(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "pod-uid",
				"name":              "my-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"name":       "my-cm",
						"uid":        "cm-uid",
					},
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if node.OwnerUID != "" {
		t.Errorf("OwnerUID = %q, want empty (no controller)", node.OwnerUID)
	}
}

func TestExtractGeneration(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "gen-uid",
				"name":              "gen-deploy",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"generation":        int64(5),
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	meta, _ := mustObservation(t, report)["metadata"].(map[string]any)
	gen, ok := meta["generation"]
	if !ok {
		t.Fatal("metadata missing 'generation'")
	}
	// JSON numbers are float64
	if gen.(float64) != 5 {
		t.Errorf("generation = %v, want 5", gen)
	}
	// generation belongs in observation metadata, not extracted
	if _, exists := extractedFields(t, report)["generation"]; exists {
		t.Error("generation should not be duplicated into extracted")
	}
}

func TestExtractDeletionTimestamp(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "del-uid",
				"name":              "del-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"deletionTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	meta, _ := mustObservation(t, report)["metadata"].(map[string]any)
	dt, ok := meta["deletionTimestamp"]
	if !ok {
		t.Fatal("metadata missing 'deletionTimestamp'")
	}
	if dt.(string) != "2025-06-01T12:00:00Z" {
		t.Errorf("deletionTimestamp = %q, want %q", dt, "2025-06-01T12:00:00Z")
	}
	if _, exists := extractedFields(t, report)["deletionTimestamp"]; exists {
		t.Error("deletionTimestamp should not be duplicated into extracted")
	}
}

func TestExtractAnnotations_WithSizeCap(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "ann-uid",
				"name":              "ann-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"annotations": map[string]any{
					"short": "ok",
					"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"ann-pod"}}`,
					"long": "this is a very long annotation that exceeds the size cap and should be filtered out",
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:                schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind:               "Pod",
		ExtractAnnotations: true,
		AnnotationSizeCap:  20,
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	annotations, ok := fields["annotations"]
	if !ok {
		t.Fatal("extracted missing 'annotations'")
	}

	annMap, ok := annotations.(map[string]any)
	if !ok {
		t.Fatal("annotations is not a map")
	}

	if annMap["short"].(string) != "ok" {
		t.Errorf("annotations[short] = %q, want %q", annMap["short"], "ok")
	}

	if _, exists := annMap["kubectl.kubernetes.io/last-applied-configuration"]; exists {
		t.Error("kubectl.kubernetes.io/last-applied-configuration should be filtered out")
	}

	if _, exists := annMap["long"]; exists {
		t.Error("long annotation should be filtered out due to size cap")
	}
}

func TestExtractAnnotations_DefaultSizeCap(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "ann-uid2",
				"name":              "ann-pod2",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"annotations": map[string]any{
					"short": "ok",
					"long":  "this is a very long annotation that exceeds the default 64 character size cap and should be filtered out completely",
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:                schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind:               "Pod",
		ExtractAnnotations: true,
		// AnnotationSizeCap not set, should default to 64
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	annotations, ok := fields["annotations"]
	if !ok {
		t.Fatal("extracted missing 'annotations'")
	}

	annMap, ok := annotations.(map[string]any)
	if !ok {
		t.Fatal("annotations is not a map")
	}

	if annMap["short"].(string) != "ok" {
		t.Errorf("annotations[short] = %q, want %q", annMap["short"], "ok")
	}

	if _, exists := annMap["long"]; exists {
		t.Error("long annotation should be filtered out due to default 64-char size cap")
	}
}

func TestExtractAnnotations_Disabled(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "ann-uid3",
				"name":              "ann-pod3",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"annotations": map[string]any{
					"note": "hello",
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:                schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind:               "Pod",
		ExtractAnnotations: false,
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	fields := extractedFields(t, report)
	if _, exists := fields["annotations"]; exists {
		t.Error("annotations should not be in extracted when ExtractAnnotations is false")
	}
	// Full source annotations still appear in observation metadata.
	meta, _ := mustObservation(t, report)["metadata"].(map[string]any)
	annotations, _ := meta["annotations"].(map[string]any)
	if annotations["note"] != "hello" {
		t.Errorf("metadata.annotations = %#v, want note=hello", annotations)
	}
}

func TestComputeExtra_HookInvocation(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "hook-uid",
				"name":              "hook-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"phase": "Running",
			},
		},
	}

	hookCalled := false
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		ComputeExtra: func(r *unstructured.Unstructured, fields map[string]any) {
			hookCalled = true
			fields["computedStatus"] = "computed-value"
		},
	}

	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if !hookCalled {
		t.Error("ComputeExtra hook was not called")
	}

	fields := extractedFields(t, report)
	computed, ok := fields["computedStatus"]
	if !ok {
		t.Fatal("extracted missing 'computedStatus' field added by hook")
	}
	if computed.(string) != "computed-value" {
		t.Errorf("computedStatus = %q, want %q", computed, "computed-value")
	}
}

func TestInventoryNode_Fields(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "node-uid",
				"name":              "node-deploy",
				"namespace":         "kube-system",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"generation":        int64(3),
				"labels": map[string]any{
					"app": "web",
				},
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "DaemonSet",
						"name":       "my-ds",
						"uid":        "ds-uid",
						"controller": true,
					},
				},
			},
			"spec": map[string]any{
				"replicas": int64(5),
			},
		},
	}

	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		Kind: "Deployment",
		Fields: []FieldExtraction{
			{Name: "replicas", JSONPath: ".spec.replicas", DataType: DataTypeNumber},
		},
	}

	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if node.UID != "node-uid" {
		t.Errorf("node.UID = %q, want %q", node.UID, "node-uid")
	}
	if node.Kind != "Deployment" {
		t.Errorf("node.Kind = %q, want %q", node.Kind, "Deployment")
	}
	if node.Name != "node-deploy" {
		t.Errorf("node.Name = %q, want %q", node.Name, "node-deploy")
	}
	if node.Namespace != "kube-system" {
		t.Errorf("node.Namespace = %q, want %q", node.Namespace, "kube-system")
	}
	if node.OwnerUID != "ds-uid" {
		t.Errorf("node.OwnerUID = %q, want %q", node.OwnerUID, "ds-uid")
	}
	if node.Labels["app"] != "web" {
		t.Errorf("node.Labels[app] = %q, want %q", node.Labels["app"], "web")
	}

	// Properties mirrors extracted (schema fields / ComputeExtra), not
	// observation metadata such as generation.
	if node.Properties["replicas"].(float64) != 5 {
		t.Errorf("node.Properties[replicas] = %v, want 5", node.Properties["replicas"])
	}
	if _, ok := node.Properties["generation"]; ok {
		t.Error("node.Properties should not carry generation; that lives in observation metadata")
	}
}

func TestExtractAnnotations_DoesNotMutateSource(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"uid":               "uid-1",
				"name":              "test",
				"namespace":         "default",
				"resourceVersion":   "100",
				"creationTimestamp": "2025-06-01T12:00:00Z",
				"annotations": map[string]any{
					"keep": "short",
					"kubectl.kubernetes.io/last-applied-configuration": `{"large":"config"}`,
				},
			},
		},
	}

	entry := SchemaEntry{
		GVR:                schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"},
		ExtractAnnotations: true,
	}
	mustExtract(t, r, entry, testClusterResourceName("target-1"))

	annotations := r.GetAnnotations()
	if _, ok := annotations["kubectl.kubernetes.io/last-applied-configuration"]; !ok {
		t.Fatal("source annotations mutated: kubectl annotation was deleted")
	}
	if _, ok := annotations["keep"]; !ok {
		t.Fatal("source annotations mutated: keep annotation was deleted")
	}
}

func TestExtractObservedResource_PreservesGVR(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               "deploy-uid",
				"name":              "my-deploy",
				"namespace":         "default",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	entry := SchemaEntry{
		GVR:  gvr,
		Kind: "Deployment",
	}

	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if node.GVR != gvr {
		t.Errorf("node.GVR = %v, want %v", node.GVR, gvr)
	}
}

func TestExtractObservedResource_EmptyUIDRejected(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":              "no-uid",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, _, err := ExtractObservedResource(r, entry, testClusterResourceName("target-1"))
	if err == nil {
		t.Fatal("expected error for empty UID")
	}
}

func TestExtractObservedResource_RejectsNonFlatClusterResourceName(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "pod-uid",
				"name":              "my-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, _, err := ExtractObservedResource(r, entry, domain.ResourceName("clusters/prod/us-east-1"))
	if err == nil {
		t.Fatal("expected error for non-flat cluster resource name")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

func TestExtractObservedResource_EmptyClusterResourceNameRejected(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "pod-uid",
				"name":              "my-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, _, err := ExtractObservedResource(r, entry, domain.ResourceName(""))
	if err == nil {
		t.Fatal("expected error for empty cluster resource name")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
}

func TestExtractObservedResource_ClusterScoped(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]any{
				"uid":               "node-uid",
				"name":              "worker-1",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
		Kind: "Node",
	}
	report, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))

	if report.Labels["k8s.scope"] != "cluster" {
		t.Errorf("Labels[k8s.scope] = %q, want cluster", report.Labels["k8s.scope"])
	}
	if _, ok := report.Labels["k8s.namespace"]; ok {
		t.Errorf("Labels should omit k8s.namespace for cluster-scoped objects, got %q", report.Labels["k8s.namespace"])
	}
	if node.Namespace != "" {
		t.Errorf("node.Namespace = %q, want empty", node.Namespace)
	}
	if node.Labels != nil {
		t.Errorf("node.Labels = %#v, want nil when object has no labels", node.Labels)
	}
	obs := mustObservation(t, report)
	gvr, _ := obs["gvr"].(map[string]any)
	if gvr["scope"] != "cluster" {
		t.Errorf("observation gvr.scope = %v, want cluster", gvr["scope"])
	}
	meta, _ := obs["metadata"].(map[string]any)
	if meta["namespace"] != "" {
		t.Errorf("metadata.namespace = %v, want empty string", meta["namespace"])
	}
	wantName := "clusters/target-1/apiResources/core~v1~nodes/objects/node-uid"
	if string(report.Name) != wantName {
		t.Errorf("Name = %q, want %q", report.Name, wantName)
	}
}

func TestExtractObservedResource_ObservedAtSet(t *testing.T) {
	before := time.Now()
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "obs-uid",
				"name":              "obs-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	after := time.Now()

	if report.ObservedAt.Before(before) || report.ObservedAt.After(after) {
		t.Errorf("ObservedAt = %v, want between %v and %v", report.ObservedAt, before, after)
	}
	if report.Observation == nil {
		t.Fatal("Observation must always be set, even with empty extracted")
	}
	fields := extractedFields(t, report)
	if len(fields) != 0 {
		t.Errorf("extracted = %#v, want empty object when no schema fields apply", fields)
	}
}

func TestExtractOwnerReferences_MalformedEntriesIgnored(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "pod-uid",
				"name":              "my-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"ownerReferences": []any{
					"not-a-map",
					map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "ReplicaSet",
						"name":       "bad-uid-type",
						"uid":        float64(12345), // non-string uid
						"controller": true,
					},
					map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "ReplicaSet",
						"name":       "my-rs",
						"uid":        "rs-uid",
						"controller": true,
					},
				},
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	if node.OwnerUID != "rs-uid" {
		t.Errorf("OwnerUID = %q, want rs-uid (skip malformed entries, keep last valid controller)", node.OwnerUID)
	}
}

func TestExtractOwnerReferences_LastControllerWins(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "pod-uid",
				"name":              "my-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
				"ownerReferences": []any{
					map[string]any{
						"uid":        "first-controller",
						"controller": true,
					},
					map[string]any{
						"uid":        "second-controller",
						"controller": true,
					},
				},
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
	}
	_, node := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	if node.OwnerUID != "second-controller" {
		t.Errorf("OwnerUID = %q, want second-controller (last controller wins)", node.OwnerUID)
	}
}

func TestExtractConditions_AbsentAndMalformed(t *testing.T) {
	t.Run("NoStatus", func(t *testing.T) {
		r := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"uid":               "no-status-uid",
					"name":              "no-status",
					"namespace":         "default",
					"creationTimestamp": "2025-01-01T00:00:00Z",
				},
			},
		}
		entry := SchemaEntry{
			GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			Kind: "Pod",
		}
		report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
		if len(report.Conditions) != 0 {
			t.Fatalf("Conditions = %#v, want empty when status is absent", report.Conditions)
		}
	})

	t.Run("NonMapEntriesSkipped", func(t *testing.T) {
		r := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"uid":               "bad-cond-uid",
					"name":              "bad-cond",
					"namespace":         "default",
					"creationTimestamp": "2025-01-01T00:00:00Z",
				},
				"status": map[string]any{
					"conditions": []any{
						"not-a-map",
						map[string]any{
							"type":   "Ready",
							"status": "True",
							"reason": float64(42), // non-string reason becomes ""
						},
					},
				},
			},
		}
		entry := SchemaEntry{
			GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			Kind: "Pod",
		}
		report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
		if len(report.Conditions) != 1 {
			t.Fatalf("Conditions = %#v, want one Ready after skipping non-map entry", report.Conditions)
		}
		if report.Conditions[0].Type() != "Ready" || report.Conditions[0].Reason() != "" {
			t.Fatalf("condition = type=%s reason=%q, want Ready with empty reason", report.Conditions[0].Type(), report.Conditions[0].Reason())
		}
	})

	t.Run("FalseAndUnknownStatuses", func(t *testing.T) {
		r := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"uid":               "status-uid",
					"name":              "status-deploy",
					"namespace":         "default",
					"creationTimestamp": "2025-01-01T00:00:00Z",
				},
				"status": map[string]any{
					"conditions": []any{
						map[string]any{"type": "Available", "status": "False"},
						map[string]any{"type": "Progressing", "status": "Unknown"},
					},
				},
			},
		}
		entry := SchemaEntry{
			GVR:  schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			Kind: "Deployment",
		}
		report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
		if len(report.Conditions) != 2 {
			t.Fatalf("Conditions len = %d, want 2", len(report.Conditions))
		}
		if report.Conditions[0].Status() != domain.ConditionFalse {
			t.Errorf("Conditions[0].Status = %q, want False", report.Conditions[0].Status())
		}
		if report.Conditions[1].Status() != domain.ConditionUnknown {
			t.Errorf("Conditions[1].Status = %q, want Unknown", report.Conditions[1].Status())
		}
	})
}

func TestExtractAnnotations_EmptyAndFullyFiltered(t *testing.T) {
	t.Run("NoAnnotations", func(t *testing.T) {
		r := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"uid":               "no-ann-uid",
					"name":              "no-ann",
					"namespace":         "default",
					"creationTimestamp": "2025-01-01T00:00:00Z",
				},
			},
		}
		entry := SchemaEntry{
			GVR:                schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			Kind:               "Pod",
			ExtractAnnotations: true,
		}
		report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
		if _, ok := extractedFields(t, report)["annotations"]; ok {
			t.Error("extracted should omit annotations when the object has none")
		}
	})

	t.Run("AllFilteredOut", func(t *testing.T) {
		r := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]any{
					"uid":               "all-filtered-uid",
					"name":              "all-filtered",
					"namespace":         "default",
					"creationTimestamp": "2025-01-01T00:00:00Z",
					"annotations": map[string]any{
						"kubectl.kubernetes.io/last-applied-configuration": `{"apiVersion":"v1"}`,
						"long": "this annotation is longer than the twenty character size cap",
					},
				},
			},
		}
		entry := SchemaEntry{
			GVR:                schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			Kind:               "Pod",
			ExtractAnnotations: true,
			AnnotationSizeCap:  20,
		}
		report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
		if _, ok := extractedFields(t, report)["annotations"]; ok {
			t.Error("extracted should omit annotations when every annotation is filtered out")
		}
	})
}

func TestExtractObservedFields_NumberCoercion(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "num-uid",
				"name":              "num-pod",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"intField":        7,
				"floatField":      1.5,
				"stringNumber":    "42",
				"stringNotNumber": "abc",
				"boolField":       true,
				"int64Field":      int64(9),
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "intField", JSONPath: ".spec.intField", DataType: DataTypeNumber},
			{Name: "floatField", JSONPath: ".spec.floatField", DataType: DataTypeNumber},
			{Name: "stringNumber", JSONPath: ".spec.stringNumber", DataType: DataTypeNumber},
			{Name: "stringNotNumber", JSONPath: ".spec.stringNotNumber", DataType: DataTypeNumber},
			{Name: "boolField", JSONPath: ".spec.boolField", DataType: DataTypeNumber},
			{Name: "int64Field", JSONPath: ".spec.int64Field", DataType: DataTypeNumber},
		},
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	if fields["intField"].(float64) != 7 {
		t.Errorf("intField = %v, want 7", fields["intField"])
	}
	if fields["floatField"].(float64) != 1.5 {
		t.Errorf("floatField = %v, want 1.5", fields["floatField"])
	}
	if fields["stringNumber"].(float64) != 42 {
		t.Errorf("stringNumber = %v, want 42", fields["stringNumber"])
	}
	if fields["int64Field"].(float64) != 9 {
		t.Errorf("int64Field = %v, want 9", fields["int64Field"])
	}
	if _, ok := fields["stringNotNumber"]; ok {
		t.Error("stringNotNumber should be skipped when it cannot parse as float")
	}
	if _, ok := fields["boolField"]; ok {
		t.Error("boolField should be skipped for DataTypeNumber")
	}

	// Also cover coerceNumber's int branch directly in case JSONPath
	// normalizes numeric literals differently across Go versions.
	if got := coerceNumber(7); got != float64(7) {
		t.Errorf("coerceNumber(int) = %v, want 7", got)
	}
}

func TestExtractObservedFields_BytesFailures(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]any{
				"uid":               "bytes-fail-uid",
				"name":              "bytes-fail",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"allocatable": map[string]any{
					"memory":    "not-a-quantity",
					"cpu":       int64(2),
					"ephemeral": "1Gi",
				},
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
		Kind: "Node",
		Fields: []FieldExtraction{
			{Name: "badQuantity", JSONPath: ".status.allocatable.memory", DataType: DataTypeBytes},
			{Name: "nonString", JSONPath: ".status.allocatable.cpu", DataType: DataTypeBytes},
			{Name: "good", JSONPath: ".status.allocatable.ephemeral", DataType: DataTypeBytes},
		},
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	if _, ok := fields["badQuantity"]; ok {
		t.Error("badQuantity should be skipped for unparseable quantity")
	}
	if _, ok := fields["nonString"]; ok {
		t.Error("nonString should be skipped when DataTypeBytes value is not a string")
	}
	if fields["good"].(float64) != float64(1<<30) {
		t.Errorf("good = %v, want %v", fields["good"], float64(1<<30))
	}
}

func TestExtractObservedFields_MapStringFailures(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "map-fail-uid",
				"name":              "map-fail",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"notAMap":     "string",
				"emptyMap":    map[string]any{},
				"mixedValues": map[string]any{"n": int64(3), "b": true},
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "notAMap", JSONPath: ".spec.notAMap", DataType: DataTypeMapString},
			{Name: "emptyMap", JSONPath: ".spec.emptyMap", DataType: DataTypeMapString},
			{Name: "mixedValues", JSONPath: ".spec.mixedValues", DataType: DataTypeMapString},
		},
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)

	if _, ok := fields["notAMap"]; ok {
		t.Error("notAMap should be skipped when value is not a map")
	}
	if _, ok := fields["emptyMap"]; ok {
		t.Error("emptyMap should be skipped")
	}
	mixed, ok := fields["mixedValues"].(map[string]any)
	if !ok {
		t.Fatalf("mixedValues missing or wrong type: %#v", fields["mixedValues"])
	}
	if mixed["n"] != "3" || mixed["b"] != "true" {
		t.Errorf("mixedValues = %#v, want stringified values", mixed)
	}
}

func TestExtractObservedFields_InvalidJSONPathSkipped(t *testing.T) {
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "bad-jp-uid",
				"name":              "bad-jp",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"status": map[string]any{
				"phase": "Running",
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "bad", JSONPath: ".status.phase[", DataType: DataTypeString},
			{Name: "phase", JSONPath: ".status.phase"},
		},
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	fields := extractedFields(t, report)
	if _, ok := fields["bad"]; ok {
		t.Error("invalid JSONPath should be skipped")
	}
	if fields["phase"] != "Running" {
		t.Errorf("phase = %v, want Running", fields["phase"])
	}
}

func TestExtractObservedFields_SliceNestedFlattening(t *testing.T) {
	// Direct unit coverage for collectSlice's nested-[]any branch, which
	// JSONPath results do not normally produce for simple field paths.
	got := collectSlice([][]reflect.Value{{
		reflect.ValueOf([]any{"a", "b"}),
		reflect.ValueOf("c"),
	}})
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("collectSlice nested flatten = %#v, want [a b c]", got)
	}

	if collectSlice([][]reflect.Value{{}}) != nil {
		t.Fatal("collectSlice of empty results should return nil")
	}
}

func TestExtractObservedFields_EmptySliceSkipped(t *testing.T) {
	// An empty list at the JSONPath result becomes a typed-nil []any from
	// collectSlice; extractSingleField must convert that to a true nil any
	// so the field is omitted from extracted (same typed-nil concern as
	// DataTypeMapString).
	r := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "empty-slice-uid",
				"name":              "empty-slice",
				"namespace":         "default",
				"creationTimestamp": "2025-01-01T00:00:00Z",
			},
			"spec": map[string]any{
				"emptyList": []any{},
			},
		},
	}
	entry := SchemaEntry{
		GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		Kind: "Pod",
		Fields: []FieldExtraction{
			{Name: "emptyList", JSONPath: ".spec.emptyList", DataType: DataTypeSlice},
		},
	}
	report, _ := mustExtract(t, r, entry, testClusterResourceName("target-1"))
	if _, ok := extractedFields(t, report)["emptyList"]; ok {
		t.Error("emptyList should be skipped when the JSONPath result flattens to nothing")
	}
}
