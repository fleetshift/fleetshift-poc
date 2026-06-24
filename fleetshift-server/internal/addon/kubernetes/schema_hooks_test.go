package kubernetes

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestComputePodStatus(t *testing.T) {
	tests := []struct {
		name     string
		pod      *unstructured.Unstructured
		expected string
	}{
		{
			name: "Running pod",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []any{
							map[string]any{
								"state": map[string]any{
									"running": map[string]any{
										"startedAt": "2025-01-01T00:00:00Z",
									},
								},
							},
						},
					},
				},
			},
			expected: "Running",
		},
		{
			name: "CrashLoopBackOff",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []any{
							map[string]any{
								"state": map[string]any{
									"waiting": map[string]any{
										"reason": "CrashLoopBackOff",
									},
								},
							},
						},
					},
				},
			},
			expected: "CrashLoopBackOff",
		},
		{
			name: "Init container failed",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"phase": "Pending",
						"initContainerStatuses": []any{
							map[string]any{
								"state": map[string]any{
									"terminated": map[string]any{
										"exitCode": int64(1),
										"reason":   "Error",
									},
								},
							},
						},
					},
				},
			},
			expected: "Init:Error",
		},
		{
			name: "Terminating pod",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"deletionTimestamp": "2025-01-01T00:00:00Z",
					},
					"status": map[string]any{
						"phase": "Running",
					},
				},
			},
			expected: "Terminating",
		},
		{
			name: "NodeLost becomes Unknown",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"deletionTimestamp": "2025-01-01T00:00:00Z",
					},
					"status": map[string]any{
						"phase":  "Running",
						"reason": "NodeLost",
					},
				},
			},
			expected: "Unknown",
		},
		{
			name: "Container with exit code",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"phase": "Failed",
						"containerStatuses": []any{
							map[string]any{
								"state": map[string]any{
									"terminated": map[string]any{
										"exitCode": int64(137),
									},
								},
							},
						},
					},
				},
			},
			expected: "ExitCode:137",
		},
		{
			name: "Container with signal",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"phase": "Failed",
						"containerStatuses": []any{
							map[string]any{
								"state": map[string]any{
									"terminated": map[string]any{
										"signal": int64(9),
									},
								},
							},
						},
					},
				},
			},
			expected: "Signal:9",
		},
		{
			name: "Completed with running container",
			pod: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"phase": "Completed",
						"containerStatuses": []any{
							map[string]any{
								"state": map[string]any{
									"running": map[string]any{
										"startedAt": "2025-01-01T00:00:00Z",
									},
								},
							},
						},
					},
				},
			},
			expected: "Running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := make(map[string]any)
			computePodStatus(tt.pod, fields)

			status, ok := fields["status"]
			if !ok {
				t.Fatalf("status field not set")
			}

			if status != tt.expected {
				t.Errorf("expected status=%q, got %q", tt.expected, status)
			}
		})
	}
}

func TestBuildPodEdges(t *testing.T) {
	pod := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"namespace": "default",
				"uid":       "pod-1",
			},
			"spec": map[string]any{
				"nodeName": "node-1",
				"volumes": []any{
					map[string]any{
						"secret": map[string]any{
							"secretName": "secret-1",
						},
					},
					map[string]any{
						"configMap": map[string]any{
							"name": "cm-1",
						},
					},
					map[string]any{
						"persistentVolumeClaim": map[string]any{
							"claimName": "pvc-1",
						},
					},
				},
				"containers": []any{
					map[string]any{
						"env": []any{
							map[string]any{
								"valueFrom": map[string]any{
									"secretKeyRef": map[string]any{
										"name": "secret-2",
									},
								},
							},
							map[string]any{
								"valueFrom": map[string]any{
									"configMapKeyRef": map[string]any{
										"name": "cm-2",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Build the edge factory closure
	edgeFunc := buildPodEdges(pod, "pod-1")

	// Create a NodeStore with the referenced resources
	ns := buildNodeStore(map[string]inventoryNode{
		"node-1": {
			UID:       "node-1",
			Kind:      "Node",
			Name:      "node-1",
			Namespace: "",
		},
		"secret-1": {
			UID:       "secret-1",
			Kind:      "Secret",
			Name:      "secret-1",
			Namespace: "default",
		},
		"secret-2": {
			UID:       "secret-2",
			Kind:      "Secret",
			Name:      "secret-2",
			Namespace: "default",
		},
		"cm-1": {
			UID:       "cm-1",
			Kind:      "ConfigMap",
			Name:      "cm-1",
			Namespace: "default",
		},
		"cm-2": {
			UID:       "cm-2",
			Kind:      "ConfigMap",
			Name:      "cm-2",
			Namespace: "default",
		},
		"pvc-1": {
			UID:       "pvc-1",
			Kind:      "PersistentVolumeClaim",
			Name:      "pvc-1",
			Namespace: "default",
		},
	})

	// Call the closure to build edges
	edges := edgeFunc(ns)

	// Verify edges
	if len(edges) != 6 {
		t.Fatalf("expected 6 edges, got %d", len(edges))
	}

	// Check runsOn edge
	var foundRunsOn bool
	for _, e := range edges {
		if e.EdgeType == EdgeRunsOn && e.SourceUID == "pod-1" && e.DestUID == "node-1" {
			foundRunsOn = true
			if e.SourceKind != "Pod" || e.DestKind != "Node" {
				t.Errorf("runsOn edge has wrong kinds: %s -> %s", e.SourceKind, e.DestKind)
			}
		}
	}
	if !foundRunsOn {
		t.Error("runsOn edge to node-1 not found")
	}

	// Check attachedTo edges
	expectedAttachments := map[string]string{
		"secret-1": "Secret",
		"secret-2": "Secret",
		"cm-1":     "ConfigMap",
		"cm-2":     "ConfigMap",
		"pvc-1":    "PersistentVolumeClaim",
	}

	foundAttachments := make(map[string]bool)
	for _, e := range edges {
		if e.EdgeType == EdgeAttachedTo && e.SourceUID == "pod-1" {
			expectedKind, exists := expectedAttachments[e.DestUID]
			if !exists {
				t.Errorf("unexpected attachedTo edge to %s", e.DestUID)
			}
			if e.DestKind != expectedKind {
				t.Errorf("attachedTo edge to %s has wrong kind: expected %s, got %s",
					e.DestUID, expectedKind, e.DestKind)
			}
			foundAttachments[e.DestUID] = true
		}
	}

	for destUID := range expectedAttachments {
		if !foundAttachments[destUID] {
			t.Errorf("missing attachedTo edge to %s", destUID)
		}
	}
}

func TestBuildServiceEdges(t *testing.T) {
	service := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"namespace": "default",
			},
			"spec": map[string]any{
				"selector": map[string]any{
					"app":  "web",
					"tier": "frontend",
				},
			},
		},
	}

	// Build the edge factory closure
	edgeFunc := buildServiceEdges(service, "svc-1")

	// Create pods with various labels
	ns := buildNodeStore(map[string]inventoryNode{
		"pod-1": {
			UID: "pod-1", Kind: "Pod", Name: "pod-1", Namespace: "default",
			Labels: map[string]string{"app": "web", "tier": "frontend"},
		},
		"pod-2": {
			UID: "pod-2", Kind: "Pod", Name: "pod-2", Namespace: "default",
			Labels: map[string]string{"app": "web"}, // missing "tier"
		},
		"pod-3": {
			UID: "pod-3", Kind: "Pod", Name: "pod-3", Namespace: "default",
			Labels: map[string]string{"app": "web", "tier": "backend"}, // wrong tier
		},
		"pod-4": {
			UID: "pod-4", Kind: "Pod", Name: "pod-4", Namespace: "other",
			Labels: map[string]string{"app": "web", "tier": "frontend"}, // wrong namespace
		},
	})

	edges := edgeFunc(ns)

	// Should only select pod-1 (correct labels and namespace)
	if len(edges) != 1 {
		t.Fatalf("expected 1 selects edge, got %d", len(edges))
	}

	if edges[0].EdgeType != EdgeSelects {
		t.Errorf("expected EdgeSelects, got %s", edges[0].EdgeType)
	}

	if edges[0].SourceUID != "svc-1" || edges[0].DestUID != "pod-1" {
		t.Errorf("expected edge from svc-1 to pod-1, got %s -> %s",
			edges[0].SourceUID, edges[0].DestUID)
	}

	if edges[0].SourceKind != "Service" || edges[0].DestKind != "Pod" {
		t.Errorf("expected Service -> Pod kinds, got %s -> %s",
			edges[0].SourceKind, edges[0].DestKind)
	}
}

func TestBuildPVCEdges(t *testing.T) {
	pvc := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"volumeName": "pv-1",
			},
		},
	}

	edgeFunc := buildPVCEdges(pvc, "pvc-1")

	ns := buildNodeStore(map[string]inventoryNode{
		"pv-1": {
			UID:       "pv-1",
			Kind:      "PersistentVolume",
			Name:      "pv-1",
			Namespace: "",
		},
	})

	edges := edgeFunc(ns)

	if len(edges) != 1 {
		t.Fatalf("expected 1 attachedTo edge, got %d", len(edges))
	}

	if edges[0].EdgeType != EdgeAttachedTo {
		t.Errorf("expected EdgeAttachedTo, got %s", edges[0].EdgeType)
	}

	if edges[0].SourceUID != "pvc-1" || edges[0].DestUID != "pv-1" {
		t.Errorf("expected edge from pvc-1 to pv-1, got %s -> %s",
			edges[0].SourceUID, edges[0].DestUID)
	}

	if edges[0].SourceKind != "PersistentVolumeClaim" || edges[0].DestKind != "PersistentVolume" {
		t.Errorf("expected PersistentVolumeClaim -> PersistentVolume kinds, got %s -> %s",
			edges[0].SourceKind, edges[0].DestKind)
	}
}

func TestBuildPVCEdges_NoVolumeName(t *testing.T) {
	pvc := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{},
		},
	}

	edgeFunc := buildPVCEdges(pvc, "pvc-1")

	ns := buildNodeStore(map[string]inventoryNode{})
	edges := edgeFunc(ns)

	if len(edges) != 0 {
		t.Fatalf("expected 0 edges for unbound PVC, got %d", len(edges))
	}
}

func TestComputeNodeRoles(t *testing.T) {
	tests := []struct {
		name     string
		node     *unstructured.Unstructured
		expected string
	}{
		{
			name: "Control plane node",
			node: &unstructured.Unstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"node-role.kubernetes.io/control-plane": "",
						},
					},
				},
			},
			expected: "control-plane",
		},
		{
			name: "Worker node",
			node: &unstructured.Unstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"node-role.kubernetes.io/worker": "",
						},
					},
				},
			},
			expected: "worker",
		},
		{
			name: "Multiple roles",
			node: &unstructured.Unstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"node-role.kubernetes.io/control-plane": "",
							"node-role.kubernetes.io/master":        "",
						},
					},
				},
			},
			expected: "control-plane,master", // Sorted alphabetically
		},
		{
			name: "No role labels",
			node: &unstructured.Unstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"kubernetes.io/hostname": "node-1",
						},
					},
				},
			},
			expected: "worker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := make(map[string]any)
			computeNodeRoles(tt.node, fields)

			role, ok := fields["role"]
			if !ok {
				t.Fatalf("role field not set")
			}

			roleStr, ok := role.(string)
			if !ok {
				t.Fatalf("role field is not a string: %T", role)
			}

			// Roles are now sorted, so we can check exact match
			if roleStr != tt.expected {
				t.Errorf("expected role=%q, got %q", tt.expected, roleStr)
			}
		})
	}
}

func TestMatchesSelector(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		selector map[string]string
		expected bool
	}{
		{
			name:     "Exact match",
			labels:   map[string]string{"app": "web", "tier": "frontend"},
			selector: map[string]string{"app": "web", "tier": "frontend"},
			expected: true,
		},
		{
			name:     "Subset match",
			labels:   map[string]string{"app": "web", "tier": "frontend", "env": "prod"},
			selector: map[string]string{"app": "web", "tier": "frontend"},
			expected: true,
		},
		{
			name:     "Missing label",
			labels:   map[string]string{"app": "web"},
			selector: map[string]string{"app": "web", "tier": "frontend"},
			expected: false,
		},
		{
			name:     "Wrong value",
			labels:   map[string]string{"app": "web", "tier": "backend"},
			selector: map[string]string{"app": "web", "tier": "frontend"},
			expected: false,
		},
		{
			name:     "Empty selector",
			labels:   map[string]string{"app": "web"},
			selector: map[string]string{},
			expected: false,
		},
		{
			name:     "No labels",
			labels:   nil,
			selector: map[string]string{"app": "web"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesSelector(tt.labels, tt.selector)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestServiceEdges_ThroughExtraction exercises the full extraction→edge
// pipeline: build real inventoryNodes via ExtractObservedResource, then
// run the Service edge closure against them. This catches type mismatches
// between what extraction produces and what edge builders expect.
func TestServiceEdges_ThroughExtraction(t *testing.T) {
	svcGVR := schema.GroupVersionResource{Version: "v1", Resource: "services"}
	podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	schemaEntries := map[schema.GroupVersionResource]SchemaEntry{
		svcGVR: {GVR: svcGVR, Kind: "Service", BuildEdges: buildServiceEdges},
		podGVR: {GVR: podGVR, Kind: "Pod"},
	}

	svc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{
			"uid": "svc-uid", "name": "my-svc", "namespace": "default",
			"resourceVersion": "1", "creationTimestamp": "2025-01-01T00:00:00Z",
		},
		"spec": map[string]any{
			"selector": map[string]any{"app": "web"},
			"ports":    []any{map[string]any{"port": int64(80)}},
		},
	}}

	matchingPod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "pod-match", "name": "web-pod", "namespace": "default",
			"resourceVersion": "2", "creationTimestamp": "2025-01-01T00:00:00Z",
			"labels": map[string]any{"app": "web", "version": "v1"},
		},
	}}

	nonMatchingPod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "pod-other", "name": "db-pod", "namespace": "default",
			"resourceVersion": "3", "creationTimestamp": "2025-01-01T00:00:00Z",
			"labels": map[string]any{"app": "db"},
		},
	}}

	_, svcNode := ExtractObservedResource(svc, schemaEntries[svcGVR], "t1")
	_, podNode1 := ExtractObservedResource(matchingPod, schemaEntries[podGVR], "t1")
	_, podNode2 := ExtractObservedResource(nonMatchingPod, schemaEntries[podGVR], "t1")

	ns := buildNodeStore(map[string]inventoryNode{
		svcNode.UID:  svcNode,
		podNode1.UID: podNode1,
		podNode2.UID: podNode2,
	})

	edgeFn := buildServiceEdges(svc, svcNode.UID)
	edges := edgeFn(ns)

	if len(edges) != 1 {
		t.Fatalf("expected 1 selects edge, got %d", len(edges))
	}
	if edges[0].DestUID != "pod-match" {
		t.Errorf("expected edge to pod-match, got %s", edges[0].DestUID)
	}
	if edges[0].EdgeType != EdgeSelects {
		t.Errorf("expected EdgeSelects, got %s", edges[0].EdgeType)
	}
}

// TestServiceEdges_ThroughExtraction_NoMatch verifies that a Service with
// a selector that doesn't match any pod labels produces no edges when
// going through the real extraction pipeline.
func TestServiceEdges_ThroughExtraction_NoMatch(t *testing.T) {
	svcGVR := schema.GroupVersionResource{Version: "v1", Resource: "services"}
	podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	schemaEntries := map[schema.GroupVersionResource]SchemaEntry{
		svcGVR: {GVR: svcGVR, Kind: "Service", BuildEdges: buildServiceEdges},
		podGVR: {GVR: podGVR, Kind: "Pod"},
	}

	svc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{
			"uid": "svc-uid", "name": "my-svc", "namespace": "default",
			"resourceVersion": "1", "creationTimestamp": "2025-01-01T00:00:00Z",
		},
		"spec": map[string]any{
			"selector": map[string]any{"app": "web"},
		},
	}}

	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "pod-1", "name": "db-pod", "namespace": "default",
			"resourceVersion": "2", "creationTimestamp": "2025-01-01T00:00:00Z",
			"labels": map[string]any{"app": "db"},
		},
	}}

	_, svcNode := ExtractObservedResource(svc, schemaEntries[svcGVR], "t1")
	_, podNode := ExtractObservedResource(pod, schemaEntries[podGVR], "t1")

	ns := buildNodeStore(map[string]inventoryNode{
		svcNode.UID: svcNode,
		podNode.UID: podNode,
	})

	edgeFn := buildServiceEdges(svc, svcNode.UID)
	edges := edgeFn(ns)

	if len(edges) != 0 {
		t.Fatalf("expected 0 edges for non-matching pod, got %d", len(edges))
	}
}

func TestDefaultSchemaWithHooks(t *testing.T) {
	schema := DefaultKubernetesSchema()

	// Verify Pod schema has hooks
	podGVR := schema.Entries[podGVR()]
	if podGVR.ComputeExtra == nil {
		t.Error("Pod schema missing ComputeExtra hook")
	}
	if podGVR.BuildEdges == nil {
		t.Error("Pod schema missing BuildEdges hook")
	}

	// Verify Service schema has BuildEdges
	svcGVR := schema.Entries[serviceGVR()]
	if svcGVR.BuildEdges == nil {
		t.Error("Service schema missing BuildEdges hook")
	}

	// Verify Node schema has ComputeExtra
	nodeGVR := schema.Entries[nodeGVR()]
	if nodeGVR.ComputeExtra == nil {
		t.Error("Node schema missing ComputeExtra hook")
	}

	// Verify PVC schema has BuildEdges
	pvcGVR := schema.Entries[pvcGVR()]
	if pvcGVR.BuildEdges == nil {
		t.Error("PVC schema missing BuildEdges hook")
	}

	// Verify Node schema has ipAddress field
	nodeFields := nodeGVR.Fields
	var foundIPField bool
	for _, field := range nodeFields {
		if field.Name == "ipAddress" {
			foundIPField = true
			expectedPath := `.status.addresses[?(@.type=="InternalIP")].address`
			if field.JSONPath != expectedPath {
				t.Errorf("Node ipAddress field has wrong JSONPath: expected %q, got %q",
					expectedPath, field.JSONPath)
			}
		}
	}
	if !foundIPField {
		t.Error("Node schema missing ipAddress field")
	}
}

// Helper functions to get GVRs
func podGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
}

func serviceGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
}

func nodeGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
}

func pvcGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
}

// TestEdgeTypesConstant verifies that edge types match expected constants
func TestEdgeTypesConstant(t *testing.T) {
	tests := []struct {
		name     string
		edgeType EdgeType
		expected string
	}{
		{"OwnedBy", EdgeOwnedBy, "ownedBy"},
		{"RunsOn", EdgeRunsOn, "runsOn"},
		{"AttachedTo", EdgeAttachedTo, "attachedTo"},
		{"Selects", EdgeSelects, "selects"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.edgeType) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.edgeType)
			}
		})
	}
}
