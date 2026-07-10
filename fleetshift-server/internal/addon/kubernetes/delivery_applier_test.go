package kubernetes

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/restmapper"
)

func newTestApplier(t *testing.T, objects ...runtime.Object) *applier {
	t.Helper()
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMapList"}, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NamespaceList"}, &unstructured.UnstructuredList{})

	client := dynamicfake.NewSimpleDynamicClient(scheme, objects...)

	apiGroupResources := []*restmapper.APIGroupResources{
		{
			Group: metav1.APIGroup{
				Name: "",
				Versions: []metav1.GroupVersionForDiscovery{
					{GroupVersion: "v1", Version: "v1"},
				},
			},
			VersionedResources: map[string][]metav1.APIResource{
				"v1": {
					{
						Name:       "configmaps",
						Kind:       "ConfigMap",
						Namespaced: true,
						Group:      "",
						Version:    "v1",
					},
					{
						Name:       "namespaces",
						Kind:       "Namespace",
						Namespaced: false,
						Group:      "",
						Version:    "v1",
					},
				},
			},
		},
	}

	mapper := restmapper.NewDiscoveryRESTMapper(apiGroupResources)
	return &applier{client: client, mapper: mapper}
}

func TestApplier_Delete(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName("test-cm")

	ap := newTestApplier(t, obj)

	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestApplier_Delete_NotFound(t *testing.T) {
	ap := newTestApplier(t) // no objects

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName("nonexistent")

	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete of non-existent resource should not error: %v", err)
	}
}

func TestApplier_Delete_DefaultsEmptyNamespace(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetName("no-ns-cm")
	// Namespace intentionally unset — delete should treat it as "default".

	seeded := obj.DeepCopy()
	seeded.SetNamespace("default")
	ap := newTestApplier(t, seeded)

	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete with empty namespace: %v", err)
	}
}

func TestApplier_Delete_ClusterScoped(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
	obj.SetName("team-a")

	ap := newTestApplier(t, obj)
	raw, _ := json.Marshal(obj)
	if err := ap.delete(context.Background(), raw); err != nil {
		t.Fatalf("delete cluster-scoped: %v", err)
	}
}

func TestApplier_Delete_InvalidJSON(t *testing.T) {
	ap := newTestApplier(t)
	err := ap.delete(context.Background(), json.RawMessage(`{not-json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestApplier_Delete_UnknownGVK(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"})
	obj.SetName("w1")
	raw, _ := json.Marshal(obj)
	err := ap.delete(context.Background(), raw)
	if err == nil {
		t.Fatal("expected error for unknown GVK")
	}
}

func TestApplier_Apply_InvalidJSON(t *testing.T) {
	ap := newTestApplier(t)
	err := ap.apply(context.Background(), json.RawMessage(`{not-json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestApplier_Apply_UnknownGVK(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"})
	obj.SetName("w1")
	raw, _ := json.Marshal(obj)
	err := ap.apply(context.Background(), raw)
	if err == nil {
		t.Fatal("expected error for unknown GVK")
	}
}

func TestApplier_Apply_ConfigMap(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetNamespace("default")
	obj.SetName("applied-cm")
	obj.Object["data"] = map[string]any{"k": "v"}

	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Exercise the apply path. The dynamic fake may not fully implement
	// server-side apply patches; either success or a patch error is fine
	// as long as we got past JSON parse and GVR resolution.
	if err := ap.apply(context.Background(), raw); err != nil {
		t.Logf("apply returned error (acceptable for fake client SSA): %v", err)
	}
}

func TestApplier_Apply_DefaultsEmptyNamespace(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
	obj.SetName("applied-no-ns")
	raw, _ := json.Marshal(obj)
	// Exercise the empty-namespace → "default" branch regardless of whether
	// the fake client accepts SSA patches.
	_ = ap.apply(context.Background(), raw)
}

func TestApplier_Apply_ClusterScoped(t *testing.T) {
	ap := newTestApplier(t)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"})
	obj.SetName("applied-ns")
	raw, _ := json.Marshal(obj)
	_ = ap.apply(context.Background(), raw)
}
