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

	client := dynamicfake.NewSimpleDynamicClient(scheme, objects...)

	// Create a proper APIGroupResources structure for the REST mapper
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
