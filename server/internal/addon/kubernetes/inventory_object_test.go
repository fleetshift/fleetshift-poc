package kubernetes_test

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
	"k8s.io/utils/ptr"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestParseObjectScope(t *testing.T) {
	t.Run("namespaced", func(t *testing.T) {
		got, err := kubernetes.ParseObjectScope("namespaced")
		if err != nil {
			t.Fatalf("ParseObjectScope: %v", err)
		}
		if got != kubernetes.ObjectScopeNamespaced {
			t.Fatalf("got %q, want namespaced", got)
		}
	})
	t.Run("cluster", func(t *testing.T) {
		got, err := kubernetes.ParseObjectScope("cluster")
		if err != nil {
			t.Fatalf("ParseObjectScope: %v", err)
		}
		if got != kubernetes.ObjectScopeCluster {
			t.Fatalf("got %q, want cluster", got)
		}
	})
	t.Run("empty", func(t *testing.T) {
		_, err := kubernetes.ParseObjectScope("")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		_, err := kubernetes.ParseObjectScope("namespace")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
}

func mustTestScopeNamespace(t *testing.T, scope kubernetes.ObjectScope, namespace string) kubernetes.ScopeNamespace {
	t.Helper()
	sn, err := kubernetes.NewScopeNamespace(scope, namespace)
	if err != nil {
		t.Fatalf("NewScopeNamespace: %v", err)
	}
	return sn
}

func TestNewScopeNamespace(t *testing.T) {
	t.Run("namespaced", func(t *testing.T) {
		if _, err := kubernetes.NewScopeNamespace(kubernetes.ObjectScopeNamespaced, "default"); err != nil {
			t.Fatalf("NewScopeNamespace: %v", err)
		}
	})
	t.Run("cluster", func(t *testing.T) {
		if _, err := kubernetes.NewScopeNamespace(kubernetes.ObjectScopeCluster, ""); err != nil {
			t.Fatalf("NewScopeNamespace: %v", err)
		}
	})
	t.Run("rejects empty scope", func(t *testing.T) {
		_, err := kubernetes.NewScopeNamespace("", "default")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	t.Run("rejects namespaced without namespace", func(t *testing.T) {
		_, err := kubernetes.NewScopeNamespace(kubernetes.ObjectScopeNamespaced, "")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	t.Run("rejects cluster with namespace", func(t *testing.T) {
		_, err := kubernetes.NewScopeNamespace(kubernetes.ObjectScopeCluster, "default")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestParseClusterResourceName(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		got, err := kubernetes.ParseClusterResourceName("clusters/c1")
		if err != nil {
			t.Fatalf("ParseClusterResourceName: %v", err)
		}
		if got != "clusters/c1" {
			t.Fatalf("got %q, want clusters/c1", got)
		}
	})
	t.Run("empty", func(t *testing.T) {
		_, err := kubernetes.ParseClusterResourceName("")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	t.Run("wrong collection", func(t *testing.T) {
		_, err := kubernetes.ParseClusterResourceName("nodes/n1")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	t.Run("nested rejected", func(t *testing.T) {
		_, err := kubernetes.ParseClusterResourceName("orgs/o1/clusters/c1")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		_, err := kubernetes.ParseClusterResourceName("clusters/")
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestGroupResourceKey(t *testing.T) {
	tests := []struct {
		name    string
		gr      schema.GroupResource
		want    string
		wantErr bool
	}{
		{"CoreGroup", schema.GroupResource{Resource: "pods"}, "pods", false},
		{"NamedGroup", schema.GroupResource{Group: "apps", Resource: "deployments"}, "deployments.apps", false},
		{"DottedGroup", schema.GroupResource{Group: "route.openshift.io", Resource: "routes"}, "routes.route.openshift.io", false},
		{"EmptyResource", schema.GroupResource{Group: "apps"}, "", true},
		{"Subresource", schema.GroupResource{Resource: "pods/status"}, "", true},
		{"DotInResource", schema.GroupResource{Resource: "foo.bar"}, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := kubernetes.GroupResourceKey(tc.gr)
			if tc.wantErr {
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("error = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("GroupResourceKey: %v", err)
			}
			if got != tc.want {
				t.Errorf("GroupResourceKey(%+v) = %q, want %q", tc.gr, got, tc.want)
			}
			parsed, err := kubernetes.ParseGroupResourceKey(got)
			if err != nil {
				t.Fatalf("ParseGroupResourceKey: %v", err)
			}
			if parsed != tc.gr {
				t.Fatalf("ParseGroupResourceKey = %+v, want %+v", parsed, tc.gr)
			}
		})
	}
}

func TestParseGroupResourceKey_Rejects(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"Empty", ""},
		{"LeadingDot", ".apps"},
		{"TrailingDot", "deployments.apps."},
		{"TrailingDotCore", "pods."},
		{"Subresource", "pods/status"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := kubernetes.ParseGroupResourceKey(tc.key)
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Fatalf("ParseGroupResourceKey(%q) error = %v, want ErrInvalidArgument", tc.key, err)
			}
		})
	}
}

func TestObjectResourceName(t *testing.T) {
	t.Run("NamespacedShape", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"),
			UID:                 "0d12-uid",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/namespaces/default/apiResources/deployments.apps/objects/0d12-uid")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
	})

	t.Run("ClusterScopedShape", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeCluster, ""),
			UID:                 "node-uid",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/apiResources/nodes/objects/node-uid")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
	})

	t.Run("CoreNamespacedUsesResourceOnlyKey", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "kube-system"),
			UID:                 "8a91-uid",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/namespaces/kube-system/apiResources/pods/objects/8a91-uid")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
	})

	t.Run("SlashBearingUIDIsEncoded", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"),
			UID:                 "uid/with/slash",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/namespaces/default/apiResources/pods/objects/uid%2Fwith%2Fslash")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
		if name.ID() != "uid%2Fwith%2Fslash" {
			t.Fatalf("ID() = %q, want %q", name.ID(), "uid%2Fwith%2Fslash")
		}
	})

	t.Run("SlashBearingNamespaceIsEncoded", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "team/a"),
			UID:                 "uid-1",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/namespaces/team%2Fa/apiResources/pods/objects/uid-1")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
	})

	t.Run("VersionlessKeyIgnoresAPIVersion", func(t *testing.T) {
		v1 := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
		v1beta1 := schema.GroupVersionResource{Group: "apps", Version: "v1beta1", Resource: "deployments"}
		sn := mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default")
		nameV1, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod", GVR: v1, ScopeNamespace: sn, UID: "uid-1",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName v1: %v", err)
		}
		nameBeta, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod", GVR: v1beta1, ScopeNamespace: sn, UID: "uid-1",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName v1beta1: %v", err)
		}
		if nameV1 != nameBeta {
			t.Fatalf("version change renamed object: v1=%q v1beta1=%q", nameV1, nameBeta)
		}
		want := domain.ResourceName("clusters/prod/namespaces/default/apiResources/deployments.apps/objects/uid-1")
		if nameV1 != want {
			t.Fatalf("ObjectResourceName = %q, want %q", nameV1, want)
		}
	})

	t.Run("RejectsEmptyUID", func(t *testing.T) {
		_, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"),
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectResourceName error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("RejectsEmptyClusterResourceName", func(t *testing.T) {
		_, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"),
			UID:                 "uid-1",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectResourceName error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("RejectsZeroScopeNamespace", func(t *testing.T) {
		_, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			UID:                 "uid-1",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectResourceName error = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestObjectCollectionName(t *testing.T) {
	t.Run("NamespacedShape", func(t *testing.T) {
		got, err := kubernetes.ObjectCollectionName("clusters/prod", mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"), schema.GroupVersionResource{
			Group: "apps", Version: "v1", Resource: "deployments",
		})
		if err != nil {
			t.Fatalf("ObjectCollectionName: %v", err)
		}
		want := domain.CollectionName("clusters/prod/namespaces/default/apiResources/deployments.apps/objects")
		if got != want {
			t.Fatalf("ObjectCollectionName = %q, want %q", got, want)
		}
	})

	t.Run("ClusterScopedShape", func(t *testing.T) {
		got, err := kubernetes.ObjectCollectionName("clusters/prod", mustTestScopeNamespace(t, kubernetes.ObjectScopeCluster, ""), schema.GroupVersionResource{
			Group: "", Version: "v1", Resource: "nodes",
		})
		if err != nil {
			t.Fatalf("ObjectCollectionName: %v", err)
		}
		want := domain.CollectionName("clusters/prod/apiResources/nodes/objects")
		if got != want {
			t.Fatalf("ObjectCollectionName = %q, want %q", got, want)
		}
	})

	t.Run("RejectsZeroScopeNamespace", func(t *testing.T) {
		_, err := kubernetes.ObjectCollectionName("clusters/prod", kubernetes.ScopeNamespace{}, schema.GroupVersionResource{
			Version: "v1", Resource: "pods",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectCollectionName error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("MatchesObjectResourceNameCollection", func(t *testing.T) {
		gvr := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
		objName, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 gvr,
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"),
			UID:                 "uid-1",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		collection, err := kubernetes.ObjectCollectionName("clusters/prod", mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"), gvr)
		if err != nil {
			t.Fatalf("ObjectCollectionName: %v", err)
		}
		if objName.Collection() != collection {
			t.Fatalf("ObjectCollectionName = %q, ObjectResourceName.Collection = %q",
				collection, objName.Collection())
		}
	})

	t.Run("RejectsNonFlatClusterResourceName", func(t *testing.T) {
		_, err := kubernetes.ObjectCollectionName("clusters/prod/us-east-1", mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"), schema.GroupVersionResource{
			Version: "v1", Resource: "pods",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectCollectionName error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("RejectsEmptyClusterResourceName", func(t *testing.T) {
		_, err := kubernetes.ObjectCollectionName("", mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"), schema.GroupVersionResource{
			Version: "v1", Resource: "pods",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectCollectionName error = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestResourceNameCollectionIDs(t *testing.T) {
	if kubernetes.ClusterCollectionID != "clusters" {
		t.Errorf("ClusterCollectionID = %q, want %q", kubernetes.ClusterCollectionID, "clusters")
	}
	if kubernetes.NamespaceCollectionID != "namespaces" {
		t.Errorf("NamespaceCollectionID = %q, want %q", kubernetes.NamespaceCollectionID, "namespaces")
	}
	if kubernetes.APIResourceCollectionID != "apiResources" {
		t.Errorf("APIResourceCollectionID = %q, want %q", kubernetes.APIResourceCollectionID, "apiResources")
	}
	if kubernetes.ObjectCollectionID != "objects" {
		t.Errorf("ObjectCollectionID = %q, want %q", kubernetes.ObjectCollectionID, "objects")
	}
	if kubernetes.InventorySchema().CollectionID != string(kubernetes.ObjectCollectionID) {
		t.Errorf("InventorySchema().CollectionID = %q, want ObjectCollectionID %q",
			kubernetes.InventorySchema().CollectionID, kubernetes.ObjectCollectionID)
	}
}

func TestObjectLabels(t *testing.T) {
	t.Run("CopiesMetadataLabels", func(t *testing.T) {
		obj := &unstructured.Unstructured{}
		obj.SetLabels(map[string]string{
			"app":                    "nginx",
			"kubernetes.io/hostname": "node-1",
			"kube-aggregator.kubernetes.io/automanaged": "onstart",
		})
		got := kubernetes.ObjectLabels(obj)
		want := map[string]string{
			"app":                    "nginx",
			"kubernetes.io/hostname": "node-1",
			"kube-aggregator.kubernetes.io/automanaged": "onstart",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ObjectLabels = %#v, want %#v", got, want)
		}
	})

	t.Run("EmptyWhenNoLabels", func(t *testing.T) {
		got := kubernetes.ObjectLabels(&unstructured.Unstructured{})
		if got == nil {
			t.Fatal("ObjectLabels = nil, want empty non-nil map")
		}
		if len(got) != 0 {
			t.Fatalf("ObjectLabels = %#v, want empty map", got)
		}
	})

	t.Run("DefensiveCopy", func(t *testing.T) {
		obj := &unstructured.Unstructured{}
		obj.SetLabels(map[string]string{"app": "nginx"})
		got := kubernetes.ObjectLabels(obj)
		got["app"] = "mutated"
		if obj.GetLabels()["app"] != "nginx" {
			t.Fatal("ObjectLabels must not share the object's label map")
		}
	})
}

func TestObjectObservation(t *testing.T) {
	newObj := func() *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("apps/v1")
		obj.SetKind("Deployment")
		obj.SetNamespace("default")
		obj.SetName("nginx")
		obj.SetUID(types.UID("0d12-uid"))
		obj.SetResourceVersion("12345")
		obj.SetGeneration(4)
		obj.SetCreationTimestamp(metav1.NewTime(time.Date(2026, 7, 7, 10, 0, 0, 0, time.UTC)))
		obj.SetLabels(map[string]string{"app": "nginx"})
		obj.SetAnnotations(map[string]string{"note": "x"})
		obj.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       "nginx-rs",
			UID:        types.UID("rs-uid"),
			Controller: ptr.To(true),
		}})
		return obj
	}
	id := kubernetes.KubernetesObjectIdentity{
		ClusterResourceName: "clusters/prod",
		GVR:                 schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeNamespaced, "default"),
		Kind:                "Deployment",
		Name:                "nginx",
		UID:                 "0d12-uid",
	}

	t.Run("FieldsMatchTheWatchedObject", func(t *testing.T) {
		raw := kubernetes.ObjectObservation(id, newObj(), map[string]any{"replicas": float64(3)})

		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal observation: %v", err)
		}

		gvr, _ := got["gvr"].(map[string]any)
		if gvr["group"] != "apps" || gvr["version"] != "v1" || gvr["resource"] != "deployments" || gvr["scope"] != "namespaced" {
			t.Fatalf("gvr = %#v, want group=apps version=v1 resource=deployments scope=namespaced", gvr)
		}
		if got["apiVersion"] != "apps/v1" || got["kind"] != "Deployment" {
			t.Fatalf("apiVersion/kind = %v/%v, want apps/v1 / Deployment", got["apiVersion"], got["kind"])
		}

		meta, _ := got["metadata"].(map[string]any)
		if meta["uid"] != "0d12-uid" || meta["namespace"] != "default" || meta["name"] != "nginx" {
			t.Fatalf("metadata identity = %#v, want uid=0d12-uid namespace=default name=nginx", meta)
		}
		if meta["resourceVersion"] != "12345" || meta["generation"] != float64(4) {
			t.Fatalf("metadata versioning = %#v, want resourceVersion=12345 generation=4", meta)
		}
		if meta["creationTimestamp"] != "2026-07-07T10:00:00Z" {
			t.Fatalf("creationTimestamp = %v, want 2026-07-07T10:00:00Z", meta["creationTimestamp"])
		}
		if meta["deletionTimestamp"] != nil {
			t.Fatalf("deletionTimestamp = %v, want null (object is not being deleted)", meta["deletionTimestamp"])
		}
		if _, ok := meta["labels"]; ok {
			t.Fatalf("metadata.labels = %#v, want omitted (labels live on localLabels)", meta["labels"])
		}
		annotations, _ := meta["annotations"].(map[string]any)
		if annotations["note"] != "x" {
			t.Fatalf("metadata.annotations = %#v, want note=x", annotations)
		}
		ownerRefs, _ := meta["ownerReferences"].([]any)
		if len(ownerRefs) != 1 {
			t.Fatalf("metadata.ownerReferences = %#v, want 1 entry", meta["ownerReferences"])
		}
		owner, _ := ownerRefs[0].(map[string]any)
		if owner["apiVersion"] != "apps/v1" || owner["kind"] != "ReplicaSet" || owner["name"] != "nginx-rs" || owner["uid"] != "rs-uid" || owner["controller"] != true {
			t.Fatalf("ownerReferences[0] = %#v, want apiVersion=apps/v1 kind=ReplicaSet name=nginx-rs uid=rs-uid controller=true", owner)
		}

		extracted, _ := got["extracted"].(map[string]any)
		if extracted["replicas"] != float64(3) {
			t.Fatalf("extracted = %#v, want replicas=3", extracted)
		}
	})

	t.Run("NilExtractedBecomesEmptyObject", func(t *testing.T) {
		raw := kubernetes.ObjectObservation(id, newObj(), nil)

		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal observation: %v", err)
		}
		extracted, ok := got["extracted"].(map[string]any)
		if !ok || len(extracted) != 0 {
			t.Fatalf("extracted = %#v, want an empty object", got["extracted"])
		}
	})

	t.Run("UnmarshalableExtractedDegradesToEmptyObject", func(t *testing.T) {
		raw := kubernetes.ObjectObservation(id, newObj(), map[string]any{"bad": make(chan int)})
		if string(raw) != "{}" {
			t.Fatalf("ObjectObservation with unmarshalable extracted = %s, want {}", raw)
		}
	})

	t.Run("DeletionTimestampIsIncludedWhenSet", func(t *testing.T) {
		obj := newObj()
		obj.SetDeletionTimestamp(ptr.To(metav1.NewTime(time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC))))

		raw := kubernetes.ObjectObservation(id, obj, nil)

		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal observation: %v", err)
		}
		meta, _ := got["metadata"].(map[string]any)
		if meta["deletionTimestamp"] != "2026-07-08T00:00:00Z" {
			t.Fatalf("deletionTimestamp = %v, want 2026-07-08T00:00:00Z", meta["deletionTimestamp"])
		}
	})

	t.Run("OwnerReferencesAreNullWhenAbsent", func(t *testing.T) {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("apps/v1")
		obj.SetKind("Deployment")
		obj.SetName("nginx")
		obj.SetUID(types.UID("0d12-uid"))

		raw := kubernetes.ObjectObservation(id, obj, nil)

		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal observation: %v", err)
		}
		meta, _ := got["metadata"].(map[string]any)
		if meta["ownerReferences"] != nil {
			t.Fatalf("ownerReferences = %v, want null (object has no owner)", meta["ownerReferences"])
		}
	})

	t.Run("ClusterScopedObjectReportsClusterScope", func(t *testing.T) {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("v1")
		obj.SetKind("Node")
		obj.SetName("node-1")
		obj.SetUID(types.UID("node-uid"))

		raw := kubernetes.ObjectObservation(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
			ScopeNamespace:      mustTestScopeNamespace(t, kubernetes.ObjectScopeCluster, ""),
			Kind:                "Node",
			Name:                "node-1",
			UID:                 "node-uid",
		}, obj, nil)

		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal observation: %v", err)
		}
		gvr, _ := got["gvr"].(map[string]any)
		if gvr["group"] != "" || gvr["scope"] != "cluster" {
			t.Fatalf("gvr = %#v, want group=\"\" scope=cluster", gvr)
		}
	})
}

func TestObjectConditions(t *testing.T) {
	observedAt := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	t.Run("DropsEmptyType", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "", Status: "True"},
			{Type: "Ready", Status: "True"},
		}, observedAt)
		if len(got) != 1 || got[0].Type() != "Ready" {
			t.Fatalf("ObjectConditions = %#v, want only Ready", got)
		}
	})

	t.Run("DropsNonStandardStatus", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "Health", Status: "Healthy"},
			{Type: "Ready", Status: "True"},
		}, observedAt)
		if len(got) != 1 || got[0].Type() != "Ready" {
			t.Fatalf("ObjectConditions = %#v, want only Ready", got)
		}
	})

	t.Run("AcceptsTrueFalseUnknown", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "A", Status: "True"},
			{Type: "B", Status: "False"},
			{Type: "C", Status: "Unknown"},
		}, observedAt)
		if len(got) != 3 {
			t.Fatalf("ObjectConditions = %#v, want 3 conditions", got)
		}
	})

	t.Run("MissingTransitionTimeFallsBackToObservedAt", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "Ready", Status: "True"},
		}, observedAt)
		if len(got) != 1 || !got[0].LastTransitionTime().Equal(observedAt) {
			t.Fatalf("LastTransitionTime = %v, want %v", got[0].LastTransitionTime(), observedAt)
		}
	})

	t.Run("MalformedTransitionTimeFallsBackToObservedAt", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "Ready", Status: "True", LastTransitionTime: "not-a-timestamp"},
		}, observedAt)
		if len(got) != 1 || !got[0].LastTransitionTime().Equal(observedAt) {
			t.Fatalf("LastTransitionTime = %v, want %v", got[0].LastTransitionTime(), observedAt)
		}
	})

	t.Run("ValidTransitionTimeIsPreserved", func(t *testing.T) {
		want := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "Ready", Status: "True", LastTransitionTime: want.Format(time.RFC3339)},
		}, observedAt)
		if len(got) != 1 || !got[0].LastTransitionTime().Equal(want) {
			t.Fatalf("LastTransitionTime = %v, want %v", got[0].LastTransitionTime(), want)
		}
	})

	t.Run("DuplicateTypeLastEntryWins", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "Ready", Status: "False", Reason: "Initializing"},
			{Type: "Ready", Status: "True", Reason: "AllGood"},
		}, observedAt)
		if len(got) != 1 {
			t.Fatalf("ObjectConditions = %#v, want exactly one Ready condition", got)
		}
		if got[0].Status() != domain.ConditionTrue || got[0].Reason() != "AllGood" {
			t.Fatalf("condition = %+v, want the last entry (True/AllGood) to win", got[0])
		}
	})

	t.Run("PreservesFirstSeenOrderForDistinctTypes", func(t *testing.T) {
		got := kubernetes.ObjectConditions([]kubernetes.RawCondition{
			{Type: "B", Status: "True"},
			{Type: "A", Status: "True"},
		}, observedAt)
		if len(got) != 2 || got[0].Type() != "B" || got[1].Type() != "A" {
			t.Fatalf("ObjectConditions = %#v, want [B, A] in first-seen order", got)
		}
	})

	t.Run("NilInputReturnsEmptySlice", func(t *testing.T) {
		got := kubernetes.ObjectConditions(nil, observedAt)
		if len(got) != 0 {
			t.Fatalf("ObjectConditions(nil, ...) = %#v, want an empty slice", got)
		}
	})
}
