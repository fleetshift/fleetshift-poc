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

func TestGVRKey(t *testing.T) {
	tests := []struct {
		name string
		gvr  schema.GroupVersionResource
		want string
	}{
		{"CoreGroup", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}, "core~v1~pods"},
		{"NamedGroup", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, "apps~v1~deployments"},
		{"DottedGroup", schema.GroupVersionResource{Group: "route.openshift.io", Version: "v1", Resource: "routes"}, "route.openshift.io~v1~routes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := kubernetes.GVRKey(tc.gvr); got != tc.want {
				t.Errorf("GVRKey(%+v) = %q, want %q", tc.gvr, got, tc.want)
			}
		})
	}
}

func TestObjectResourceName(t *testing.T) {
	t.Run("BasicShape", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			UID:                 "0d12-uid",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/apiResources/apps~v1~deployments/objects/0d12-uid")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
	})

	t.Run("CoreGroupUsesCoreKey", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			UID:                 "8a91-uid",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		want := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/8a91-uid")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
	})

	t.Run("SlashBearingUIDIsEncoded", func(t *testing.T) {
		name, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			UID:                 "uid/with/slash",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		// If the UID's "/" were not encoded, this would parse as extra
		// path segments and silently shift every segment after it.
		want := domain.ResourceName("clusters/prod/apiResources/core~v1~pods/objects/uid%2Fwith%2Fslash")
		if name != want {
			t.Fatalf("ObjectResourceName = %q, want %q", name, want)
		}
		if name.ID() != "uid%2Fwith%2Fslash" {
			t.Fatalf("ID() = %q, want %q", name.ID(), "uid%2Fwith%2Fslash")
		}
	})

	t.Run("RejectsEmptyUID", func(t *testing.T) {
		_, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectResourceName error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("RejectsEmptyClusterResourceName", func(t *testing.T) {
		_, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			UID:                 "uid-1",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectResourceName error = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestObjectCollectionName(t *testing.T) {
	t.Run("BasicShape", func(t *testing.T) {
		got, err := kubernetes.ObjectCollectionName("clusters/prod", schema.GroupVersionResource{
			Group: "apps", Version: "v1", Resource: "deployments",
		})
		if err != nil {
			t.Fatalf("ObjectCollectionName: %v", err)
		}
		want := domain.CollectionName("clusters/prod/apiResources/apps~v1~deployments/objects")
		if got != want {
			t.Fatalf("ObjectCollectionName = %q, want %q", got, want)
		}
	})

	t.Run("MatchesObjectResourceNameCollection", func(t *testing.T) {
		gvr := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
		objName, err := kubernetes.ObjectResourceName(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod", GVR: gvr, UID: "uid-1",
		})
		if err != nil {
			t.Fatalf("ObjectResourceName: %v", err)
		}
		collection, err := kubernetes.ObjectCollectionName("clusters/prod", gvr)
		if err != nil {
			t.Fatalf("ObjectCollectionName: %v", err)
		}
		if objName.Collection() != collection {
			t.Fatalf("ObjectCollectionName = %q, ObjectResourceName.Collection = %q",
				collection, objName.Collection())
		}
	})

	t.Run("RejectsNonFlatClusterResourceName", func(t *testing.T) {
		_, err := kubernetes.ObjectCollectionName("clusters/prod/us-east-1", schema.GroupVersionResource{
			Version: "v1", Resource: "pods",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectCollectionName error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("RejectsEmptyClusterResourceName", func(t *testing.T) {
		_, err := kubernetes.ObjectCollectionName("", schema.GroupVersionResource{
			Version: "v1", Resource: "pods",
		})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("ObjectCollectionName error = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestResourceNameCollectionIDs(t *testing.T) {
	if kubernetes.TargetCollectionID != "clusters" {
		t.Errorf("TargetCollectionID = %q, want %q", kubernetes.TargetCollectionID, "clusters")
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
	t.Run("Namespaced", func(t *testing.T) {
		got := kubernetes.ObjectLabels(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			Kind:      "Deployment",
			Namespace: "default",
			Name:      "nginx",
			UID:       "0d12-uid",
		})
		want := map[string]string{
			"k8s.gvr":       "apps~v1~deployments",
			"k8s.group":     "apps",
			"k8s.version":   "v1",
			"k8s.resource":  "deployments",
			"k8s.kind":      "Deployment",
			"k8s.scope":     "namespaced",
			"k8s.namespace": "default",
			"k8s.name":      "nginx",
			"k8s.uid":       "0d12-uid",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ObjectLabels = %#v, want %#v", got, want)
		}
	})

	t.Run("ClusterScopedOmitsNamespaceAndUsesCoreGroup", func(t *testing.T) {
		got := kubernetes.ObjectLabels(kubernetes.KubernetesObjectIdentity{
			ClusterResourceName: "clusters/prod",
			GVR:                 schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
			Kind:     "Node",
			Name:     "node-1",
			UID:      "node-uid",
		})
		if got["k8s.scope"] != "cluster" {
			t.Errorf("k8s.scope = %q, want %q", got["k8s.scope"], "cluster")
		}
		if got["k8s.gvr"] != "core~v1~nodes" {
			t.Errorf("k8s.gvr = %q, want %q", got["k8s.gvr"], "core~v1~nodes")
		}
		if got["k8s.group"] != "" {
			t.Errorf("k8s.group = %q, want empty (raw core group, not the \"core\" path key)", got["k8s.group"])
		}
		if _, ok := got["k8s.namespace"]; ok {
			t.Errorf("k8s.namespace = %q, want key omitted for a cluster-scoped object", got["k8s.namespace"])
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
		Kind:      "Deployment",
		Namespace: "default",
		Name:      "nginx",
		UID:       "0d12-uid",
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
		labels, _ := meta["labels"].(map[string]any)
		if labels["app"] != "nginx" {
			t.Fatalf("metadata.labels = %#v, want app=nginx", labels)
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
		// A channel value can never reach here through the real
		// extraction pipeline, but this pins down the fallback the
		// doc comment promises: one malformed hook must not panic or
		// silently corrupt the observation.
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
			Kind:     "Node",
			Name:     "node-1",
			UID:      "node-uid",
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
			{Type: "Health", Status: "Healthy"}, // some CRDs report free-form statuses instead of True/False/Unknown
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
