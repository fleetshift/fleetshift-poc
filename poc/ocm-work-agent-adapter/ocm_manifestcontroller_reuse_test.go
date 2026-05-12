package adapter

import (
	"context"
	"encoding/json"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakekube "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"

	fakeworkclient "open-cluster-management.io/api/client/work/clientset/versioned/fake"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	workapiv1 "open-cluster-management.io/api/work/v1"
	"open-cluster-management.io/ocm/pkg/work/spoke/controllers/manifestcontroller"
	"open-cluster-management.io/ocm/pkg/work/spoke/spoketesting"
)

var newObjectGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "newobjects"}

// allowAllValidator keeps the reuse test focused on controller wiring rather
// than OCM's executor authorization path.
type allowAllValidator struct{}

func (allowAllValidator) Validate(
	context.Context,
	*workapiv1.ManifestWorkExecutor,
	schema.GroupVersionResource,
	string,
	string,
	bool,
	*unstructured.Unstructured,
) error {
	return nil
}

// noopObjectReader is enough for this focused sync test because the real OCM
// controller only needs informer registration when status feedback and cleanup
// paths are exercised.
type noopObjectReader struct{}

func (noopObjectReader) Get(
	context.Context,
	workapiv1.ManifestResourceMeta,
) (*unstructured.Unstructured, metav1.Condition, error) {
	return nil, metav1.Condition{}, nil
}

func (noopObjectReader) RegisterInformer(
	context.Context,
	string,
	workapiv1.ManifestResourceMeta,
	workqueue.TypedRateLimitingInterface[string],
) error {
	return nil
}

func (noopObjectReader) UnRegisterInformer(string, workapiv1.ManifestResourceMeta) error {
	return nil
}

func TestProjectedManifestWorkCanDriveRealOCMManifestController(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	work, err := ManifestWorkForDelivery(DeliveryEnvelope{
		DeliveryID: "delivery-a",
		TargetID:   "cluster1",
		UpdateMode: UpdateModeUpdate,
		Manifests: []Manifest{
			{Raw: rawNewObject("team-a", "settings")},
		},
	})
	if err != nil {
		t.Fatalf("project delivery into manifestwork: %v", err)
	}

	hubWorkClient := fakeworkclient.NewSimpleClientset(work.DeepCopy())
	hubInformers := workinformers.NewSharedInformerFactoryWithOptions(
		hubWorkClient,
		0,
		workinformers.WithNamespace("cluster1"),
	)
	manifestWorkInformer := hubInformers.Work().V1().ManifestWorks()
	appliedManifestWorkInformer := hubInformers.Work().V1().AppliedManifestWorks()

	if err := manifestWorkInformer.Informer().GetStore().Add(work.DeepCopy()); err != nil {
		t.Fatalf("seed manifestwork informer store: %v", err)
	}

	spokeDynamicClient := fakedynamic.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			newObjectGVR: "NewObjectList",
		},
	)
	spokeKubeClient := fakekube.NewSimpleClientset()

	controller := manifestcontroller.NewManifestWorkController(
		spokeDynamicClient,
		spokeKubeClient,
		nil,
		hubWorkClient.WorkV1().ManifestWorks("cluster1"),
		manifestWorkInformer,
		manifestWorkInformer.Lister().ManifestWorks("cluster1"),
		hubWorkClient.WorkV1().AppliedManifestWorks(),
		appliedManifestWorkInformer,
		noopObjectReader{},
		"hub123",
		"agent-1",
		spoketesting.NewFakeRestMapper(),
		allowAllValidator{},
	)

	if err := controller.Sync(ctx, controller.SyncContext(), "delivery-a"); err != nil {
		t.Fatalf("sync real ocm manifest controller: %v", err)
	}

	gotWork, err := hubWorkClient.WorkV1().ManifestWorks("cluster1").Get(ctx, "delivery-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get manifestwork after sync: %v", err)
	}
	if !apimeta.IsStatusConditionTrue(gotWork.Status.Conditions, workapiv1.WorkApplied) {
		t.Fatalf("manifestwork %q was not marked applied: %#v", gotWork.Name, gotWork.Status.Conditions)
	}
	if len(gotWork.Status.ResourceStatus.Manifests) != 1 {
		t.Fatalf("manifest status entries = %d, want 1", len(gotWork.Status.ResourceStatus.Manifests))
	}

	applied, err := hubWorkClient.WorkV1().AppliedManifestWorks().Get(ctx, "hub123-delivery-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get appliedmanifestwork after sync: %v", err)
	}
	if applied.Spec.ManifestWorkName != "delivery-a" {
		t.Fatalf("appliedmanifestwork manifest link = %q, want delivery-a", applied.Spec.ManifestWorkName)
	}
	if applied.Spec.AgentID != "agent-1" {
		t.Fatalf("appliedmanifestwork agent id = %q, want agent-1", applied.Spec.AgentID)
	}
	if len(applied.Status.AppliedResources) != 1 {
		t.Fatalf("applied resources = %d, want 1", len(applied.Status.AppliedResources))
	}
	if applied.Status.AppliedResources[0].Resource != "newobjects" {
		t.Fatalf("applied resource kind = %q, want newobjects", applied.Status.AppliedResources[0].Resource)
	}

	appliedObject, err := spokeDynamicClient.Resource(newObjectGVR).Namespace("team-a").Get(ctx, "settings", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get applied spoke object: %v", err)
	}
	if appliedObject.GetName() != "settings" {
		t.Fatalf("applied object name = %q, want settings", appliedObject.GetName())
	}
}

func rawNewObject(namespace, name string) []byte {
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "NewObject",
		"metadata": map[string]any{
			"namespace": namespace,
			"name":      name,
		},
		"spec": map[string]any{
			"value": "demo",
		},
	})
	if err != nil {
		panic(err)
	}
	return raw
}
