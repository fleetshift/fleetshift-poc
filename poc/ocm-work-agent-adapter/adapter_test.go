package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workapiv1 "open-cluster-management.io/api/work/v1"
)

func TestHubProjectsDeliveryIntoVirtualManifestWorkView(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewHub()
	view := hub.View(TargetID("cluster-a"))
	view.Start(ctx)
	if !view.WaitForSync(ctx) {
		t.Fatal("manifestwork informer did not sync")
	}

	_, err := hub.Deliver(ctx, DeliveryEnvelope{
		DeliveryID:     DeliveryID("delivery-a"),
		TargetID:       TargetID("cluster-a"),
		UpdateMode:     UpdateModeServerSideApply,
		ForceOwnership: true,
		AttestationRef: "att-123",
		Manifests: []Manifest{
			{Raw: rawConfigMap("team-a", "settings"), Watch: true},
		},
	})
	if err != nil {
		t.Fatalf("deliver cluster-a manifestwork: %v", err)
	}

	_, err = hub.Deliver(ctx, DeliveryEnvelope{
		DeliveryID: DeliveryID("delivery-b"),
		TargetID:   TargetID("cluster-b"),
		Manifests: []Manifest{
			{Raw: rawNamespace("other")},
		},
	})
	if err != nil {
		t.Fatalf("deliver cluster-b manifestwork: %v", err)
	}

	eventually(t, "cluster-a work to appear in scoped informer", func() bool {
		_, err := view.Lister().Get("delivery-a")
		return err == nil
	})

	got, err := view.Lister().Get("delivery-a")
	if err != nil {
		t.Fatalf("get manifestwork from scoped informer: %v", err)
	}

	if got.Namespace != "cluster-a" {
		t.Fatalf("namespace = %q, want cluster-a", got.Namespace)
	}
	if got.Name != "delivery-a" {
		t.Fatalf("name = %q, want delivery-a", got.Name)
	}
	if got.Annotations[AnnotationAttestationRef] != "att-123" {
		t.Fatalf("attestation annotation = %q, want att-123", got.Annotations[AnnotationAttestationRef])
	}
	if len(got.Spec.ManifestConfigs) != 1 {
		t.Fatalf("manifest config count = %d, want 1", len(got.Spec.ManifestConfigs))
	}

	cfg := got.Spec.ManifestConfigs[0]
	if cfg.ResourceIdentifier.Resource != "configmaps" {
		t.Fatalf("resource = %q, want configmaps", cfg.ResourceIdentifier.Resource)
	}
	if cfg.UpdateStrategy == nil {
		t.Fatal("update strategy was nil")
	}
	if cfg.UpdateStrategy.Type != workapiv1.UpdateStrategyTypeServerSideApply {
		t.Fatalf("update strategy type = %q, want %q", cfg.UpdateStrategy.Type, workapiv1.UpdateStrategyTypeServerSideApply)
	}
	if cfg.UpdateStrategy.ServerSideApply == nil || !cfg.UpdateStrategy.ServerSideApply.Force {
		t.Fatal("server-side apply force flag was not preserved")
	}
	if cfg.FeedbackScrapeType != workapiv1.FeedbackWatchType {
		t.Fatalf("feedback scrape type = %q, want %q", cfg.FeedbackScrapeType, workapiv1.FeedbackWatchType)
	}

	_, err = view.Lister().Get("delivery-b")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("cluster-a scoped informer unexpectedly saw delivery-b: %v", err)
	}
}

func TestVirtualManifestWorkClientUpdatesInformerCache(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewHub()
	view := hub.View(TargetID("cluster-a"))
	view.Start(ctx)
	if !view.WaitForSync(ctx) {
		t.Fatal("manifestwork informer did not sync")
	}

	_, err := hub.Deliver(ctx, DeliveryEnvelope{
		DeliveryID: DeliveryID("delivery-a"),
		TargetID:   TargetID("cluster-a"),
		Manifests: []Manifest{
			{Raw: rawConfigMap("team-a", "settings")},
		},
	})
	if err != nil {
		t.Fatalf("deliver manifestwork: %v", err)
	}

	eventually(t, "virtual manifestwork to appear in scoped view", func() bool {
		_, err := view.Lister().Get("delivery-a")
		return err == nil
	})

	work, err := view.Client().Get(ctx, "delivery-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get manifestwork via virtual client: %v", err)
	}
	applied := metav1.Condition{
		Type:   workapiv1.WorkApplied,
		Status: metav1.ConditionTrue,
		Reason: "SyntheticUpdateStatus",
	}
	work.Status.Conditions = []metav1.Condition{applied}
	if _, err := view.Client().UpdateStatus(ctx, work, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status via virtual client: %v", err)
	}

	eventually(t, "status update to reach informer cache", func() bool {
		current, err := view.Lister().Get("delivery-a")
		if err != nil {
			return false
		}
		return hasCondition(current.Status.Conditions, workapiv1.WorkApplied)
	})
}

func TestSpokeReconcilerUsesMinimalLocalJournalProjection(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewHub()
	view := hub.View(TargetID("cluster-a"))
	spoke := NewSpokeReconciler(TargetID("cluster-a"), view, SpokeOptions{
		HubHash: "hub123",
		AgentID: "agent-1",
	})

	runErr := make(chan error, 1)
	go func() {
		runErr <- spoke.Run(ctx)
	}()

	if !view.WaitForSync(ctx) {
		t.Fatal("manifestwork informer did not sync")
	}

	_, err := hub.Deliver(ctx, DeliveryEnvelope{
		DeliveryID: DeliveryID("delivery-a"),
		TargetID:   TargetID("cluster-a"),
		Manifests: []Manifest{
			{Raw: rawConfigMap("team-a", "settings"), Watch: true},
			{Raw: rawDeployment("team-a", "backend")},
		},
	})
	if err != nil {
		t.Fatalf("deliver manifestwork: %v", err)
	}

	eventually(t, "appliedmanifestwork to be created", func() bool {
		_, err := spoke.AppliedManifestWork(ctx, DeliveryID("delivery-a"))
		return err == nil
	})

	entry, ok := spoke.JournalEntry(DeliveryID("delivery-a"))
	if !ok {
		t.Fatal("missing journal entry for delivery-a")
	}
	if entry.ManifestWorkName != "delivery-a" {
		t.Fatalf("journal manifestwork link = %q, want delivery-a", entry.ManifestWorkName)
	}
	if entry.AgentID != "agent-1" {
		t.Fatalf("journal agent id = %q, want agent-1", entry.AgentID)
	}
	if len(entry.AppliedResources) != 2 {
		t.Fatalf("journal applied resources = %d, want 2", len(entry.AppliedResources))
	}

	applied, err := spoke.AppliedManifestWork(ctx, DeliveryID("delivery-a"))
	if err != nil {
		t.Fatalf("get appliedmanifestwork: %v", err)
	}

	if applied.Name != "hub123-delivery-a" {
		t.Fatalf("appliedmanifestwork name = %q, want hub123-delivery-a", applied.Name)
	}
	if applied.Spec.ManifestWorkName != "delivery-a" {
		t.Fatalf("manifestwork link = %q, want delivery-a", applied.Spec.ManifestWorkName)
	}
	if applied.Spec.AgentID != "agent-1" {
		t.Fatalf("agent id = %q, want agent-1", applied.Spec.AgentID)
	}
	if len(applied.Status.AppliedResources) != 2 {
		t.Fatalf("applied resources = %d, want 2", len(applied.Status.AppliedResources))
	}

	feedback, ok := spoke.Feedback(DeliveryID("delivery-a"))
	if !ok {
		t.Fatal("missing feedback for delivery-a")
	}
	if !feedback.Applied {
		t.Fatal("feedback should report applied=true")
	}
	if !feedback.Available {
		t.Fatal("feedback should report available=true")
	}
	if len(feedback.AppliedResources) != 2 {
		t.Fatalf("feedback applied resources = %d, want 2", len(feedback.AppliedResources))
	}

	work, err := view.Client().Get(ctx, "delivery-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get updated manifestwork: %v", err)
	}

	if !hasCondition(work.Status.Conditions, workapiv1.WorkApplied) {
		t.Fatalf("manifestwork status missing %q condition", workapiv1.WorkApplied)
	}
	if !hasCondition(work.Status.Conditions, workapiv1.WorkAvailable) {
		t.Fatalf("manifestwork status missing %q condition", workapiv1.WorkAvailable)
	}
	if len(work.Status.ResourceStatus.Manifests) != 2 {
		t.Fatalf("manifest status entries = %d, want 2", len(work.Status.ResourceStatus.Manifests))
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil && err != context.Canceled {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("spoke reconcile loop did not exit after cancel")
	}
}

func eventually(t *testing.T, description string, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", description)
}

func hasCondition(conditions []metav1.Condition, conditionType string) bool {
	for _, cond := range conditions {
		if cond.Type == conditionType && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func rawConfigMap(namespace, name string) json.RawMessage {
	return rawManifest(fmt.Sprintf(`{
  "apiVersion": "v1",
  "kind": "ConfigMap",
  "metadata": {
    "namespace": %q,
    "name": %q
  },
  "data": {
    "key": "value"
  }
}`, namespace, name))
}

func rawNamespace(name string) json.RawMessage {
	return rawManifest(fmt.Sprintf(`{
  "apiVersion": "v1",
  "kind": "Namespace",
  "metadata": {
    "name": %q
  }
}`, name))
}

func rawDeployment(namespace, name string) json.RawMessage {
	return rawManifest(fmt.Sprintf(`{
  "apiVersion": "apps/v1",
  "kind": "Deployment",
  "metadata": {
    "namespace": %q,
    "name": %q
  },
  "spec": {
    "replicas": 1,
    "selector": {
      "matchLabels": {
        "app": %q
      }
    },
    "template": {
      "metadata": {
        "labels": {
          "app": %q
        }
      },
      "spec": {
        "containers": [
          {
            "name": "backend",
            "image": "nginx:1.25"
          }
        ]
      }
    }
  }
}`, namespace, name, name, name))
}

func rawManifest(s string) json.RawMessage {
	return json.RawMessage([]byte(s))
}
