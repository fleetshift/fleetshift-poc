package e2e_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	deliveryv1 "github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/apis/delivery/v1alpha1"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/contract"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/controllers"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/fsruntime"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/platform"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/provider"
)

func TestDeliveryReconcileViaMulticlusterProvider(t *testing.T) {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := logr.FromContextOrDiscard(context.Background())

	fake := platform.NewFake()
	target := contract.TargetInfo{
		ID:   "gcp-project-us-central1",
		Type: "gcphcp",
		Name: "demo-target",
		Properties: map[string]string{
			"gcp_project": "demo",
			"region":      "us-central1",
		},
	}

	prov, err := provider.New(provider.Options{
		Scheme:   deliveryv1.Scheme,
		Reporter: fake,
		Logger:   logger.WithName("provider"),
		Targets:  []contract.TargetInfo{target},
	})
	if err != nil {
		t.Fatal(err)
	}

	host, err := fsruntime.NewManager(fsruntime.Options{
		Scheme: deliveryv1.Scheme,
		Logger: logger.WithName("host"),
	})
	if err != nil {
		t.Fatal(err)
	}

	mcMgr, err := mcmanager.WithMultiCluster(host, prov)
	if err != nil {
		t.Fatal(err)
	}

	if err := (&controllers.DeliveryReconciler{}).SetupWithManager(mcMgr); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- mcMgr.Start(ctx)
	}()

	// Give the provider time to engage targets and the controller to start.
	waitReady(t, ctx, mcMgr, target.ID)

	manifest, _ := json.Marshal(map[string]any{
		"name":           "demo-cluster",
		"releaseVersion": "4.19.0",
	})

	deliveryID := contract.DeliveryID("del-1")
	if err := fake.Dispatch(ctx, prov, target, deliveryID, []contract.Manifest{{
		ManifestType: "api.gcphcp.cluster",
		Raw:          manifest,
	}}, contract.DeliveryAuth{Token: "caller-jwt"}, 1, contract.DeliveryOperationDeliver); err != nil {
		t.Fatal(err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	result, ok := fake.WaitForResult(waitCtx, deliveryID)
	if !ok {
		t.Fatal("timed out waiting for delivery result")
	}
	if result.Result.State != contract.DeliveryStateDelivered {
		t.Fatalf("state = %q, want delivered; msg=%q", result.Result.State, result.Result.Message)
	}

	events, _ := fake.Snapshot()
	if len(events) == 0 {
		t.Fatal("expected at least one progress event")
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}
}

func TestStaleGenerationDiscarded(t *testing.T) {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := logr.Discard()

	fake := platform.NewFake()
	target := contract.TargetInfo{ID: "t1", Type: "kind", Name: "t1"}

	prov, err := provider.New(provider.Options{
		Scheme:   deliveryv1.Scheme,
		Reporter: fake,
		Logger:   logger,
		Targets:  []contract.TargetInfo{target},
	})
	if err != nil {
		t.Fatal(err)
	}

	host, err := fsruntime.NewManager(fsruntime.Options{Scheme: deliveryv1.Scheme, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	mcMgr, err := mcmanager.WithMultiCluster(host, prov)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controllers.DeliveryReconciler{}).SetupWithManagerNamed(mcMgr, "delivery-stale"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mcMgr.Start(ctx) }()
	waitReady(t, ctx, mcMgr, target.ID)

	deliveryID := contract.DeliveryID("del-stale")
	manifest := []contract.Manifest{{ManifestType: "api.kind.cluster", Raw: []byte(`{"name":"c"}`)}}
	auth := contract.DeliveryAuth{Token: "tok"}

	if err := fake.Dispatch(ctx, prov, target, deliveryID, manifest, auth, 2, contract.DeliveryOperationDeliver); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if _, ok := fake.WaitForResult(waitCtx, deliveryID); !ok {
		t.Fatal("timed out waiting for gen=2")
	}

	// Stale generation must be fenced at the provider and never re-applied.
	if err := prov.Deliver(ctx, target, deliveryID, manifest, auth, nil, 1); err != nil {
		t.Fatal(err)
	}

	// Give the controller a moment; no new result for gen=1 should appear.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, results := fake.Snapshot()
		for _, r := range results {
			if r.DeliveryID == deliveryID && r.Generation == 1 {
				t.Fatal("stale generation 1 was reported")
			}
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}
}

func TestAuthFailureReported(t *testing.T) {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := logr.Discard()
	fake := platform.NewFake()
	target := contract.TargetInfo{ID: "t-auth", Type: "gcphcp", Name: "t-auth"}

	prov, err := provider.New(provider.Options{
		Scheme: deliveryv1.Scheme, Reporter: fake, Logger: logger, Targets: []contract.TargetInfo{target},
	})
	if err != nil {
		t.Fatal(err)
	}
	host, err := fsruntime.NewManager(fsruntime.Options{Scheme: deliveryv1.Scheme, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	mcMgr, err := mcmanager.WithMultiCluster(host, prov)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controllers.DeliveryReconciler{}).SetupWithManagerNamed(mcMgr, "delivery-auth"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mcMgr.Start(ctx) }()
	waitReady(t, ctx, mcMgr, target.ID)

	deliveryID := contract.DeliveryID("del-auth")
	if err := fake.Dispatch(ctx, prov, target, deliveryID, []contract.Manifest{{
		ManifestType: "api.gcphcp.cluster", Raw: []byte(`{}`),
	}}, contract.DeliveryAuth{}, 1, contract.DeliveryOperationDeliver); err != nil {
		t.Fatal(err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	result, ok := fake.WaitForResult(waitCtx, deliveryID)
	if !ok {
		t.Fatal("timed out")
	}
	if result.Result.State != contract.DeliveryStateAuthFailed {
		t.Fatalf("state = %q, want auth_failed", result.Result.State)
	}
}

func waitReady(t *testing.T, ctx context.Context, mgr mcmanager.Manager, targetID contract.TargetID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := mgr.GetCluster(ctx, multicluster.ClusterName(targetID)); err == nil {
			// Controller runnables need a beat after engage.
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	t.Fatalf("target %q not engaged", targetID)
}
