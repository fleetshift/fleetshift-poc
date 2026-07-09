// Command example wires the FleetShift multicluster provider, an fsruntime
// host manager, and a Delivery reconciler — the same shape as
// multicluster-runtime's kind example, but with FleetShift delivery targets
// instead of kube API servers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
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

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := ctrl.Log.WithName("example")

	fake := platform.NewFake()
	target := contract.TargetInfo{
		ID:   "demo-target",
		Type: "gcphcp",
		Name: "demo",
	}

	prov, err := provider.New(provider.Options{
		Scheme:   deliveryv1.Scheme,
		Reporter: fake,
		Logger:   logr.Logger(logger).WithName("provider"),
		Targets:  []contract.TargetInfo{target},
	})
	if err != nil {
		logger.Error(err, "provider")
		os.Exit(1)
	}

	host, err := fsruntime.NewManager(fsruntime.Options{
		Scheme: deliveryv1.Scheme,
		Logger: logr.Logger(logger).WithName("host"),
	})
	if err != nil {
		logger.Error(err, "host manager")
		os.Exit(1)
	}

	mcMgr, err := mcmanager.WithMultiCluster(host, prov)
	if err != nil {
		logger.Error(err, "multicluster manager")
		os.Exit(1)
	}

	if err := (&controllers.DeliveryReconciler{Reporter: fake}).SetupWithManager(mcMgr); err != nil {
		logger.Error(err, "controller")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()
	go func() {
		if err := mcMgr.Start(ctx); err != nil {
			logger.Error(err, "manager exited")
			os.Exit(1)
		}
	}()

	// Wait for the target to engage, then simulate a platform delivery.
	waitForTarget(ctx, mcMgr, target.ID, logger)

	manifest, _ := json.Marshal(map[string]string{"name": "demo-cluster"})
	deliveryID := contract.DeliveryID("example-1")
	logger.Info("dispatching delivery", "id", deliveryID)
	if err := fake.Dispatch(ctx, prov, target, deliveryID, []contract.Manifest{{
		ManifestType: "api.gcphcp.cluster",
		Raw:          manifest,
	}}, contract.DeliveryAuth{Token: "example-token"}, 1, contract.DeliveryOperationDeliver); err != nil {
		logger.Error(err, "dispatch")
		os.Exit(1)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, ok := fake.WaitForResult(waitCtx, deliveryID)
	if !ok {
		logger.Error(fmt.Errorf("timeout"), "waiting for result")
		os.Exit(1)
	}
	logger.Info("delivery complete", "state", result.Result.State, "message", result.Result.Message)
}

func waitForTarget(ctx context.Context, mgr mcmanager.Manager, id contract.TargetID, logger logr.Logger) {
	for {
		if _, err := mgr.GetCluster(ctx, multicluster.ClusterName(id)); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			logger.Error(ctx.Err(), "waiting for target")
			os.Exit(1)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
