// Package controllers contains the Delivery reconciler. It is written as a
// normal controller-runtime reconciler; the only multi-cluster awareness is
// resolving the target cluster from the request via the multicluster manager.
//
// Controllers interact only with the Kubernetes-shaped client API. Delivery
// outcomes are written to Delivery status; the provider's status mirror
// translates those writes into FleetShift DeliveryReporter calls.
package controllers

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	deliveryv1 "github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/apis/delivery/v1alpha1"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/contract"
)

// DeliveryReconciler reconciles Delivery objects projected into each
// target's fsruntime cluster.
type DeliveryReconciler struct {
	Manager mcmanager.Manager
}

// Reconcile applies a delivery and writes progress/result to Delivery
// status — the same mental model as any kube controller. The provider
// status mirror reports status to the platform.
func (r *DeliveryReconciler) Reconcile(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("cluster", req.ClusterName, "delivery", req.Name)

	cl, err := r.Manager.GetCluster(ctx, req.ClusterName)
	if err != nil {
		return ctrl.Result{}, err
	}

	var delivery deliveryv1.Delivery
	if err := cl.GetClient().Get(ctx, req.NamespacedName, &delivery); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if delivery.Status.ObservedGeneration == delivery.Spec.Generation &&
		contract.DeliveryState(delivery.Status.Phase).IsTerminal() {
		return ctrl.Result{}, nil
	}

	// First progress status write transitions the platform delivery to progressing.
	delivery.Status.Phase = string(contract.DeliveryStateProgressing)
	delivery.Status.Message = fmt.Sprintf("reconciling %s on target %s", delivery.Spec.Operation, delivery.Spec.TargetID)
	delivery.Status.ObservedGeneration = delivery.Spec.Generation
	if err := cl.GetClient().Status().Update(ctx, &delivery); err != nil {
		return ctrl.Result{}, err
	}

	// Simulate target-side work. A real addon (e.g. gcphcp) would call
	// cloud APIs here. The POC "applies" by accepting the manifest.
	phase := string(contract.DeliveryStateDelivered)
	message := fmt.Sprintf("applied %s (%s)", delivery.Spec.ManifestType, delivery.Spec.Operation)
	if delivery.Spec.Operation == string(contract.DeliveryOperationRemove) {
		message = fmt.Sprintf("removed %s", delivery.Spec.ManifestType)
	}
	if delivery.Spec.AuthToken == "" {
		phase = string(contract.DeliveryStateAuthFailed)
		message = "missing auth token"
	}

	delivery.Status.Phase = phase
	delivery.Status.Message = message
	if err := cl.GetClient().Status().Update(ctx, &delivery); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("delivery reconciled", "phase", delivery.Status.Phase)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the multi-cluster Delivery controller.
func (r *DeliveryReconciler) SetupWithManager(mgr mcmanager.Manager) error {
	return r.SetupWithManagerNamed(mgr, "delivery")
}

// SetupWithManagerNamed is like SetupWithManager but allows a unique
// controller name (needed when multiple managers are created in one process,
// e.g. parallel tests sharing a metrics registry).
func (r *DeliveryReconciler) SetupWithManagerNamed(mgr mcmanager.Manager, name string) error {
	r.Manager = mgr
	return mcbuilder.ControllerManagedBy(mgr).
		Named(name).
		For(&deliveryv1.Delivery{}).
		Complete(r)
}
