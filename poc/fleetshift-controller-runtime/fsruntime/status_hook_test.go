package fsruntime_test

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	deliveryv1 "github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/apis/delivery/v1alpha1"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/fsruntime"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/store"
)

func TestStatusHookFailureDoesNotPersist(t *testing.T) {
	st := store.New(deliveryv1.Scheme)
	hookErr := errors.New("mirror failed")
	cl, err := fsruntime.NewCluster(fsruntime.Options{
		Scheme: deliveryv1.Scheme,
		Store:  st,
		StatusHook: func(ctx context.Context, obj client.Object) error {
			return hookErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	d := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "d1", Namespace: "default"},
		Spec:       deliveryv1.DeliverySpec{DeliveryID: "d1", Generation: 1},
	}
	if err := cl.DirectClient().Create(context.Background(), d); err != nil {
		t.Fatal(err)
	}

	d.Status.Phase = "progressing"
	d.Status.Message = "working"
	if err := cl.DirectClient().Status().Update(context.Background(), d); !errors.Is(err, hookErr) {
		t.Fatalf("Status().Update error = %v, want %v", err, hookErr)
	}

	got := &deliveryv1.Delivery{}
	if err := cl.DirectClient().Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "d1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "" {
		t.Fatalf("status was persisted despite hook failure: %+v", got.Status)
	}
}

func TestStatusHookSuccessPersists(t *testing.T) {
	st := store.New(deliveryv1.Scheme)
	called := false
	cl, err := fsruntime.NewCluster(fsruntime.Options{
		Scheme: deliveryv1.Scheme,
		Store:  st,
		StatusHook: func(ctx context.Context, obj client.Object) error {
			called = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	d := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "d1", Namespace: "default"},
		Spec:       deliveryv1.DeliverySpec{DeliveryID: "d1", Generation: 1},
	}
	if err := cl.DirectClient().Create(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	d.Status.Phase = "delivered"
	if err := cl.DirectClient().Status().Update(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("status hook was not called")
	}
	got := &deliveryv1.Delivery{}
	if err := cl.DirectClient().Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "d1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "delivered" {
		t.Fatalf("phase = %q", got.Status.Phase)
	}
}

func TestFullUpdateDoesNotInvokeStatusHook(t *testing.T) {
	st := store.New(deliveryv1.Scheme)
	called := false
	cl, err := fsruntime.NewCluster(fsruntime.Options{
		Scheme: deliveryv1.Scheme,
		Store:  st,
		StatusHook: func(ctx context.Context, obj client.Object) error {
			called = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	d := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "d1", Namespace: "default"},
		Spec:       deliveryv1.DeliverySpec{DeliveryID: "d1", Generation: 1},
	}
	if err := cl.DirectClient().Create(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	d.Spec.Generation = 2
	if err := cl.DirectClient().Update(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("status hook must not run on full-object Update (projection path)")
	}
}
