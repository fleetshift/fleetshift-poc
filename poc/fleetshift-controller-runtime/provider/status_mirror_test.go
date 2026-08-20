package provider

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	deliveryv1 "github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/apis/delivery/v1alpha1"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/contract"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/platform"
)

func TestStatusMirrorReportsProgressThenTerminal(t *testing.T) {
	fake := platform.NewFake()
	m := newStatusMirror(fake)

	d := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "del-1", Namespace: "default"},
		Spec: deliveryv1.DeliverySpec{
			DeliveryID: "del-1",
			Generation: 1,
		},
		Status: deliveryv1.DeliveryStatus{
			Phase:              string(contract.DeliveryStateProgressing),
			Message:            "applying",
			ObservedGeneration: 1,
		},
	}

	if err := m.Mirror(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	events, results := fake.Snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if len(results) != 0 {
		t.Fatalf("results = %d, want 0", len(results))
	}
	if events[0].Event.Kind != contract.DeliveryEventProgress {
		t.Fatalf("event kind = %q", events[0].Event.Kind)
	}

	d.Status.Phase = string(contract.DeliveryStateDelivered)
	d.Status.Message = "applied"
	if err := m.Mirror(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	events, results = fake.Snapshot()
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if results[0].Result.State != contract.DeliveryStateDelivered {
		t.Fatalf("state = %q", results[0].Result.State)
	}
	// Progress event from the terminal transition is not expected; only ReportResult.
	if len(events) != 1 {
		t.Fatalf("events after terminal = %d, want 1 (no extra progress)", len(events))
	}
}

func TestStatusMirrorDedupesIdenticalStatus(t *testing.T) {
	fake := platform.NewFake()
	m := newStatusMirror(fake)

	d := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "del-1", Namespace: "default"},
		Spec:       deliveryv1.DeliverySpec{DeliveryID: "del-1", Generation: 1},
		Status: deliveryv1.DeliveryStatus{
			Phase:              string(contract.DeliveryStateDelivered),
			Message:            "done",
			ObservedGeneration: 1,
		},
	}
	if err := m.Mirror(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if err := m.Mirror(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	_, results := fake.Snapshot()
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1 (deduped)", len(results))
	}
}

func TestStatusMirrorPropagatesReporterError(t *testing.T) {
	m := newStatusMirror(&failingReporter{err: errors.New("platform down")})
	d := &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "del-1", Namespace: "default"},
		Spec:       deliveryv1.DeliverySpec{DeliveryID: "del-1", Generation: 1},
		Status: deliveryv1.DeliveryStatus{
			Phase:              string(contract.DeliveryStateDelivered),
			Message:            "done",
			ObservedGeneration: 1,
		},
	}
	if err := m.Mirror(context.Background(), d); err == nil {
		t.Fatal("expected error from reporter")
	}
}

func TestStatusMirrorIgnoresEmptyPhase(t *testing.T) {
	fake := platform.NewFake()
	m := newStatusMirror(fake)
	if err := m.Mirror(context.Background(), &deliveryv1.Delivery{
		ObjectMeta: metav1.ObjectMeta{Name: "empty"},
		Spec:       deliveryv1.DeliverySpec{DeliveryID: "empty", Generation: 1},
	}); err != nil {
		t.Fatal(err)
	}
	events, results := fake.Snapshot()
	if len(events) != 0 || len(results) != 0 {
		t.Fatalf("empty phase should not report: events=%d results=%d", len(events), len(results))
	}
}

type failingReporter struct {
	err error
}

func (f *failingReporter) ReportEvent(ctx context.Context, deliveryID contract.DeliveryID, generation contract.Generation, event contract.DeliveryEvent) error {
	return f.err
}
func (f *failingReporter) ReportResult(ctx context.Context, deliveryID contract.DeliveryID, generation contract.Generation, result contract.DeliveryResult) error {
	return f.err
}
func (f *failingReporter) ListActiveDeliveries(ctx context.Context, targetIDs []contract.TargetID) ([]contract.ActiveDelivery, error) {
	return nil, nil
}
