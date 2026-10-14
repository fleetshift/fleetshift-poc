package provider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	deliveryv1 "github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/apis/delivery/v1alpha1"
	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/contract"
)

// statusMirror translates Delivery status writes into DeliveryReporter
// calls. It is invoked from Status().Update *before* the store persists
// the object: a reporter failure fails the status write so the
// controller requeues — the same retry loop as a failed kube Status().Update.
//
// Report* runs before local persistence so the platform (the durable sink
// for delivery outcomes) is updated first. Duplicate identical status
// writes are deduped so reconcile retries after a successful report but
// failed store write do not spam the platform.
type statusMirror struct {
	reporter contract.DeliveryReporter

	mu   sync.Mutex
	last map[string]mirrorKey // deliveryID -> last successfully reported
}

type mirrorKey struct {
	generation contract.Generation
	phase      string
	message    string
}

func newStatusMirror(reporter contract.DeliveryReporter) *statusMirror {
	return &statusMirror{
		reporter: reporter,
		last:     make(map[string]mirrorKey),
	}
}

// Hook returns a StatusHook suitable for fsruntime.Options.
func (m *statusMirror) Hook() func(ctx context.Context, obj client.Object) error {
	return m.Mirror
}

// Mirror reports Delivery status to the platform. Non-Delivery objects and
// empty phases are no-ops. Errors from the reporter are returned so
// Status().Update fails and nothing is stored.
func (m *statusMirror) Mirror(ctx context.Context, obj client.Object) error {
	d, ok := obj.(*deliveryv1.Delivery)
	if !ok {
		return nil
	}
	if d.Status.Phase == "" {
		return nil
	}

	deliveryID := contract.DeliveryID(d.Spec.DeliveryID)
	if deliveryID == "" {
		deliveryID = contract.DeliveryID(d.Name)
	}
	generation := contract.Generation(d.Spec.Generation)
	phase := d.Status.Phase
	message := d.Status.Message

	key := mirrorKey{generation: generation, phase: phase, message: message}
	id := string(deliveryID)

	m.mu.Lock()
	if prev, ok := m.last[id]; ok && prev == key {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	state := contract.DeliveryState(phase)
	if state.IsTerminal() {
		result := contract.DeliveryResult{
			State:   state,
			Message: message,
		}
		if err := m.reporter.ReportResult(ctx, deliveryID, generation, result); err != nil {
			return fmt.Errorf("status mirror ReportResult: %w", err)
		}
	} else {
		event := contract.DeliveryEvent{
			Timestamp: time.Now().UTC(),
			Kind:      eventKindForPhase(state),
			Message:   message,
		}
		if err := m.reporter.ReportEvent(ctx, deliveryID, generation, event); err != nil {
			return fmt.Errorf("status mirror ReportEvent: %w", err)
		}
	}

	m.mu.Lock()
	m.last[id] = key
	m.mu.Unlock()
	return nil
}

// Reset clears dedupe state for a delivery (e.g. when a newer generation
// is projected). Without this, a re-delivered generation with the same
// phase/message as a prior run would be silently skipped.
func (m *statusMirror) Reset(deliveryID contract.DeliveryID) {
	m.mu.Lock()
	delete(m.last, string(deliveryID))
	m.mu.Unlock()
}

func eventKindForPhase(state contract.DeliveryState) contract.DeliveryEventKind {
	switch state {
	case contract.DeliveryStateFailed, contract.DeliveryStateAuthFailed:
		return contract.DeliveryEventError
	case contract.DeliveryStatePartial:
		return contract.DeliveryEventWarning
	default:
		return contract.DeliveryEventProgress
	}
}
