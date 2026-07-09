// Package platform is a fake FleetShift control plane for the POC.
// It records DeliveryReporter calls and can dispatch Deliver/Remove to
// a DeliveryAgent, simulating orchestration without the real server.
package platform

import (
	"context"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/poc/fleetshift-controller-runtime/contract"
)

// EventRecord is a recorded ReportEvent call.
type EventRecord struct {
	DeliveryID contract.DeliveryID
	Generation contract.Generation
	Event      contract.DeliveryEvent
}

// ResultRecord is a recorded ReportResult call.
type ResultRecord struct {
	DeliveryID contract.DeliveryID
	Generation contract.Generation
	Result     contract.DeliveryResult
}

// Fake is an in-memory platform stand-in.
type Fake struct {
	mu sync.Mutex

	Events  []EventRecord
	Results []ResultRecord
	Active  []contract.ActiveDelivery

	// generations tracks the latest accepted generation per delivery so
	// stale reports are discarded (mirrors DeliveryReportService).
	generations map[contract.DeliveryID]contract.Generation
}

// NewFake returns an empty Fake platform.
func NewFake() *Fake {
	return &Fake{
		generations: make(map[contract.DeliveryID]contract.Generation),
	}
}

var _ contract.DeliveryReporter = (*Fake)(nil)

// ReportEvent implements contract.DeliveryReporter.
func (f *Fake) ReportEvent(ctx context.Context, deliveryID contract.DeliveryID, generation contract.Generation, event contract.DeliveryEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, ok := f.generations[deliveryID]; ok && generation < current {
		return nil // stale
	}
	f.generations[deliveryID] = generation
	f.Events = append(f.Events, EventRecord{DeliveryID: deliveryID, Generation: generation, Event: event})
	return nil
}

// ReportResult implements contract.DeliveryReporter.
func (f *Fake) ReportResult(ctx context.Context, deliveryID contract.DeliveryID, generation contract.Generation, result contract.DeliveryResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, ok := f.generations[deliveryID]; ok && generation < current {
		return nil
	}
	f.generations[deliveryID] = generation
	f.Results = append(f.Results, ResultRecord{DeliveryID: deliveryID, Generation: generation, Result: result})
	// Drop from active set on terminal result.
	if result.State.IsTerminal() {
		kept := f.Active[:0]
		for _, ad := range f.Active {
			if ad.DeliveryID != deliveryID {
				kept = append(kept, ad)
			}
		}
		f.Active = kept
	}
	return nil
}

// ListActiveDeliveries implements contract.DeliveryReporter.
func (f *Fake) ListActiveDeliveries(ctx context.Context, targetIDs []contract.TargetID) ([]contract.ActiveDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(targetIDs) == 0 {
		out := make([]contract.ActiveDelivery, len(f.Active))
		copy(out, f.Active)
		return out, nil
	}
	want := make(map[contract.TargetID]struct{}, len(targetIDs))
	for _, id := range targetIDs {
		want[id] = struct{}{}
	}
	var out []contract.ActiveDelivery
	for _, ad := range f.Active {
		if _, ok := want[ad.Target.ID]; ok {
			out = append(out, ad)
		}
	}
	return out, nil
}

// SeedActive adds an in-flight delivery for recovery testing.
func (f *Fake) SeedActive(ad contract.ActiveDelivery) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Active = append(f.Active, ad)
	f.generations[ad.DeliveryID] = ad.Generation
}

// Dispatch simulates orchestration calling the agent, and tracks the
// delivery as active until a terminal result arrives.
func (f *Fake) Dispatch(ctx context.Context, agent contract.DeliveryAgent, target contract.TargetInfo, deliveryID contract.DeliveryID, manifests []contract.Manifest, auth contract.DeliveryAuth, generation contract.Generation, op contract.DeliveryOperation) error {
	f.mu.Lock()
	f.Active = append(f.Active, contract.ActiveDelivery{
		DeliveryID: deliveryID,
		Target:     target,
		Manifests:  manifests,
		Generation: generation,
		Operation:  op,
		State:      contract.DeliveryStatePending,
		Auth:       auth,
	})
	f.generations[deliveryID] = generation
	f.mu.Unlock()

	switch op {
	case contract.DeliveryOperationRemove:
		return agent.Remove(ctx, target, deliveryID, manifests, auth, nil, generation)
	default:
		return agent.Deliver(ctx, target, deliveryID, manifests, auth, nil, generation)
	}
}

// WaitForResult blocks until a terminal result for deliveryID is recorded
// or the context is done. Deterministic polling — no time.Sleep in tests
// that can use a short timeout context instead.
func (f *Fake) WaitForResult(ctx context.Context, deliveryID contract.DeliveryID) (ResultRecord, bool) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		f.mu.Lock()
		for i := len(f.Results) - 1; i >= 0; i-- {
			if f.Results[i].DeliveryID == deliveryID {
				r := f.Results[i]
				f.mu.Unlock()
				return r, true
			}
		}
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return ResultRecord{}, false
		case <-ticker.C:
		}
	}
}

// Snapshot returns copies of recorded events and results.
func (f *Fake) Snapshot() (events []EventRecord, results []ResultRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	events = append([]EventRecord(nil), f.Events...)
	results = append([]ResultRecord(nil), f.Results...)
	return events, results
}
