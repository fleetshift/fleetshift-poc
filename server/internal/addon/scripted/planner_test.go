package scripted_test

import (
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
)

func TestPlanner_ConstantLatencyAndOutcome(t *testing.T) {
	p := scripted.NewPlanner()
	behavior := scripted.PhaseBehavior{
		Latency: scripted.ConstantLatency{Duration: 200 * time.Millisecond},
		Outcome: scripted.ConstantOutcome{Value: scripted.OutcomeSuccess},
	}
	key := scripted.AttemptKey{
		InstanceKey: "managed-resource:test-uid",
		Generation:  1,
		Operation:   scripted.OperationDeliver,
		Phase:       scripted.PhaseAcknowledgement,
	}

	for i := range 5 {
		d := p.Decide(behavior, key)
		if d.Latency != 200*time.Millisecond {
			t.Errorf("attempt %d: latency = %v, want 200ms", i, d.Latency)
		}
		if d.Outcome != scripted.OutcomeSuccess {
			t.Errorf("attempt %d: outcome = %v, want success", i, d.Outcome)
		}
	}
}

func TestPlanner_SequenceOutcome(t *testing.T) {
	p := scripted.NewPlanner()
	behavior := scripted.PhaseBehavior{
		Latency: scripted.ConstantLatency{Duration: 0},
		Outcome: scripted.SequenceOutcome{Values: []scripted.OutcomeValue{
			scripted.OutcomeFailure,
			scripted.OutcomeFailure,
			scripted.OutcomeSuccess,
		}},
	}
	key := scripted.AttemptKey{
		InstanceKey: "managed-resource:test-uid",
		Generation:  1,
		Operation:   scripted.OperationDeliver,
		Phase:       scripted.PhaseAcknowledgement,
	}

	expected := []scripted.OutcomeValue{
		scripted.OutcomeFailure,
		scripted.OutcomeFailure,
		scripted.OutcomeSuccess,
		scripted.OutcomeSuccess, // final value repeats
		scripted.OutcomeSuccess,
	}
	for i, want := range expected {
		d := p.Decide(behavior, key)
		if d.Outcome != want {
			t.Errorf("attempt %d: outcome = %v, want %v", i, d.Outcome, want)
		}
	}
}

func TestPlanner_IndependentKeys(t *testing.T) {
	p := scripted.NewPlanner()
	behavior := scripted.PhaseBehavior{
		Latency: scripted.ConstantLatency{Duration: 0},
		Outcome: scripted.SequenceOutcome{Values: []scripted.OutcomeValue{
			scripted.OutcomeFailure,
			scripted.OutcomeSuccess,
		}},
	}

	key1 := scripted.AttemptKey{InstanceKey: "managed-resource:uid-1", Generation: 1, Operation: scripted.OperationDeliver, Phase: scripted.PhaseAcknowledgement}
	key2 := scripted.AttemptKey{InstanceKey: "managed-resource:uid-2", Generation: 1, Operation: scripted.OperationDeliver, Phase: scripted.PhaseAcknowledgement}

	// Advance key1 twice (FAILURE, SUCCESS) before checking key2.
	d1a := p.Decide(behavior, key1)
	if d1a.Outcome != scripted.OutcomeFailure {
		t.Errorf("key1 first attempt: outcome = %v, want failure", d1a.Outcome)
	}
	d1b := p.Decide(behavior, key1)
	if d1b.Outcome != scripted.OutcomeSuccess {
		t.Errorf("key1 second attempt: outcome = %v, want success", d1b.Outcome)
	}

	// key2 should still be at cursor 0, independent of key1's advances.
	d2 := p.Decide(behavior, key2)
	if d2.Outcome != scripted.OutcomeFailure {
		t.Errorf("key2 first attempt: outcome = %v, want failure", d2.Outcome)
	}
}

func TestPlanner_NewGenerationResetsSequence(t *testing.T) {
	p := scripted.NewPlanner()
	behavior := scripted.PhaseBehavior{
		Latency: scripted.ConstantLatency{Duration: 0},
		Outcome: scripted.SequenceOutcome{Values: []scripted.OutcomeValue{
			scripted.OutcomeFailure,
			scripted.OutcomeSuccess,
		}},
	}

	key := scripted.AttemptKey{InstanceKey: "managed-resource:uid-1", Generation: 1, Operation: scripted.OperationDeliver, Phase: scripted.PhaseAcknowledgement}
	d := p.Decide(behavior, key)
	if d.Outcome != scripted.OutcomeFailure {
		t.Errorf("gen 1 first: outcome = %v, want failure", d.Outcome)
	}

	// New generation gets an independent cursor.
	key.Generation = 2
	d = p.Decide(behavior, key)
	if d.Outcome != scripted.OutcomeFailure {
		t.Errorf("gen 2 first: outcome = %v, want failure (independent cursor)", d.Outcome)
	}
}

func TestPlanner_Reset(t *testing.T) {
	p := scripted.NewPlanner()
	behavior := scripted.PhaseBehavior{
		Latency: scripted.ConstantLatency{Duration: 0},
		Outcome: scripted.SequenceOutcome{Values: []scripted.OutcomeValue{
			scripted.OutcomeFailure,
			scripted.OutcomeSuccess,
		}},
	}

	key := scripted.AttemptKey{InstanceKey: "managed-resource:uid-1", Generation: 1, Operation: scripted.OperationDeliver, Phase: scripted.PhaseAcknowledgement}
	p.Decide(behavior, key) // cursor 0 -> 1

	p.Reset("managed-resource:uid-1")

	// After reset, cursor should start at 0 again.
	d := p.Decide(behavior, key)
	if d.Outcome != scripted.OutcomeFailure {
		t.Errorf("after reset: outcome = %v, want failure (cursor reset)", d.Outcome)
	}
}

func TestPlanner_StressAdapterInstanceKey(t *testing.T) {
	// Verify that PR #114's "stress-delivery:" namespace works
	// independently of the canonical "managed-resource:" namespace.
	p := scripted.NewPlanner()
	behavior := scripted.PhaseBehavior{
		Latency: scripted.ConstantLatency{Duration: 100 * time.Millisecond},
		Outcome: scripted.SequenceOutcome{Values: []scripted.OutcomeValue{
			scripted.OutcomeFailure,
			scripted.OutcomeSuccess,
		}},
	}

	canonical := scripted.AttemptKey{InstanceKey: "managed-resource:some-uid", Generation: 1, Operation: scripted.OperationDeliver, Phase: scripted.PhaseAcknowledgement}
	stress := scripted.AttemptKey{InstanceKey: "stress-delivery:some-delivery-id", Generation: 1, Operation: scripted.OperationDeliver, Phase: scripted.PhaseAcknowledgement}

	// Advance canonical twice (FAILURE, SUCCESS) before checking stress.
	dc1 := p.Decide(behavior, canonical)
	if dc1.Outcome != scripted.OutcomeFailure {
		t.Errorf("canonical first: outcome = %v, want failure", dc1.Outcome)
	}
	dc2 := p.Decide(behavior, canonical)
	if dc2.Outcome != scripted.OutcomeSuccess {
		t.Errorf("canonical second: outcome = %v, want success", dc2.Outcome)
	}

	// Stress namespace should still be at cursor 0, independent of canonical.
	ds := p.Decide(behavior, stress)
	if ds.Outcome != scripted.OutcomeFailure {
		t.Errorf("stress first: outcome = %v, want failure", ds.Outcome)
	}
}
