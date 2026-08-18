package scripted

import (
	"encoding/json"
	"time"
)

// Operation distinguishes delivery from removal.
type Operation int

const (
	OperationDeliver Operation = iota
	OperationRemove
)

func (o Operation) String() string {
	switch o {
	case OperationDeliver:
		return "deliver"
	case OperationRemove:
		return "remove"
	default:
		return "unknown"
	}
}

// Phase distinguishes the synchronous acknowledgement from the
// asynchronous completion within one operation.
type Phase int

const (
	PhaseAcknowledgement Phase = iota
	PhaseCompletion
)

func (p Phase) String() string {
	switch p {
	case PhaseAcknowledgement:
		return "acknowledgement"
	case PhaseCompletion:
		return "completion"
	default:
		return "unknown"
	}
}

// OutcomeValue is the result of a single phase attempt.
type OutcomeValue int

const (
	OutcomeSuccess OutcomeValue = iota
	OutcomeFailure
)

func (o OutcomeValue) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	default:
		return "unknown"
	}
}

// LatencyDecider resolves the latency for one phase attempt.
// The interface is intentionally narrow so the planner can dispatch
// on type without interpreting the configuration. v1 provides only
// ConstantLatency; future stochastic arms (e.g. BoundedNormalLatency)
// implement this same interface.
type LatencyDecider interface {
	// ResolveLatency returns the duration to wait for this attempt.
	ResolveLatency() time.Duration
}

// ConstantLatency always returns the same duration.
type ConstantLatency struct {
	Duration time.Duration
}

// ResolveLatency returns the constant duration.
func (c ConstantLatency) ResolveLatency() time.Duration {
	return c.Duration
}

// OutcomeDecider resolves the outcome for one phase attempt given the
// zero-based cursor position. It returns the outcome and the next
// cursor value.
type OutcomeDecider interface {
	// ResolveOutcome returns the outcome for the given cursor position
	// and the next cursor value.
	ResolveOutcome(cursor int) (OutcomeValue, int)
}

// ConstantOutcome always returns the same value.
type ConstantOutcome struct {
	Value OutcomeValue
}

// ResolveOutcome returns the constant value and advances the cursor.
func (c ConstantOutcome) ResolveOutcome(cursor int) (OutcomeValue, int) {
	return c.Value, cursor + 1
}

// SequenceOutcome returns values from a nonempty finite sequence,
// repeating the final value after exhaustion.
type SequenceOutcome struct {
	Values []OutcomeValue
}

// ResolveOutcome returns the value at min(cursor, len-1) and advances
// the cursor.
func (s SequenceOutcome) ResolveOutcome(cursor int) (OutcomeValue, int) {
	idx := cursor
	if idx >= len(s.Values) {
		idx = len(s.Values) - 1
	}
	return s.Values[idx], cursor + 1
}

// PhaseBehavior holds the configured latency and outcome deciders for
// one phase of one operation.
type PhaseBehavior struct {
	Latency LatencyDecider
	Outcome OutcomeDecider
}

// OperationSpec holds the acknowledgement and completion behavior for
// one operation (deliver or remove).
type OperationSpec struct {
	Acknowledgement PhaseBehavior
	Completion      PhaseBehavior
}

// NormalizedSpec is the fully validated and defaulted spec extracted
// from a ScriptedResourceSpec proto message. It is immutable after
// construction.
type NormalizedSpec struct {
	Delivery  OperationSpec
	Removal   OperationSpec
	Inventory InventoryProjection
}

// InventoryProjection holds the labels and observation to project onto
// the managed resource after a successful delivery. An empty projection
// (nil labels, nil observation) is used when inventory is omitted from
// the spec, ensuring that a later successful generation clears older
// scripted inventory.
type InventoryProjection struct {
	Labels      map[string]string
	Observation json.RawMessage
}
