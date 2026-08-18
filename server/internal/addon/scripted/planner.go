package scripted

import (
	"sync"
	"time"
)

// AttemptKey uniquely identifies one phase attempt across all scripted
// work. The canonical agent uses "managed-resource:<UID>" as the
// InstanceKey; The stress test adapter uses "stress-delivery:<ID>"
// inside a fresh run-scoped planner, so it does not need managed
// manifests or a fabricated resource UID and cannot collide with the
// canonical namespace.
type AttemptKey struct {
	InstanceKey string
	Generation  int64
	Operation   Operation
	Phase       Phase
}

// PhaseDecision is the resolved latency and outcome for one phase
// attempt. It is immutable after construction.
type PhaseDecision struct {
	Latency time.Duration
	Outcome OutcomeValue
}

// Planner owns the sequence state for deterministic phase decisions.
// It is safe for concurrent use.
//
// The planning seam is deliberately narrow: callers provide a
// PhaseBehavior (latency + outcome deciders) and an AttemptKey, and
// receive one PhaseDecision. The planner does not interpret resource
// specs, manifests, or delivery IDs.
type Planner struct {
	mu      sync.Mutex
	cursors map[AttemptKey]int
}

// NewPlanner creates a new planner with empty cursor state.
func NewPlanner() *Planner {
	return &Planner{
		cursors: make(map[AttemptKey]int),
	}
}

// Decide resolves a phase decision for the given behavior and attempt
// key. The cursor for the key is atomically advanced under the lock so
// concurrent callers sharing the same key each see their own unique
// cursor position.
func (p *Planner) Decide(behavior PhaseBehavior, key AttemptKey) PhaseDecision {
	latency := behavior.Latency.ResolveLatency()

	p.mu.Lock()
	cursor := p.cursors[key]
	outcome, next := behavior.Outcome.ResolveOutcome(cursor)
	p.cursors[key] = next
	p.mu.Unlock()

	return PhaseDecision{
		Latency: latency,
		Outcome: outcome,
	}
}

// Reset removes all cursor state for the given instance key across all
// generations, operations, and phases. This is used when a resource UID
// is retired (successful removal or same-name recreation).
func (p *Planner) Reset(instanceKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k := range p.cursors {
		if k.InstanceKey == instanceKey {
			delete(p.cursors, k)
		}
	}
}
