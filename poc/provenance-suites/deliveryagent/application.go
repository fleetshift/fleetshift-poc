package deliveryagent

import (
	"context"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Application owns ordinary delivery progress, generation ordering, and target
// effects. NewAttempt creates delivery-local operational state without admitting
// a generation or acknowledging work. Applications must support concurrent attempts.
type Application interface {
	NewAttempt() ApplicationAttempt
}

// ApplicationAttempt receives only fully evaluated ordinary actions. Accept
// must enforce generation ordering with effects or durable acceptance, even if
// an earlier precheck succeeded. A nil error permits acknowledgement: the work
// is completed or enough evaluated work and execution state are safely retained
// to resume it. Accepted/in-progress work must not be mistaken for completed work.
// An application may perform effects synchronously or schedule retained work.
// Close releases transient observations or guards, including after evaluation
// failure, without undoing accepted work. An attempt is used by one delivery.
type ApplicationAttempt interface {
	Accept(context.Context, AuthorizedAction) error
	Close()
}

// GenerationPrechecker is an optional application-attempt capability. Precheck
// runs after root authentication and common scope validation, before supporting
// evaluation. It reports completed only for work already fully evaluated and
// completed, rejects stale generations, or returns false to proceed or defer.
// It may observe state or hold a transient guard but must not advance accepted
// generations or authorize effects. Accept remains responsible for final fencing.
// Verifier serializes verification around this callback. Precheck, and an attempt
// retaining a guard, must not call verifier methods before releasing that guard.
// A cheap precheck is recommended; omitting it can make an invalid retry fail
// evaluation before previously successful completion is discovered.
type GenerationPrechecker interface {
	Precheck(context.Context, protocol.DeliveryScope) (completed bool, err error)
}

// MemoryApplication is the POC's synchronous in-memory target application.
// It retains detached put actions and generation high-water marks after removal.
// Its mutex fences only progress checks and effects; evaluation holds no target
// lock. MemoryApplication is safe for concurrent use and is not crash durable.
type MemoryApplication struct {
	mu          sync.Mutex
	applied     map[protocol.FullResourceName]AppliedDelivery
	generations map[protocol.FullResourceName]uint64
}

// NewMemoryApplication constructs an empty in-memory target application.
func NewMemoryApplication() *MemoryApplication {
	return &MemoryApplication{
		applied:     make(map[protocol.FullResourceName]AppliedDelivery),
		generations: make(map[protocol.FullResourceName]uint64),
	}
}

// NewAttempt returns an attempt with the optional generation precheck enabled.
func (a *MemoryApplication) NewAttempt() ApplicationAttempt {
	return &memoryAttempt{application: a}
}

// Applied returns a detached observation of the live accepted put action.
// Removal leaves no view; its generation fence is retained separately.
func (a *MemoryApplication) Applied(name protocol.FullResourceName) (AppliedDelivery, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	view, exists := a.applied[name]
	if !exists {
		return AppliedDelivery{}, false
	}
	return AppliedDelivery{AuthorizedAction: view.AuthorizedAction.Clone()}, true
}

type memoryAttempt struct {
	application *MemoryApplication
}

func (a *memoryAttempt) Precheck(ctx context.Context, scope protocol.DeliveryScope) (bool, error) {
	a.application.mu.Lock()
	defer a.application.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return a.application.completedLocked(scope)
}

func (a *memoryAttempt) Accept(ctx context.Context, action AuthorizedAction) error {
	a.application.mu.Lock()
	defer a.application.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	completed, err := a.application.completedLocked(action.Scope)
	if err != nil || completed {
		return err
	}
	// The application takes ownership at this boundary. A caller may retain
	// or subsequently mutate its evaluation result without changing target state.
	accepted := action.Clone()
	a.application.generations[action.Scope.FullResourceName] = action.Scope.Generation
	if action.Scope.Action == protocol.ActionRemove {
		delete(a.application.applied, action.Scope.FullResourceName)
		return nil
	}
	a.application.applied[action.Scope.FullResourceName] = AppliedDelivery{AuthorizedAction: accepted}
	return nil
}

func (*memoryAttempt) Close() {}

func (a *MemoryApplication) completedLocked(scope protocol.DeliveryScope) (bool, error) {
	previous, exists := a.generations[scope.FullResourceName]
	if !exists {
		return false, nil
	}
	if scope.Generation < previous {
		return false, fmt.Errorf("%w: generation %d is older than %d", ErrGeneration, scope.Generation, previous)
	}
	return scope.Generation == previous, nil
}
