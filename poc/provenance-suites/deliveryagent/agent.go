// Package deliveryagent models the target verifier. It begins from
// bootstrapped trust configuration, verifies provenance under matched
// delivery policy, derives common actions, and delegates progress and effects
// to an application. Acknowledgement follows evaluation and safe acceptance.
package deliveryagent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stolostron/ocm-fleet/poc/provenance-suites/directkey"
	"github.com/stolostron/ocm-fleet/poc/provenance-suites/protocol"
)

var (
	ErrGeneration = errors.New("delivery generation is stale")
	ErrLogFork    = errors.New("package does not extend retained evidence-log checkpoint")

	// ErrAcknowledgementLost is a test fault injected after an otherwise
	// successful, locally retained delivery.
	ErrAcknowledgementLost = errors.New("delivery acknowledgement was lost")
	ErrCheckpointStale     = errors.New("manager used a stale agent checkpoint")
	ErrDeliveryUnavailable = errors.New("delivery did not reach the agent")

	// ErrFulfillmentRelationRequired is returned when a managed-resource
	// authorization has no verified fulfillment relation.
	ErrFulfillmentRelationRequired = errors.New("managed resource requires a verified fulfillment relation")
)

// CheckpointStaleError tells the manager that proof construction started from
// an older checkpoint than this agent currently retains.
type CheckpointStaleError struct {
	checkpoint protocol.Checkpoint
	cause      error
}

func (e *CheckpointStaleError) Error() string {
	return fmt.Sprintf("%v: agent is at checkpoint size %d: %v", ErrCheckpointStale, e.checkpoint.Size, e.cause)
}

func (e *CheckpointStaleError) Unwrap() error {
	return ErrCheckpointStale
}

func (e *CheckpointStaleError) LatestCheckpoint() protocol.Checkpoint {
	return e.checkpoint
}

// Config provisions one delivery agent.
type Config struct {
	// Tenant is the provisioned external identity for resource deliveries.
	Tenant protocol.Tenant
	// ProviderTenant references the platform's single provider tenant, whose
	// fulfillment relations this target admits. It is provisioned separately
	// from the authority/profile configurations used to authenticate evidence.
	ProviderTenant protocol.Tenant
	// A stand-in, single target identifier for this delivery agent.
	// A real agent may support multiple, dynamic target IDs.
	TargetID string
}

// AppliedDelivery exposes the accepted put action retained by the POC's fake
// target application. Removal deletes this view and retains only its generation.
// Production delivery agents need not persist the complete action or its basis.
type AppliedDelivery struct {
	AuthorizedAction
}

// Agent coordinates common verification with an application and transport ACKs.
// Verifier owns verification state; the application owns ordinary progress,
// generation fencing, and effects. Agent is safe for concurrent delivery.
type Agent struct {
	*Verifier
	mu sync.Mutex

	application Application
	// The default direct-key and memory application expose POC observations.
	// Custom applications retain and expose their own operational state.
	profile *directkey.Target
	memory  *MemoryApplication

	failBeforeAccepting     uint64
	loseNextAcknowledgement bool
	suiteApplyCount         uint64
}

// New constructs the POC agent with direct-key verification and synchronous
// in-memory application. Use [NewVerifier] and [NewAgent] for explicit composition.
func New(config Config) (*Agent, error) {
	profile := directkey.NewTarget()
	verifier, err := NewVerifier(config, func(kind protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		return profile, kind == profile.ProvenanceType()
	})
	if err != nil {
		return nil, err
	}
	application := NewMemoryApplication()
	agent, err := NewAgent(verifier, application)
	if err != nil {
		return nil, err
	}
	agent.profile = profile
	return agent, nil
}

// NewAgent composes a common verifier with application-owned progress and
// execution. The application decides whether safe acceptance completes effects
// or retains evaluated work for later execution. Neither path ACKs before evaluation.
func NewAgent(verifier *Verifier, application Application) (*Agent, error) {
	if verifier == nil || application == nil {
		return nil, errors.New("verifier and application are required")
	}
	agent := &Agent{Verifier: verifier, application: application}
	agent.memory, _ = application.(*MemoryApplication)
	return agent, nil
}

// Deliver verifies the package and delegates ordinary safe acceptance or
// profile-owned apply before acknowledging. See [Agent.DeliverContext].
func (a *Agent) Deliver(pkg protocol.DeliveryPackage) error {
	return a.DeliverContext(context.Background(), pkg)
}

// DeliverContext verifies a bounded delivery through [Verifier.Verify].
// An attempt's optional precheck runs only after authenticated scope validation;
// omitting it defers application progress queries until a complete action exists.
// Every attempt is closed, including on evaluation or acceptance failure.
// A nil return acknowledges only previously completed work, successful suite
// apply, or an application Accept that guarantees safe progress or resumption.
// The default memory application completes effects before ACK; another application
// may safely retain evaluated work and ACK before effects. No ACK precedes evaluation.
func (a *Agent) DeliverContext(ctx context.Context, pkg protocol.DeliveryPackage) error {
	a.mu.Lock()
	if a.failBeforeAccepting > 0 {
		a.failBeforeAccepting--
		a.mu.Unlock()
		return ErrDeliveryUnavailable
	}
	a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	attempt := a.application.NewAttempt()
	if attempt == nil {
		return errors.New("application returned no delivery attempt")
	}
	defer attempt.Close()
	var precheck GenerationPrecheck
	if checker, ok := attempt.(GenerationPrechecker); ok {
		precheck = checker.Precheck
	}
	result, err := a.Verify(ctx, pkg, precheck)
	if err != nil {
		return err
	}
	if err := a.acceptVerifiedDelivery(ctx, result, attempt); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.loseNextAcknowledgement {
		a.loseNextAcknowledgement = false
		return ErrAcknowledgementLost
	}
	return nil
}

func (a *Agent) acceptVerifiedDelivery(ctx context.Context, result VerifiedDelivery, attempt ApplicationAttempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch {
	case result.Completed:
		return nil
	case result.Action != nil:
		return attempt.Accept(ctx, *result.Action)
	case result.SuiteEvent != nil:
		a.mu.Lock()
		a.suiteApplyCount++
		a.mu.Unlock()
		return result.SuiteEvent.Apply(ctx)
	default:
		return errors.New("verification returned no completed work, action, or suite event")
	}
}

// Applied returns a detached observation from the default memory application.
// Custom applications expose their own progress and target observations.
func (a *Agent) Applied(name protocol.FullResourceName) (AppliedDelivery, bool) {
	if a.memory == nil {
		return AppliedDelivery{}, false
	}
	return a.memory.Applied(name)
}

// SuiteApplyCount is the number of evaluated suite events Deliver sent for apply.
// Intent, trust-config-update, and unowned predicates do not increment it.
func (a *Agent) SuiteApplyCount() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.suiteApplyCount
}

// PublicKey returns the retained direct-key mapping for tests.
func (a *Agent) PublicKey(principal protocol.Principal) ([]byte, bool) {
	if a.profile == nil {
		return nil, false
	}
	return a.profile.PublicKey(principal)
}

// FailNextDeliveriesBeforeAccepting injects transport failures before verify.
func (a *Agent) FailNextDeliveriesBeforeAccepting(count uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failBeforeAccepting += count
}

// LoseNextAcknowledgement injects a lost ack after local acceptance.
func (a *Agent) LoseNextAcknowledgement() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.loseNextAcknowledgement = true
}

func cloneManifests(in []protocol.TypedManifest) []protocol.TypedManifest {
	if len(in) == 0 {
		return nil
	}
	out := make([]protocol.TypedManifest, len(in))
	for i, item := range in {
		out[i] = protocol.TypedManifest(protocol.Encoded(item).Clone())
	}
	return out
}

// cloneApplyRequest detaches only the data exposed to the profile's Apply API.
func cloneApplyRequest(in protocol.ApplyRequest) protocol.ApplyRequest {
	out := in
	out.Authenticated.SatisfiedConstraints = cloneSlice(in.Authenticated.SatisfiedConstraints)
	out.Assertion.Bytes = cloneBytes(in.Assertion.Bytes)
	out.Statement = cloneSignedStatement(in.Statement)
	out.Temporal = cloneSubjectTemporalInfo(in.Temporal)
	return out
}

func cloneSignedStatement(in protocol.SignedStatement) protocol.SignedStatement {
	return protocol.SignedStatement{
		Evidence: protocol.TypedEvidence{
			ProvenanceType: in.Evidence.ProvenanceType,
			Encoded:        in.Evidence.Encoded.Clone(),
		},
		Support: protocol.SupportMaterial(protocol.Encoded(in.Support).Clone()),
	}
}

func cloneSubjectTemporalInfo(in protocol.VerifiedSubjectTemporalInfo) protocol.VerifiedSubjectTemporalInfo {
	out := protocol.VerifiedSubjectTemporalInfo{Times: cloneSlice(in.Times)}
	if in.LogPosition != nil {
		position := *in.LogPosition
		out.LogPosition = &position
	}
	return out
}
