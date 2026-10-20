package deliveryagent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stolostron/ocm-fleet/poc/provenance-suites/protocol"
	"github.com/stolostron/ocm-fleet/poc/provenance-suites/temporal"
)

// GenerationPrecheck optionally consults application progress for a validated
// authenticated root scope. See [GenerationPrechecker]. A nil callback defers
// generation checks until the application receives the completed action.
type GenerationPrecheck func(context.Context, protocol.DeliveryScope) (completed bool, err error)

// VerifiedDelivery is the result of common delivery verification. Exactly one
// of Action, Completed, or SuiteEvent is set on success. Action requires application-owned
// generation admission and safe acceptance before acknowledgement. Completed
// means the optional precheck recognized previously completed work. SuiteEvent
// requires profile-owned apply before acknowledgement. Verification sends no ACK.
type VerifiedDelivery struct {
	Action     *AuthorizedAction
	Completed  bool
	SuiteEvent *SuiteEvent
}

// SuiteEvent carries an evaluated profile-owned event and its finalized temporal
// facts. Common evaluation produces it without applying profile state. Its
// private request cannot be replaced with an unverified caller-supplied event.
type SuiteEvent struct {
	verifier *Verifier
	target   protocol.TargetAPI
	request  protocol.ApplyRequest
}

// Apply updates the selected profile's retained state using detached evaluated
// facts. Failure prevents acknowledgement; profiles own event idempotency.
func (e *SuiteEvent) Apply(ctx context.Context) error {
	if e == nil || e.verifier == nil || e.target == nil {
		return fmt.Errorf("%w: unevaluated suite event", protocol.ErrUnknownPredicateType)
	}
	e.verifier.mu.Lock()
	defer e.verifier.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.target.Apply(ctx, cloneApplyRequest(e.request))
}

// Verifier authenticates evidence and evaluates common predicates independently
// of ordinary target storage, application effects, and acknowledgements. It owns
// authenticated trust and the retained evidence-log checkpoint; installed native
// implementations own profile state. Verifier serializes verification state and
// is safe for concurrent use. Its mutex is not an application generation fence.
type Verifier struct {
	mu sync.Mutex

	config      Config
	trust       protocol.TrustConfiguration
	initialized bool
	lookup      protocol.TargetLookup
	retained    temporal.RetainedState

	staleCheckpointResponses uint64
}

// NewVerifier constructs an uninitialized common verifier with explicitly
// installed trusted provenance implementations. No application is required.
func NewVerifier(config Config, lookup protocol.TargetLookup) (*Verifier, error) {
	if config.Tenant.Scheme == "" || config.Tenant.Authority == "" || config.TargetID == "" {
		return nil, errors.New("tenant and target are required")
	}
	if lookup == nil {
		return nil, errors.New("installed verifier lookup is required")
	}
	return &Verifier{
		config:   config,
		lookup:   lookup,
		retained: temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()},
	}, nil
}

// Bootstrap installs initial authenticated trust. An initialized verifier
// never returns to TOFU.
func (v *Verifier) Bootstrap(trust protocol.TrustConfiguration) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.initialized {
		return protocol.ErrAlreadyInitialized
	}
	if err := trust.Validate(); err != nil {
		return err
	}
	v.trust = trust.Clone()
	v.initialized = true
	return nil
}

// Verify snapshots and bounds the package, prepares common temporal services,
// authenticates its root, validates common scope, optionally checks completed
// application progress, and evaluates all required premises before returning an
// action or suite event. It performs no ordinary application or profile-owned
// apply and does not acknowledge work. Verified checkpoint advancement remains
// independent of semantic acceptance. Inputs are detached at ingestion; outputs
// are owned by the caller. The precheck must not call back into this verifier.
// Unused supporting inclusions are structurally checked but not cryptographically
// verified. Each selected statement has independent source policy and finalized
// temporal facts; a suite event carries only its root's facts to profile apply.
// A stale manager checkpoint returns the newer retained checkpoint for proof
// reconstruction without another evidence registration or repeated effects.
func (v *Verifier) Verify(ctx context.Context, pkg protocol.DeliveryPackage, precheck GenerationPrecheck) (VerifiedDelivery, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return VerifiedDelivery{}, err
	}
	if !v.initialized {
		return VerifiedDelivery{}, protocol.ErrUninitializedVerifier
	}
	catalog, err := newEvidenceCatalog(pkg, v.lookup, defaultVerificationLimits())
	if err != nil {
		return VerifiedDelivery{}, err
	}
	root := catalog.item(catalog.rootID)
	prepared, err := temporal.Prepare(v.retained, catalog.update, root.Evidence, root.EvidenceLog)
	if err != nil {
		return VerifiedDelivery{}, v.mapLogError(err)
	}
	// The log observation is independent of apply. Pinning it here keeps an
	// inert rejected leaf in the accepted prefix so a later fork cannot omit it.
	// It grants no authority to entries and does not satisfy their log requirements.
	v.retained = prepared.NextState
	session := newVerificationSession(catalog, v.trust, protocol.TemporalVerificationServices{Log: prepared.Log})
	var result VerifiedDelivery
	// Source policy authenticates every assertion independently of its use.
	// Common semantics bind intent to this tenant and fulfillment relations to
	// the platform-wide provider reference provisioned on this verifier.
	err = session.withNode(ctx, catalog.rootID, func(root verifiedNode) error {
		var err error
		result, err = v.evaluateRoot(ctx, session, root, precheck)
		return err
	})
	if err != nil {
		return VerifiedDelivery{}, err
	}
	if err := ctx.Err(); err != nil {
		return VerifiedDelivery{}, err
	}
	return result, nil
}

func (v *Verifier) evaluateRoot(ctx context.Context, session *verificationSession, root verifiedNode, precheck GenerationPrecheck) (VerifiedDelivery, error) {
	result := root.result
	switch result.Authenticated.PredicateType {
	case protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1:
		var action AuthorizedAction
		var required bool
		var err error
		if result.Authenticated.PredicateType == protocol.PredicateTypeDeploymentV1 {
			action, required, err = v.handleDeployment(ctx, session, root, precheck)
		} else {
			action, required, err = v.handleManagedResource(ctx, session, root, precheck)
		}
		if err != nil {
			return VerifiedDelivery{}, err
		}
		if !required {
			return VerifiedDelivery{Completed: true}, nil
		}
		return VerifiedDelivery{Action: &action}, nil
	case protocol.PredicateTypeFulfillmentRelationV1:
		return VerifiedDelivery{}, fmt.Errorf("%w: fulfillment-relation/v1 is supporting evidence", protocol.ErrUnknownPredicateType)
	case protocol.PredicateTypeTrustConfigUpdateV1:
		return VerifiedDelivery{}, fmt.Errorf("%w: trust-config-update/v1 is not implemented", protocol.ErrUnknownPredicateType)
	default:
		target, ok := session.catalog.lookup(result.Authenticated.ProvenanceType)
		if !ok {
			return VerifiedDelivery{}, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, result.Authenticated.ProvenanceType)
		}
		if !target.Owns(result.Authenticated.PredicateType) {
			return VerifiedDelivery{}, fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, result.Authenticated.PredicateType)
		}
		return VerifiedDelivery{SuiteEvent: &SuiteEvent{verifier: v, target: target, request: cloneApplyRequest(protocol.ApplyRequest{
			Authenticated: result.Authenticated,
			Assertion:     result.Assertion,
			Statement:     root.statement,
			Temporal:      result.Temporal,
		})}}, nil
	}
}

// Checkpoint returns the retained append-only evidence-log checkpoint.
func (v *Verifier) Checkpoint() protocol.Checkpoint {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.retained.EvidenceLog
}

// StaleCheckpointResponses reports stale-checkpoint verification responses.
func (v *Verifier) StaleCheckpointResponses() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.staleCheckpointResponses
}

func (v *Verifier) mapLogError(err error) error {
	var stale *temporal.CheckpointStaleError
	if errors.As(err, &stale) {
		v.staleCheckpointResponses++
		return &CheckpointStaleError{checkpoint: stale.LatestCheckpoint(), cause: err}
	}
	return fmt.Errorf("%w: %w", ErrLogFork, err)
}
