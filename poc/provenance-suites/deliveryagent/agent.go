// Package deliveryagent models the target verifier. It begins from
// bootstrapped trust configuration, verifies provenance under matched
// delivery policy, applies authenticated content, and acknowledges only
// after it has retained enough state to retry safely.
package deliveryagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

var (
	ErrGeneration = errors.New("delivery generation is stale or conflicting")
	ErrLogFork    = errors.New("delivery does not extend retained checkpoint")

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
	TenantID protocol.TenantID
	TargetID string
}

// AppliedDelivery is the agent's retained view of an accepted delivery.
type AppliedDelivery struct {
	Scope         protocol.DeliveryScope
	PredicateType protocol.PredicateType
	Manifests     []protocol.TypedManifest
}

type appliedState struct {
	view   AppliedDelivery
	signed []byte
}

// Agent is the target role.
type Agent struct {
	mu sync.Mutex

	config      Config
	trust       protocol.TrustConfiguration
	initialized bool
	profile     *directkey.Target
	checkpoint  protocol.Checkpoint

	applied     map[string]appliedState
	generations map[string]uint64

	failBeforeAccepting      uint64
	loseNextAcknowledgement  bool
	staleCheckpointResponses uint64
	suiteApplyCount          uint64
}

// New constructs an uninitialized verifier.
func New(config Config) (*Agent, error) {
	if config.TenantID == "" || config.TargetID == "" {
		return nil, errors.New("tenant and target are required")
	}
	return &Agent{
		config:      config,
		profile:     directkey.NewTarget(),
		checkpoint:  protocol.EmptyCheckpoint(),
		applied:     make(map[string]appliedState),
		generations: make(map[string]uint64),
	}, nil
}

// Bootstrap installs the initial authenticated trust configuration.
// An initialized verifier never returns to TOFU.
func (a *Agent) Bootstrap(trust protocol.TrustConfiguration) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.initialized {
		return protocol.ErrAlreadyInitialized
	}
	if len(trust.AuthorityRegistry) == 0 {
		return errors.New("trust configuration has no authority registry")
	}
	a.trust = trust
	a.initialized = true
	return nil
}

// Deliver verifies the log update, then provenance under matched policy.
// Authenticated predicate type selects apply: intent predicates use
// fulfillment apply, trust-config-update is reserved on the agent, and
// predicates the selected profile Owns call TargetAPI.Apply. Unknown
// predicates fail closed. A verified log checkpoint is retained even when
// the included delivery is later rejected. When the manager constructed
// proofs from an obsolete checkpoint, Deliver returns the newer retained
// checkpoint so the manager can retry without applying again.
func (a *Agent) Deliver(pkg resourcemanager.DeliveryPackage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.initialized {
		return protocol.ErrUninitializedVerifier
	}
	if a.failBeforeAccepting > 0 {
		a.failBeforeAccepting--
		return ErrDeliveryUnavailable
	}

	retained := a.checkpoint
	if err := a.acceptLogLocked(pkg); err != nil {
		return err
	}
	if isStaleLogProof(pkg.Log, retained) {
		// RFC 6962 consistency from the empty tree is empty. A proof built from
		// that older cache can therefore verify as an equal-size no-op against
		// the retained head. Report the retained checkpoint so the manager can
		// reconstruct without applying the included delivery again.
		a.staleCheckpointResponses++
		return &CheckpointStaleError{
			checkpoint: retained,
			cause:      fmt.Errorf("constructed from checkpoint size %d", pkg.Log.From.Size),
		}
	}

	// The log observation is independent of apply. Pinning it here keeps an
	// inert rejected leaf in the accepted prefix so a later fork cannot omit it.
	a.checkpoint = pkg.Log.Checkpoint

	authenticated, assertion, err := protocol.SelectAndVerify(
		context.Background(),
		pkg.Root,
		protocol.DeliveryContext{
			ClaimedTenant:     a.config.TenantID,
			RootAuthorization: true,
		},
		a.trust,
		a.lookupLocked,
	)
	if err != nil {
		return err
	}

	if err := a.dispatchApplyLocked(pkg, authenticated, assertion); err != nil {
		return err
	}
	if a.loseNextAcknowledgement {
		a.loseNextAcknowledgement = false
		return ErrAcknowledgementLost
	}
	return nil
}

// Applied returns the last accepted delivery for a fulfillment.
func (a *Agent) Applied(fulfillmentID string) (AppliedDelivery, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.applied[fulfillmentID]
	if !ok {
		return AppliedDelivery{}, false
	}
	return cloneApplied(state.view), true
}

// Checkpoint is the retained append-only log position.
func (a *Agent) Checkpoint() protocol.Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.checkpoint
}

// StaleCheckpointResponses is the number of times Deliver returned
// CheckpointStaleError. It is test-observable transport metadata.
func (a *Agent) StaleCheckpointResponses() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.staleCheckpointResponses
}

// SuiteApplyCount is the number of times Deliver dispatched to TargetAPI.Apply.
// Intent, trust-config-update, and unowned predicates do not increment it.
func (a *Agent) SuiteApplyCount() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.suiteApplyCount
}

// PublicKey returns the retained direct-key mapping for tests.
func (a *Agent) PublicKey(principal protocol.Principal) ([]byte, bool) {
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

func (a *Agent) lookupLocked(pt protocol.ProvenanceType) (protocol.TargetAPI, bool) {
	if pt == protocol.ProvenanceTypeDirectKeyV1 {
		return a.profile, true
	}
	return nil, false
}

func (a *Agent) dispatchApplyLocked(pkg resourcemanager.DeliveryPackage, authenticated protocol.AuthenticatedEvidence, assertion protocol.TypedAssertion) error {
	switch authenticated.PredicateType {
	case protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1:
		view, err := a.decodeAndDeriveLocked(pkg, authenticated, assertion)
		if err != nil {
			return err
		}
		if view.Scope.TenantID != a.config.TenantID || view.Scope.TargetID != a.config.TargetID {
			return fmt.Errorf("%w: tenant or target mismatch", protocol.ErrPolicyReevaluation)
		}
		if authenticated.MappedFleetShiftTenant != a.config.TenantID {
			return fmt.Errorf("%w: mapped tenant %q, agent tenant %q", protocol.ErrTenantMismatch, authenticated.MappedFleetShiftTenant, a.config.TenantID)
		}
		return a.applyLocked(view, append([]byte(nil), assertion.Bytes...))
	case protocol.PredicateTypeTrustConfigUpdateV1:
		return fmt.Errorf("%w: trust-config-update/v1 is not implemented", protocol.ErrUnknownPredicateType)
	default:
		target, ok := a.lookupLocked(authenticated.ProvenanceType)
		if !ok {
			return fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, authenticated.ProvenanceType)
		}
		if !target.Owns(authenticated.PredicateType) {
			return fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, authenticated.PredicateType)
		}
		a.suiteApplyCount++
		return target.Apply(context.Background(), protocol.ApplyRequest{
			Authenticated: authenticated,
			Assertion:     assertion,
			Statement:     pkg.Root,
			Index:         pkg.Log.Index,
		})
	}
}

func (a *Agent) acceptLogLocked(pkg resourcemanager.DeliveryPackage) error {
	if err := protocol.VerifyLogUpdate(a.checkpoint, pkg.Log, pkg.Root.Evidence); err != nil {
		if isStaleLogProof(pkg.Log, a.checkpoint) {
			a.staleCheckpointResponses++
			return &CheckpointStaleError{checkpoint: a.checkpoint, cause: err}
		}
		return fmt.Errorf("%w: %v", ErrLogFork, err)
	}
	return nil
}

func isStaleLogProof(update protocol.LogUpdate, retained protocol.Checkpoint) bool {
	if update.From.Size >= retained.Size {
		return false
	}
	// A proof constructed from an older manager cache of the same branch either
	// already matches the retained head (lost acknowledgement) or extends past it.
	return update.Checkpoint == retained || update.Checkpoint.Size > retained.Size
}

func (a *Agent) decodeAndDeriveLocked(pkg resourcemanager.DeliveryPackage, authenticated protocol.AuthenticatedEvidence, assertion protocol.TypedAssertion) (AppliedDelivery, error) {
	switch authenticated.PredicateType {
	case protocol.PredicateTypeDeploymentV1:
		authorization, err := protocol.DecodeDeploymentAuthorization(assertion)
		if err != nil {
			return AppliedDelivery{}, err
		}
		for i, manifest := range authorization.Manifests {
			if manifest.MediaType == "" {
				return AppliedDelivery{}, fmt.Errorf("%w: manifest %d media type is required", protocol.ErrMalformedEvidence, i)
			}
		}
		return AppliedDelivery{
			Scope:         authorization.DeliveryScope,
			PredicateType: protocol.PredicateTypeDeploymentV1,
			Manifests:     cloneManifests(authorization.Manifests),
		}, nil
	case protocol.PredicateTypeManagedResourceV1:
		authorization, err := protocol.DecodeManagedResourceAuthorization(assertion)
		if err != nil {
			return AppliedDelivery{}, err
		}
		relation, err := a.verifyFulfillmentRelationLocked(pkg, authorization)
		if err != nil {
			return AppliedDelivery{}, err
		}
		// RegisteredSelfTarget: the named delivery target is the addon
		// itself. The caller already required DeliveryScope.TargetID to
		// equal this agent's ID before apply.
		return AppliedDelivery{
			Scope:         authorization.DeliveryScope,
			PredicateType: protocol.PredicateTypeManagedResourceV1,
			Manifests: []protocol.TypedManifest{{
				MediaType: relation.MediaType,
				Bytes:     append([]byte(nil), authorization.Spec...),
			}},
		}, nil
	default:
		return AppliedDelivery{}, fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, authenticated.PredicateType)
	}
}

func (a *Agent) verifyFulfillmentRelationLocked(pkg resourcemanager.DeliveryPackage, authorization protocol.ManagedResourceAuthorization) (protocol.FulfillmentRelation, error) {
	var found *protocol.SignedStatement
	for i := range pkg.Supporting {
		item := &pkg.Supporting[i]
		hints, err := a.profile.ParseHints(item.Evidence)
		if err != nil {
			return protocol.FulfillmentRelation{}, err
		}
		if hints.PredicateType != protocol.PredicateTypeFulfillmentRelationV1 {
			continue
		}
		if found != nil {
			return protocol.FulfillmentRelation{}, fmt.Errorf("%w: multiple fulfillment relations", protocol.ErrAmbiguousPolicy)
		}
		found = item
	}
	if found == nil {
		return protocol.FulfillmentRelation{}, ErrFulfillmentRelationRequired
	}

	authenticated, assertion, err := protocol.SelectAndVerify(
		context.Background(),
		*found,
		protocol.DeliveryContext{
			ClaimedTenant:     a.config.TenantID,
			RootAuthorization: false,
		},
		a.trust,
		a.lookupLocked,
	)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}
	if authenticated.PredicateType != protocol.PredicateTypeFulfillmentRelationV1 {
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, authenticated.PredicateType)
	}
	relation, err := protocol.DecodeFulfillmentRelation(assertion)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}
	if relation.MediaType == "" {
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: fulfillment relation media type is required", protocol.ErrMalformedEvidence)
	}
	if relation.ResourceType != authorization.ResourceType {
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: fulfillment relation resource type %q, authorization %q", protocol.ErrPolicyReevaluation, relation.ResourceType, authorization.ResourceType)
	}
	return relation, nil
}

func (a *Agent) applyLocked(view AppliedDelivery, signed []byte) error {
	if view.Scope.Action != protocol.ActionPut && view.Scope.Action != protocol.ActionRemove {
		return fmt.Errorf("unsupported action %q", view.Scope.Action)
	}
	previous, exists := a.generations[view.Scope.FulfillmentID]
	if exists {
		if view.Scope.Generation < previous {
			return fmt.Errorf("%w: generation %d is older than %d", ErrGeneration, view.Scope.Generation, previous)
		}
		if view.Scope.Generation == previous {
			applied := a.applied[view.Scope.FulfillmentID]
			if bytes.Equal(applied.signed, signed) {
				return nil
			}
			return fmt.Errorf("%w: generation %d has different signed content", ErrGeneration, view.Scope.Generation)
		}
	}
	a.generations[view.Scope.FulfillmentID] = view.Scope.Generation
	if view.Scope.Action == protocol.ActionRemove {
		delete(a.applied, view.Scope.FulfillmentID)
		return nil
	}
	a.applied[view.Scope.FulfillmentID] = appliedState{view: cloneApplied(view), signed: signed}
	return nil
}

func cloneApplied(in AppliedDelivery) AppliedDelivery {
	out := in
	out.Manifests = cloneManifests(in.Manifests)
	return out
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
