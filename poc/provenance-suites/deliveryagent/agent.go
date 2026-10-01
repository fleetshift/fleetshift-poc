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
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/temporal"
)

var (
	ErrGeneration = errors.New("delivery generation is stale or conflicting")
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
	retained    temporal.RetainedState

	applied     map[protocol.FullResourceName]appliedState
	generations map[protocol.FullResourceName]uint64

	failBeforeAccepting      uint64
	loseNextAcknowledgement  bool
	staleCheckpointResponses uint64
	suiteApplyCount          uint64
}

// New constructs an uninitialized verifier.
func New(config Config) (*Agent, error) {
	if config.Tenant.Scheme == "" || config.Tenant.Authority == "" || config.TargetID == "" {
		return nil, errors.New("tenant and target are required")
	}
	return &Agent{
		config:      config,
		profile:     directkey.NewTarget(),
		retained:    temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()},
		applied:     make(map[protocol.FullResourceName]appliedState),
		generations: make(map[protocol.FullResourceName]uint64),
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
	if err := trust.Validate(); err != nil {
		return err
	}
	a.trust = trust.Clone()
	a.initialized = true
	return nil
}

// Deliver first validates and snapshots package structure, then verifies
// any supplied package-wide evidence-log consistency and root inclusion through
// temporal.Prepare. A per-delivery verificationSession selects and verifies
// the root and any semantically used supporting items. Unused supporting-item
// inclusions are structurally checked but not cryptographically verified.
// ApplyRequest.Temporal receives the root VerificationResult only.
// Authenticated predicate type selects apply: intent predicates use
// fulfillment apply, trust-config-update is reserved on the agent, and
// predicates the selected profile Owns call TargetAPI.Apply. Unknown
// predicates fail closed. A verified evidence-log checkpoint is retained
// even when the included evidence is later rejected. When the manager
// constructed proofs from an obsolete checkpoint, Deliver returns the
// newer retained checkpoint so the manager can retry without applying
// again.
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

	catalog, err := newEvidenceCatalog(pkg, a.lookupLocked, defaultVerificationLimits())
	if err != nil {
		return err
	}
	root := catalog.item(catalog.rootID)
	prepared, err := temporal.Prepare(a.retained, catalog.update, root.Evidence, root.EvidenceLog)
	if err != nil {
		return a.mapLogError(err)
	}

	// The log observation is independent of apply. Pinning it here keeps an
	// inert rejected leaf in the accepted prefix so a later fork cannot omit it.
	// This POC retains the observed prefix before per-statement policy admission;
	// it grants no authority to entries and does not satisfy their log requirements.
	a.retained = prepared.NextState

	services := protocol.TemporalVerificationServices{Log: prepared.Log}
	session := newVerificationSession(catalog, a.trust, services)
	// Source policy authenticates every assertion independently of its use.
	// Common semantics bind intent to this tenant and fulfillment relations to
	// the platform-wide provider reference provisioned on this agent.
	if err := session.withNode(
		context.Background(),
		catalog.rootID,
		func(root verifiedNode) error {
			return a.dispatchApplyLocked(session, root)
		},
	); err != nil {
		return err
	}
	if a.loseNextAcknowledgement {
		a.loseNextAcknowledgement = false
		return ErrAcknowledgementLost
	}
	return nil
}

// Applied returns the last accepted delivery for the named resource.
func (a *Agent) Applied(name protocol.FullResourceName) (AppliedDelivery, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.applied[name]
	if !ok {
		return AppliedDelivery{}, false
	}
	return cloneApplied(state.view), true
}

// Checkpoint is the retained append-only evidence-log position.
func (a *Agent) Checkpoint() protocol.Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.retained.EvidenceLog
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

func (a *Agent) dispatchApplyLocked(session *verificationSession, root verifiedNode) error {
	result := root.result
	switch result.Authenticated.PredicateType {
	case protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1:
		view, err := a.decodeAndDeriveLocked(session, root)
		if err != nil {
			return err
		}
		if view.Scope.Tenant != a.config.Tenant || view.Scope.TargetID != a.config.TargetID {
			return fmt.Errorf("%w: tenant or target mismatch", protocol.ErrPolicyReevaluation)
		}
		if result.Authenticated.Principal.Tenant() != a.config.Tenant {
			return fmt.Errorf("%w: root principal does not belong to the provisioned resource tenant", protocol.ErrTenantMismatch)
		}
		if _, err := session.validateSelectedGraph(root.identity); err != nil {
			return err
		}
		return a.applyLocked(view, result.Assertion.Bytes)
	case protocol.PredicateTypeTrustConfigUpdateV1:
		return fmt.Errorf("%w: trust-config-update/v1 is not implemented", protocol.ErrUnknownPredicateType)
	default:
		target, ok := session.catalog.lookup(result.Authenticated.ProvenanceType)
		if !ok {
			return fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, result.Authenticated.ProvenanceType)
		}
		if !target.Owns(result.Authenticated.PredicateType) {
			return fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, result.Authenticated.PredicateType)
		}
		if _, err := session.validateSelectedGraph(root.identity); err != nil {
			return err
		}
		a.suiteApplyCount++
		return target.Apply(context.Background(), cloneApplyRequest(protocol.ApplyRequest{
			Authenticated: result.Authenticated,
			Assertion:     result.Assertion,
			Statement:     root.statement,
			Temporal:      result.Temporal,
		}))
	}
}

func (a *Agent) mapLogError(err error) error {
	var stale *temporal.CheckpointStaleError
	if errors.As(err, &stale) {
		a.staleCheckpointResponses++
		return &CheckpointStaleError{
			checkpoint: stale.LatestCheckpoint(),
			cause:      err,
		}
	}
	return fmt.Errorf("%w: %w", ErrLogFork, err)
}

func (a *Agent) decodeAndDeriveLocked(session *verificationSession, root verifiedNode) (AppliedDelivery, error) {
	result := root.result
	switch result.Authenticated.PredicateType {
	case protocol.PredicateTypeDeploymentV1:
		authorization, err := protocol.DecodeDeploymentAuthorization(result.Assertion)
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
			Manifests:     authorization.Manifests,
		}, nil
	case protocol.PredicateTypeManagedResourceV1:
		authorization, err := protocol.DecodeManagedResourceAuthorization(result.Assertion)
		if err != nil {
			return AppliedDelivery{}, err
		}
		relation, err := a.verifyFulfillmentRelationLocked(session, root.identity, authorization)
		if err != nil {
			return AppliedDelivery{}, err
		}
		// RegisteredSelfTarget: the named delivery target is the addon
		// itself. TargetID is this POC's static-placement stand-in; the
		// caller already required it to equal this agent's ID before apply.
		return AppliedDelivery{
			Scope:         authorization.DeliveryScope,
			PredicateType: protocol.PredicateTypeManagedResourceV1,
			Manifests: []protocol.TypedManifest{{
				MediaType: relation.MediaType,
				Bytes:     authorization.Spec,
			}},
		}, nil
	default:
		return AppliedDelivery{}, fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, result.Authenticated.PredicateType)
	}
}

func (a *Agent) verifyFulfillmentRelationLocked(session *verificationSession, parent protocol.Digest, authorization protocol.ManagedResourceAuthorization) (protocol.FulfillmentRelation, error) {
	if a.config.ProviderTenant == (protocol.Tenant{}) {
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: provider tenant is not provisioned", protocol.ErrTenantMismatch)
	}
	candidates, err := session.supportingCandidates(protocol.PredicateTypeFulfillmentRelationV1)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}
	switch len(candidates) {
	case 0:
		return protocol.FulfillmentRelation{}, ErrFulfillmentRelationRequired
	case 1:
	default:
		return protocol.FulfillmentRelation{}, fmt.Errorf("%w: multiple fulfillment relations", protocol.ErrAmbiguousRelation)
	}

	var relation protocol.FulfillmentRelation
	err = session.withNode(
		context.Background(),
		candidates[0],
		func(support verifiedNode) error {
			if support.result.Authenticated.PredicateType != protocol.PredicateTypeFulfillmentRelationV1 {
				return fmt.Errorf("%w: %s", protocol.ErrUnknownPredicateType, support.result.Authenticated.PredicateType)
			}
			if support.result.Authenticated.Principal.Tenant() != a.config.ProviderTenant {
				return fmt.Errorf("%w: fulfillment relation principal does not belong to the platform provider tenant", protocol.ErrTenantMismatch)
			}
			decoded, err := protocol.DecodeFulfillmentRelation(support.result.Assertion)
			if err != nil {
				return err
			}
			if decoded.MediaType == "" {
				return fmt.Errorf("%w: fulfillment relation media type is required", protocol.ErrMalformedEvidence)
			}
			if decoded.ResourceType != authorization.ResourceType {
				return fmt.Errorf("%w: fulfillment relation resource type %q, authorization %q", protocol.ErrPolicyReevaluation, decoded.ResourceType, authorization.ResourceType)
			}
			if err := session.recordDependency(parent, support.identity); err != nil {
				return err
			}
			relation = decoded
			return nil
		},
	)
	if err != nil {
		return protocol.FulfillmentRelation{}, err
	}
	return relation, nil
}

func (a *Agent) applyLocked(view AppliedDelivery, signed []byte) error {
	if view.Scope.Action != protocol.ActionPut && view.Scope.Action != protocol.ActionRemove {
		return fmt.Errorf("unsupported action %q", view.Scope.Action)
	}
	previous, exists := a.generations[view.Scope.FullResourceName]
	if exists {
		if view.Scope.Generation < previous {
			return fmt.Errorf("%w: generation %d is older than %d", ErrGeneration, view.Scope.Generation, previous)
		}
		if view.Scope.Generation == previous {
			applied := a.applied[view.Scope.FullResourceName]
			if bytes.Equal(applied.signed, signed) {
				return nil
			}
			return fmt.Errorf("%w: generation %d has different signed content", ErrGeneration, view.Scope.Generation)
		}
	}
	a.generations[view.Scope.FullResourceName] = view.Scope.Generation
	if view.Scope.Action == protocol.ActionRemove {
		delete(a.applied, view.Scope.FullResourceName)
		return nil
	}
	// Common derivation already owns immutable delivery data; retaining it
	// shares those values. Applied detaches data for callers outside the agent.
	a.applied[view.Scope.FullResourceName] = appliedState{view: view, signed: signed}
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
