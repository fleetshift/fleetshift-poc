// Package deliveryagent models the target verifier. It begins from
// bootstrapped trust configuration, verifies provenance under matched
// delivery policy, applies authenticated content, and acknowledges only
// after it has retained enough state to retry safely.
package deliveryagent

import (
	"context"
	"encoding/json"
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
	ErrDeliveryUnavailable = errors.New("delivery did not reach the agent")
)

// Config provisions one delivery agent.
type Config struct {
	TenantID protocol.TenantID
	TargetID string
}

// Agent is the target role.
type Agent struct {
	mu sync.Mutex

	config      Config
	trust       protocol.TrustConfiguration
	initialized bool
	profile     *directkey.Target
	checkpoint  protocol.Checkpoint

	applied     map[string]protocol.DeliveryAuthorization
	generations map[string]uint64

	failBeforeAccepting     uint64
	loseNextAcknowledgement bool
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
		applied:     make(map[string]protocol.DeliveryAuthorization),
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

// AcceptEnrollment is the typed direct-key lifecycle courier path. It is
// not ordinary delivery-authorization content.
func (a *Agent) AcceptEnrollment(evidence protocol.TypedEvidence) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.initialized {
		return protocol.ErrUninitializedVerifier
	}
	hints, err := a.profile.ParseHints(evidence)
	if err != nil {
		return err
	}
	authority, err := a.trust.Authority(protocol.PrincipalAuthority{Scheme: hints.Scheme, Authority: hints.Authority})
	if err != nil {
		return err
	}
	return a.profile.AcceptEnrollment(evidence, authority)
}

// Deliver verifies provenance under matched policy and applies the
// authenticated delivery authorization.
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

	if err := a.acceptLogLocked(pkg.Commitment); err != nil {
		return err
	}

	authenticated, err := protocol.SelectAndVerify(
		context.Background(),
		pkg.Evidence,
		pkg.Support,
		protocol.DeliveryContext{
			ClaimedTenant:     a.config.TenantID,
			ContentType:       pkg.Assertion.ContentType,
			RootAuthorization: true,
		},
		a.trust,
		a.lookupLocked,
	)
	if err != nil {
		return err
	}
	if err := bindAssertion(pkg.Assertion, authenticated); err != nil {
		return err
	}
	authorization, err := decodeAuthorization(pkg.Assertion)
	if err != nil {
		return err
	}
	if authorization.TenantID != a.config.TenantID || authorization.TargetID != a.config.TargetID {
		return fmt.Errorf("%w: tenant or target mismatch", protocol.ErrPolicyReevaluation)
	}
	if authenticated.MappedFleetShiftTenant != a.config.TenantID {
		return fmt.Errorf("%w: mapped tenant %q, agent tenant %q", protocol.ErrTenantMismatch, authenticated.MappedFleetShiftTenant, a.config.TenantID)
	}
	if err := a.applyLocked(authorization); err != nil {
		return err
	}
	a.checkpoint.Size = pkg.Commitment.Index + 1
	if a.loseNextAcknowledgement {
		a.loseNextAcknowledgement = false
		return ErrAcknowledgementLost
	}
	return nil
}

// Applied returns the last accepted authorization for a fulfillment.
func (a *Agent) Applied(fulfillmentID string) (protocol.DeliveryAuthorization, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	authorization, ok := a.applied[fulfillmentID]
	return authorization, ok
}

// Checkpoint is the retained append-only log position.
func (a *Agent) Checkpoint() protocol.Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.checkpoint
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

func (a *Agent) acceptLogLocked(commitment protocol.DeliveryCommitment) error {
	switch {
	case commitment.Index == a.checkpoint.Size:
		return nil
	case commitment.Index+1 == a.checkpoint.Size:
		return nil
	case commitment.Index < a.checkpoint.Size:
		return fmt.Errorf("%w: commitment index %d is before retained size %d", ErrLogFork, commitment.Index, a.checkpoint.Size)
	default:
		return fmt.Errorf("%w: commitment index %d is beyond retained size %d", ErrLogFork, commitment.Index, a.checkpoint.Size)
	}
}

func (a *Agent) applyLocked(authorization protocol.DeliveryAuthorization) error {
	if authorization.Action != protocol.ActionPut && authorization.Action != protocol.ActionRemove {
		return fmt.Errorf("unsupported action %q", authorization.Action)
	}
	previous, exists := a.generations[authorization.FulfillmentID]
	if exists {
		if authorization.Generation < previous {
			return fmt.Errorf("%w: generation %d is older than %d", ErrGeneration, authorization.Generation, previous)
		}
		if authorization.Generation == previous {
			applied := a.applied[authorization.FulfillmentID]
			if sameAuthorization(applied, authorization) {
				return nil
			}
			return fmt.Errorf("%w: generation %d has different signed content", ErrGeneration, authorization.Generation)
		}
	}
	a.generations[authorization.FulfillmentID] = authorization.Generation
	if authorization.Action == protocol.ActionRemove {
		delete(a.applied, authorization.FulfillmentID)
		return nil
	}
	a.applied[authorization.FulfillmentID] = authorization
	return nil
}

func bindAssertion(assertion protocol.TypedAssertion, authenticated protocol.AuthenticatedEvidence) error {
	if assertion.ContentType != authenticated.ContentType {
		return fmt.Errorf("%w: assertion content type %s, authenticated %s", protocol.ErrPolicyReevaluation, assertion.ContentType, authenticated.ContentType)
	}
	digest, err := assertion.Digest()
	if err != nil {
		return err
	}
	if digest != authenticated.ContentDigest {
		return fmt.Errorf("%w: supplied assertion does not match authenticated content digest", protocol.ErrVerificationFailed)
	}
	return nil
}

func decodeAuthorization(assertion protocol.TypedAssertion) (protocol.DeliveryAuthorization, error) {
	if assertion.ContentType != protocol.ContentTypeDeliveryAuthorizationV1 {
		return protocol.DeliveryAuthorization{}, fmt.Errorf("unsupported assertion content type %s", assertion.ContentType)
	}
	var authorization protocol.DeliveryAuthorization
	if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
		return protocol.DeliveryAuthorization{}, fmt.Errorf("decode delivery authorization: %w", err)
	}
	return authorization, nil
}

func sameAuthorization(a, b protocol.DeliveryAuthorization) bool {
	left, err := protocol.MarshalCanonical(a)
	if err != nil {
		return false
	}
	right, err := protocol.MarshalCanonical(b)
	if err != nil {
		return false
	}
	return string(left) == string(right)
}
