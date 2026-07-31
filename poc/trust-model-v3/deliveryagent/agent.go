// Package deliveryagent models the verifier and enforcement point attached to
// a target. It retains local checkpoints and independently validates OIDC key
// enrollment, continuity transitions, content signatures, and delivery order.
package deliveryagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
)

var (
	ErrEnrollment   = errors.New("enrollment verification failed")
	ErrRotation     = errors.New("rotation verification failed")
	ErrSignature    = errors.New("delivery signature verification failed")
	ErrAttestation  = errors.New("content attestation verification failed")
	ErrSigningState = errors.New("signing state is not valid at delivery position")
	ErrTrustLogFork = errors.New("trust update does not extend retained checkpoint")
	ErrLogFork      = errors.New("delivery does not extend retained checkpoint")
	ErrGeneration   = errors.New("delivery generation is stale or conflicting")
)

type Config struct {
	TenantID     string
	TargetID     string
	OIDCIssuer   string
	OIDCClientID string
	HTTPClient   *http.Client
}

type Agent struct {
	mu sync.Mutex

	config Config

	trustCheckpoint    protocol.Checkpoint
	deliveryCheckpoint protocol.Checkpoint
	deliveryRoots      map[uint64]string
	states             map[string]protocol.ContinuityState
	currentStates      map[string]string
	validFrom          map[string]uint64
	validBefore        map[string]uint64
	applied            map[string]protocol.SignedDelivery
	appliedDigests     map[string]string
	generations        map[string]uint64
}

func New(config Config) (*Agent, error) {
	if config.TenantID == "" || config.TargetID == "" || config.OIDCIssuer == "" || config.OIDCClientID == "" {
		return nil, errors.New("tenant, target, OIDC issuer, and OIDC client ID are required")
	}
	empty := protocol.EmptyCheckpoint()
	return &Agent{
		config:             config,
		trustCheckpoint:    empty,
		deliveryCheckpoint: empty,
		deliveryRoots:      map[uint64]string{0: empty.Root},
		states:             make(map[string]protocol.ContinuityState),
		currentStates:      make(map[string]string),
		validFrom:          make(map[string]uint64),
		validBefore:        make(map[string]uint64),
		applied:            make(map[string]protocol.SignedDelivery),
		appliedDigests:     make(map[string]string),
		generations:        make(map[string]uint64),
	}, nil
}

func (a *Agent) TrustCheckpoint() protocol.Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.trustCheckpoint
}

func (a *Agent) DeliveryCheckpoint() protocol.Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.deliveryCheckpoint
}

// SyncTrust validates a batch transactionally. Production can replace this
// copy-on-write step with a durable transactional or ordered-write protocol.
func (a *Agent) SyncTrust(ctx context.Context, records []protocol.TrustRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	checkpoint := a.trustCheckpoint
	states := cloneStates(a.states)
	currentStates := cloneStringMap(a.currentStates)
	validFrom := cloneUintMap(a.validFrom)
	validBefore := cloneUintMap(a.validBefore)

	for _, record := range records {
		nextCheckpoint, err := protocol.VerifyTrustRecord(checkpoint, record)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrTrustLogFork, err)
		}
		switch record.Event.Kind {
		case protocol.TrustEventEnrollment:
			if record.Event.Enrollment == nil || record.Event.Rotation != nil {
				return fmt.Errorf("%w: malformed enrollment event", ErrEnrollment)
			}
			state, digest, err := a.verifyEnrollment(ctx, *record.Event.Enrollment, currentStates)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrEnrollment, err)
			}
			states[digest] = state
			currentStates[state.IdentityID] = digest
			validFrom[digest] = 0
		case protocol.TrustEventRotation:
			if record.Event.Rotation == nil || record.Event.Enrollment != nil {
				return fmt.Errorf("%w: malformed rotation event", ErrRotation)
			}
			oldDigest, newState, newDigest, cutoff, err := a.verifyRotation(*record.Event.Rotation, states, currentStates)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrRotation, err)
			}
			states[newDigest] = newState
			currentStates[newState.IdentityID] = newDigest
			validBefore[oldDigest] = cutoff
			validFrom[newDigest] = cutoff
		default:
			return fmt.Errorf("%w: unknown trust event kind %q", ErrRotation, record.Event.Kind)
		}
		checkpoint = nextCheckpoint
	}

	a.trustCheckpoint = checkpoint
	a.states = states
	a.currentStates = currentStates
	a.validFrom = validFrom
	a.validBefore = validBefore
	return nil
}

func (a *Agent) verifyEnrollment(ctx context.Context, enrollment protocol.EnrollmentPackage, currentStates map[string]string) (protocol.ContinuityState, string, error) {
	intent := enrollment.Intent
	if intent.Protocol != protocol.EnrollmentProtocol || intent.TenantID != a.config.TenantID {
		return protocol.ContinuityState{}, "", errors.New("enrollment protocol or tenant mismatch")
	}
	if intent.ExpectedIssuer != a.config.OIDCIssuer || intent.EnrollmentClientID != a.config.OIDCClientID {
		return protocol.ContinuityState{}, "", errors.New("enrollment issuer or client does not match provisioned tenant configuration")
	}
	if protocol.DigestBytes(enrollment.ContinuityPublicKey) != intent.ContinuityKeyDigest {
		return protocol.ContinuityState{}, "", errors.New("continuity public key does not match nonce-bound digest")
	}
	if err := protocol.Verify(enrollment.ContinuityPublicKey, "enrollment-proof-of-possession/v1", intent, enrollment.ProofOfPossession); err != nil {
		return protocol.ContinuityState{}, "", fmt.Errorf("continuity-key proof of possession: %w", err)
	}
	nonce, err := protocol.EnrollmentNonce(intent)
	if err != nil {
		return protocol.ContinuityState{}, "", err
	}
	identity, err := verifyEnrollmentIDToken(ctx, a.config, nonce, enrollment.IDToken)
	if err != nil {
		return protocol.ContinuityState{}, "", err
	}
	identityID := protocol.IdentityID(a.config.TenantID, identity.Issuer, identity.Subject)
	if _, exists := currentStates[identityID]; exists {
		return protocol.ContinuityState{}, "", errors.New("identity is already enrolled")
	}
	state := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            a.config.TenantID,
		IdentityID:          identityID,
		Generation:          0,
		ContinuityPublicKey: append([]byte(nil), enrollment.ContinuityPublicKey...),
	}
	digest, err := state.Digest()
	if err != nil {
		return protocol.ContinuityState{}, "", err
	}
	return state, digest, nil
}

func (a *Agent) verifyRotation(rotation protocol.RotationPackage, states map[string]protocol.ContinuityState, currentStates map[string]string) (string, protocol.ContinuityState, string, uint64, error) {
	intent := rotation.Intent
	if intent.Protocol != protocol.RotationProtocol || intent.TenantID != a.config.TenantID {
		return "", protocol.ContinuityState{}, "", 0, errors.New("rotation protocol or tenant mismatch")
	}
	currentDigest, ok := currentStates[intent.IdentityID]
	if !ok || currentDigest != intent.PreviousStateDigest {
		return "", protocol.ContinuityState{}, "", 0, errors.New("rotation does not continue current identity state")
	}
	currentState, ok := states[currentDigest]
	if !ok {
		return "", protocol.ContinuityState{}, "", 0, errors.New("current continuity state is unavailable")
	}
	if intent.NewGeneration != currentState.Generation+1 {
		return "", protocol.ContinuityState{}, "", 0, errors.New("rotation generation is not the next generation")
	}
	if protocol.DigestBytes(rotation.NewContinuityPublicKey) != intent.NewContinuityKeyDigest {
		return "", protocol.ContinuityState{}, "", 0, errors.New("successor key does not match signed digest")
	}
	if err := protocol.Verify(currentState.ContinuityPublicKey, "continuity-rotation-old-key/v1", intent, rotation.SignatureByOldKey); err != nil {
		return "", protocol.ContinuityState{}, "", 0, fmt.Errorf("old key did not authorize rotation: %w", err)
	}
	if err := protocol.Verify(rotation.NewContinuityPublicKey, "continuity-rotation-new-key/v1", intent, rotation.ProofByNewKey); err != nil {
		return "", protocol.ContinuityState{}, "", 0, fmt.Errorf("new key did not prove possession: %w", err)
	}
	root, ok := a.deliveryRoots[intent.DeliveryCutoff.Size]
	if !ok || root != intent.DeliveryCutoff.Root {
		return "", protocol.ContinuityState{}, "", 0, errors.New("rotation cutoff is not in the agent's accepted delivery history")
	}
	newState := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            a.config.TenantID,
		IdentityID:          intent.IdentityID,
		Generation:          intent.NewGeneration,
		ContinuityPublicKey: append([]byte(nil), rotation.NewContinuityPublicKey...),
		PreviousStateDigest: currentDigest,
	}
	newDigest, err := newState.Digest()
	if err != nil {
		return "", protocol.ContinuityState{}, "", 0, err
	}
	return currentDigest, newState, newDigest, intent.DeliveryCutoff.Size, nil
}

// ReceiveDelivery first pins structurally valid log growth, then evaluates the
// content semantically. A bad signature can be skipped without permitting the
// resource manager to rewrite the record that the agent already observed.
func (a *Agent) ReceiveDelivery(record protocol.DeliveryRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	nextCheckpoint, err := protocol.VerifyDeliveryRecord(a.deliveryCheckpoint, record)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrLogFork, err)
	}
	a.deliveryCheckpoint = nextCheckpoint
	a.deliveryRoots[nextCheckpoint.Size] = nextCheckpoint.Root

	delivery := record.Delivery
	attestation := delivery.Attestation
	if attestation.Protocol != protocol.DeliveryProtocol || attestation.TenantID != a.config.TenantID || attestation.TargetID != a.config.TargetID {
		return fmt.Errorf("%w: protocol, tenant, or target mismatch", ErrAttestation)
	}
	if attestation.Action != protocol.ActionPut && attestation.Action != protocol.ActionRemove {
		return fmt.Errorf("%w: unsupported action %q", ErrAttestation, attestation.Action)
	}
	if protocol.DigestBytes(delivery.Content) != attestation.ContentDigest {
		return fmt.Errorf("%w: delivered content does not match signed digest", ErrAttestation)
	}
	state, ok := a.states[attestation.SigningStateDigest]
	if !ok || state.IdentityID != attestation.IdentityID {
		return fmt.Errorf("%w: unknown state for signing identity", ErrSigningState)
	}
	if record.Index < a.validFrom[attestation.SigningStateDigest] {
		return fmt.Errorf("%w: state is not yet valid", ErrSigningState)
	}
	if validBefore, retired := a.validBefore[attestation.SigningStateDigest]; retired && record.Index >= validBefore {
		return fmt.Errorf("%w: state retired at delivery index %d", ErrSigningState, validBefore)
	}
	if err := protocol.Verify(state.ContinuityPublicKey, "content-delivery/v1", attestation, delivery.Signature); err != nil {
		return fmt.Errorf("%w: %v", ErrSignature, err)
	}

	digest, err := protocol.ObjectDigest(delivery)
	if err != nil {
		return fmt.Errorf("%w: digest delivery: %v", ErrAttestation, err)
	}
	if previousGeneration, exists := a.generations[attestation.FulfillmentID]; exists {
		if attestation.Generation < previousGeneration {
			return fmt.Errorf("%w: generation %d is older than %d", ErrGeneration, attestation.Generation, previousGeneration)
		}
		if attestation.Generation == previousGeneration {
			if a.appliedDigests[attestation.FulfillmentID] == digest {
				return nil
			}
			return fmt.Errorf("%w: generation %d has different signed content", ErrGeneration, attestation.Generation)
		}
	}
	a.generations[attestation.FulfillmentID] = attestation.Generation
	a.appliedDigests[attestation.FulfillmentID] = digest
	if attestation.Action == protocol.ActionRemove {
		delete(a.applied, attestation.FulfillmentID)
	} else {
		a.applied[attestation.FulfillmentID] = cloneDelivery(delivery)
	}
	return nil
}

func (a *Agent) Applied(fulfillmentID string) (protocol.SignedDelivery, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delivery, ok := a.applied[fulfillmentID]
	return cloneDelivery(delivery), ok
}

func cloneStates(in map[string]protocol.ContinuityState) map[string]protocol.ContinuityState {
	out := make(map[string]protocol.ContinuityState, len(in))
	for key, value := range in {
		value.ContinuityPublicKey = append([]byte(nil), value.ContinuityPublicKey...)
		out[key] = value
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneUintMap(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneDelivery(in protocol.SignedDelivery) protocol.SignedDelivery {
	out := in
	out.Content = append([]byte(nil), in.Content...)
	out.Signature = append([]byte(nil), in.Signature...)
	return out
}
