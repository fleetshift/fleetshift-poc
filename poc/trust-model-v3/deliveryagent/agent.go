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
	ErrMapFork      = errors.New("authenticated map update does not extend retained root")
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

type validityBoundary struct {
	Marker         protocol.DeliveryLogReference
	RotationDigest string
}

type observedLogRecord struct {
	Hash           string
	Kind           string
	RotationDigest string
}

type Agent struct {
	mu sync.Mutex

	config Config

	mapRoot            string
	deliveryCheckpoint protocol.Checkpoint
	deliveryRecords    map[uint64]observedLogRecord
	historyHeads       map[string]protocol.KeyHistoryHead
	states             map[string]protocol.ContinuityState
	validFrom          map[string]validityBoundary
	validBefore        map[string]validityBoundary
	lastMarkers        map[string]protocol.DeliveryLogReference
	applied            map[string]protocol.SignedDelivery
	appliedDigests     map[string]string
	generations        map[string]uint64
}

func New(config Config) (*Agent, error) {
	if config.TenantID == "" || config.TargetID == "" || config.OIDCIssuer == "" || config.OIDCClientID == "" {
		return nil, errors.New("tenant, target, OIDC issuer, and OIDC client ID are required")
	}
	empty := protocol.EmptyCheckpoint()
	emptyMapRoot, err := protocol.KeyHistoryMapRoot(config.TenantID, nil)
	if err != nil {
		return nil, fmt.Errorf("initialize authenticated map: %w", err)
	}
	return &Agent{
		config:             config,
		mapRoot:            emptyMapRoot,
		deliveryCheckpoint: empty,
		deliveryRecords:    make(map[uint64]observedLogRecord),
		historyHeads:       make(map[string]protocol.KeyHistoryHead),
		states:             make(map[string]protocol.ContinuityState),
		validFrom:          make(map[string]validityBoundary),
		validBefore:        make(map[string]validityBoundary),
		lastMarkers:        make(map[string]protocol.DeliveryLogReference),
		applied:            make(map[string]protocol.SignedDelivery),
		appliedDigests:     make(map[string]string),
		generations:        make(map[string]uint64),
	}, nil
}

func (a *Agent) MapRoot() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mapRoot
}

func (a *Agent) DeliveryCheckpoint() protocol.Checkpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.deliveryCheckpoint
}

func (a *Agent) HistoryHead(identityID string) (protocol.KeyHistoryHead, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	head, ok := a.historyHeads[identityID]
	return head, ok
}

// SyncMap validates a batch transactionally. Each update proves an old leaf
// (or its absence) against the retained sparse-map root, then reuses the same
// sibling path to authenticate a successor root with only that leaf replaced.
func (a *Agent) SyncMap(ctx context.Context, updates []protocol.AuthenticatedMapUpdate) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	mapRoot := a.mapRoot
	historyHeads := cloneHeads(a.historyHeads)
	states := cloneStates(a.states)
	validFrom := cloneBoundaries(a.validFrom)
	validBefore := cloneBoundaries(a.validBefore)
	lastMarkers := cloneMarkers(a.lastMarkers)

	for _, mapUpdate := range updates {
		if mapUpdate.PreviousRoot != mapRoot {
			return fmt.Errorf("%w: update starts at root %q, want %q", ErrMapFork, mapUpdate.PreviousRoot, mapRoot)
		}
		update := mapUpdate.KeyHistory
		identityID := update.Event.IdentityID
		previousHead, exists := historyHeads[identityID]
		if exists {
			if mapUpdate.PreviousHead == nil || *mapUpdate.PreviousHead != previousHead {
				return fmt.Errorf("%w: previous leaf differs from retained key-history head", ErrMapFork)
			}
		} else {
			if mapUpdate.PreviousHead != nil {
				return fmt.Errorf("%w: map proof contains an unexpected previous leaf", ErrMapFork)
			}
			previousHead = protocol.EmptyKeyHistoryHead(identityID)
		}
		if update.PreviousHead != previousHead {
			return fmt.Errorf("%w: key history does not start at retained head", ErrMapFork)
		}
		nextHead, err := protocol.VerifyAuthenticatedMapUpdate(a.config.TenantID, mapRoot, mapUpdate)
		if err != nil {
			if errors.Is(err, protocol.ErrInvalidMapProof) {
				return fmt.Errorf("%w: %v", ErrMapFork, err)
			}
			return fmt.Errorf("%w: %v", keyEventError(update.Event.Event.Kind), err)
		}

		switch keyEvent := update.Event.Event; keyEvent.Kind {
		case protocol.KeyEventEnrollment:
			if previousHead.Size != 0 {
				return fmt.Errorf("%w: identity is already enrolled", ErrEnrollment)
			}
			if keyEvent.Enrollment == nil || keyEvent.Rotation != nil || keyEvent.RotationMarker != nil {
				return fmt.Errorf("%w: malformed enrollment event", ErrEnrollment)
			}
			state, digest, err := a.verifyEnrollment(ctx, *keyEvent.Enrollment)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrEnrollment, err)
			}
			if state.IdentityID != identityID || digest != update.Event.ResultingStateDigest || digest != nextHead.CurrentStateDigest {
				return fmt.Errorf("%w: enrollment does not produce committed key-history head", ErrEnrollment)
			}
			states[digest] = state

		case protocol.KeyEventRotation:
			if previousHead.Size == 0 || keyEvent.Rotation == nil || keyEvent.RotationMarker == nil || keyEvent.Enrollment != nil {
				return fmt.Errorf("%w: malformed rotation event", ErrRotation)
			}
			oldDigest, newState, newDigest, err := a.verifyRotation(*keyEvent.Rotation, previousHead, states)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrRotation, err)
			}
			if newDigest != update.Event.ResultingStateDigest || newDigest != nextHead.CurrentStateDigest {
				return fmt.Errorf("%w: rotation does not produce committed key-history head", ErrRotation)
			}
			marker := *keyEvent.RotationMarker
			if marker.Hash == "" {
				return fmt.Errorf("%w: rotation marker hash is required", ErrRotation)
			}
			if previousMarker, ok := lastMarkers[identityID]; ok && marker.Index <= previousMarker.Index {
				return fmt.Errorf("%w: rotation marker does not advance past index %d", ErrRotation, previousMarker.Index)
			}
			rotationDigest, err := protocol.ObjectDigest(*keyEvent.Rotation)
			if err != nil {
				return fmt.Errorf("%w: digest rotation marker: %v", ErrRotation, err)
			}
			boundary := validityBoundary{Marker: marker, RotationDigest: rotationDigest}
			if a.deliveryCheckpoint.Size > marker.Index && !a.boundaryVerified(boundary) {
				return fmt.Errorf("%w: referenced rotation marker is not present in accepted delivery history", ErrRotation)
			}
			states[newDigest] = newState
			validBefore[oldDigest] = boundary
			validFrom[newDigest] = boundary
			lastMarkers[identityID] = marker

		default:
			return fmt.Errorf("%w: unknown key event kind %q", ErrRotation, keyEvent.Kind)
		}

		historyHeads[identityID] = nextHead
		mapRoot = mapUpdate.Root
	}

	a.mapRoot = mapRoot
	a.historyHeads = historyHeads
	a.states = states
	a.validFrom = validFrom
	a.validBefore = validBefore
	a.lastMarkers = lastMarkers
	return nil
}

func (a *Agent) verifyEnrollment(ctx context.Context, enrollment protocol.EnrollmentPackage) (protocol.ContinuityState, string, error) {
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
	if enrollment.IdentityID != identityID {
		return protocol.ContinuityState{}, "", errors.New("claimed identity does not match nonce-bound ID token")
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

func (a *Agent) verifyRotation(rotation protocol.RotationPackage, previousHead protocol.KeyHistoryHead, states map[string]protocol.ContinuityState) (string, protocol.ContinuityState, string, error) {
	intent := rotation.Intent
	if intent.Protocol != protocol.RotationProtocol || intent.TenantID != a.config.TenantID {
		return "", protocol.ContinuityState{}, "", errors.New("rotation protocol or tenant mismatch")
	}
	if intent.IdentityID != previousHead.IdentityID || intent.PreviousStateDigest != previousHead.CurrentStateDigest {
		return "", protocol.ContinuityState{}, "", errors.New("rotation does not continue current key-history state")
	}
	currentState, ok := states[intent.PreviousStateDigest]
	if !ok {
		return "", protocol.ContinuityState{}, "", errors.New("current continuity state is unavailable")
	}
	if intent.NewGeneration != currentState.Generation+1 {
		return "", protocol.ContinuityState{}, "", errors.New("rotation generation is not the next generation")
	}
	if protocol.DigestBytes(rotation.NewContinuityPublicKey) != intent.NewContinuityKeyDigest {
		return "", protocol.ContinuityState{}, "", errors.New("successor key does not match signed digest")
	}
	if err := protocol.Verify(currentState.ContinuityPublicKey, "continuity-rotation-old-key/v1", intent, rotation.SignatureByOldKey); err != nil {
		return "", protocol.ContinuityState{}, "", fmt.Errorf("old key did not authorize rotation: %w", err)
	}
	if err := protocol.Verify(rotation.NewContinuityPublicKey, "continuity-rotation-new-key/v1", intent, rotation.ProofByNewKey); err != nil {
		return "", protocol.ContinuityState{}, "", fmt.Errorf("new key did not prove possession: %w", err)
	}
	newState := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            a.config.TenantID,
		IdentityID:          intent.IdentityID,
		Generation:          intent.NewGeneration,
		ContinuityPublicKey: append([]byte(nil), rotation.NewContinuityPublicKey...),
		PreviousStateDigest: intent.PreviousStateDigest,
	}
	newDigest, err := newState.Digest()
	if err != nil {
		return "", protocol.ContinuityState{}, "", err
	}
	return intent.PreviousStateDigest, newState, newDigest, nil
}

// AdvanceDeliveryLog pins an RFC 6962 append-only continuation without
// applying its delivery events. The update may selectively disclose records
// (such as rotation markers) whose inclusion the agent needs to remember.
func (a *Agent) AdvanceDeliveryLog(update protocol.DeliveryLogUpdate) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.advanceDeliveryLogLocked(update)
}

func (a *Agent) advanceDeliveryLogLocked(update protocol.DeliveryLogUpdate) error {
	if err := protocol.VerifyDeliveryLogUpdate(a.deliveryCheckpoint, update); err != nil {
		return fmt.Errorf("%w: %v", ErrLogFork, err)
	}

	observed := cloneObservedRecords(a.deliveryRecords)
	for _, entry := range update.Entries {
		next := observeRecord(entry.Record)
		if previous, exists := observed[entry.Record.Index]; exists && previous != next {
			return fmt.Errorf("%w: disclosed record at index %d conflicts with an earlier proof", ErrLogFork, entry.Record.Index)
		}
		observed[entry.Record.Index] = next
	}

	a.deliveryCheckpoint = update.Checkpoint
	a.deliveryRecords = observed
	return nil
}

// ReceiveDelivery verifies inclusion in the accepted delivery log and then
// evaluates provenance. The update authenticates the checkpoint transition
// and selectively discloses the delivery (and any relevant rotation markers).
func (a *Agent) ReceiveDelivery(record protocol.DeliveryRecord, update protocol.DeliveryLogUpdate) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.advanceDeliveryLogLocked(update); err != nil {
		return err
	}
	if err := a.verifyIncludedRecord(record); err != nil {
		return err
	}

	if record.Event.Kind != protocol.DeliveryLogEventDelivery || record.Event.Delivery == nil || record.Event.Rotation != nil {
		return fmt.Errorf("%w: log record is not a well-formed delivery", ErrAttestation)
	}
	delivery := *record.Event.Delivery
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
	if boundary, rotated := a.validFrom[attestation.SigningStateDigest]; rotated {
		if !a.boundaryVerified(boundary) {
			return fmt.Errorf("%w: rotation marker establishing state is not proven", ErrSigningState)
		}
		if record.Index <= boundary.Marker.Index {
			return fmt.Errorf("%w: state is not valid before rotation marker %d", ErrSigningState, boundary.Marker.Index)
		}
	}
	if boundary, retired := a.validBefore[attestation.SigningStateDigest]; retired {
		if !a.boundaryVerified(boundary) {
			return fmt.Errorf("%w: rotation marker retiring state is not proven", ErrSigningState)
		}
		if record.Index >= boundary.Marker.Index {
			return fmt.Errorf("%w: state retired at rotation marker %d", ErrSigningState, boundary.Marker.Index)
		}
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

func (a *Agent) verifyIncludedRecord(record protocol.DeliveryRecord) error {
	if record.Index >= a.deliveryCheckpoint.Size {
		return fmt.Errorf("%w: delivery index %d is beyond accepted size %d", ErrLogFork, record.Index, a.deliveryCheckpoint.Size)
	}
	if _, err := protocol.VerifyDeliveryRecord(record); err != nil {
		return fmt.Errorf("%w: %v", ErrLogFork, err)
	}
	accepted, ok := a.deliveryRecords[record.Index]
	if !ok || accepted != observeRecord(record) {
		return fmt.Errorf("%w: delivery record is absent from accepted log history", ErrLogFork)
	}
	return nil
}

func (a *Agent) boundaryVerified(boundary validityBoundary) bool {
	observed, ok := a.deliveryRecords[boundary.Marker.Index]
	return ok &&
		observed.Hash == boundary.Marker.Hash &&
		observed.Kind == protocol.DeliveryLogEventRotation &&
		observed.RotationDigest == boundary.RotationDigest
}

func (a *Agent) Applied(fulfillmentID string) (protocol.SignedDelivery, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delivery, ok := a.applied[fulfillmentID]
	return cloneDelivery(delivery), ok
}

func observeRecord(record protocol.DeliveryRecord) observedLogRecord {
	observed := observedLogRecord{Hash: record.Hash, Kind: record.Event.Kind}
	if record.Event.Kind != protocol.DeliveryLogEventRotation || record.Event.Rotation == nil || record.Event.Delivery != nil {
		return observed
	}
	digest, err := protocol.ObjectDigest(*record.Event.Rotation)
	if err == nil {
		observed.RotationDigest = digest
	}
	return observed
}

func keyEventError(kind string) error {
	if kind == protocol.KeyEventEnrollment {
		return ErrEnrollment
	}
	return ErrRotation
}

func cloneStates(in map[string]protocol.ContinuityState) map[string]protocol.ContinuityState {
	out := make(map[string]protocol.ContinuityState, len(in))
	for key, value := range in {
		value.ContinuityPublicKey = append([]byte(nil), value.ContinuityPublicKey...)
		out[key] = value
	}
	return out
}

func cloneHeads(in map[string]protocol.KeyHistoryHead) map[string]protocol.KeyHistoryHead {
	out := make(map[string]protocol.KeyHistoryHead, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneBoundaries(in map[string]validityBoundary) map[string]validityBoundary {
	out := make(map[string]validityBoundary, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneMarkers(in map[string]protocol.DeliveryLogReference) map[string]protocol.DeliveryLogReference {
	out := make(map[string]protocol.DeliveryLogReference, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneObservedRecords(in map[uint64]observedLogRecord) map[uint64]observedLogRecord {
	out := make(map[uint64]observedLogRecord, len(in))
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
