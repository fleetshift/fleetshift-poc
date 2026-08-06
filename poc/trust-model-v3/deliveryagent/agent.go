// Package deliveryagent models the verifier and enforcement point attached to
// a target. It retains local checkpoints and independently validates OIDC key
// enrollment, continuity transitions, content signatures, and delivery order.
package deliveryagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
)

var (
	ErrEnrollment        = errors.New("enrollment verification failed")
	ErrRotation          = errors.New("rotation verification failed")
	ErrSignature         = errors.New("delivery signature verification failed")
	ErrAttestation       = errors.New("content attestation verification failed")
	ErrSigningState      = errors.New("signing state is not valid at delivery position")
	ErrMapFork           = errors.New("authenticated map update does not extend retained root")
	ErrLogFork           = errors.New("delivery does not extend retained checkpoint")
	ErrGeneration        = errors.New("delivery generation is stale or conflicting")
	ErrExceptionCapacity = errors.New("exception capacity prevents map advancement")

	// ErrAcknowledgementLost is a test fault injected after an otherwise
	// successful, locally retained delivery. It models a response lost between
	// the agent and resource manager.
	ErrAcknowledgementLost = errors.New("delivery acknowledgement was lost")
	ErrCheckpointStale     = errors.New("manager used a stale agent checkpoint")
	ErrDeliveryUnavailable = errors.New("delivery did not reach the agent")
)

var errExceptionalAncestor = errors.New("history already contains an exceptional event")

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

// DeliveryAttempt is test-observable transport metadata for the most recent
// manager-facing push. It intentionally excludes delivery content and proofs.
type DeliveryAttempt struct {
	RecordIndex            uint64
	Checkpoint             protocol.Checkpoint
	ConsistencyProofHashes int
	EntryIndexes           []uint64
	InclusionProofHashes   []int
}

type Config struct {
	TenantID          string
	TargetID          string
	OIDCIssuer        string
	OIDCClientID      string
	HTTPClient        *http.Client
	ExceptionCapacity int
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
	exceptionCapacity  int
	exceptionalEvents  map[string]struct{}

	// The following maps model delivery application state, not verifier trust
	// state. A real agent would persist the resulting effects in its own domain.
	applied        map[string]protocol.SignedDelivery
	appliedDigests map[string]string
	generations    map[string]uint64

	failBeforeAccepting      uint64
	loseNextAcknowledgement  bool
	staleCheckpointResponses uint64
	lastDeliveryAttempt      *DeliveryAttempt
}

func New(config Config) (*Agent, error) {
	if config.TenantID == "" || config.TargetID == "" || config.OIDCIssuer == "" || config.OIDCClientID == "" {
		return nil, errors.New("tenant, target, OIDC issuer, and OIDC client ID are required")
	}
	if config.ExceptionCapacity < 0 {
		return nil, errors.New("exception capacity cannot be negative")
	}
	if config.ExceptionCapacity == 0 {
		config.ExceptionCapacity = 64
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
		exceptionCapacity:  config.ExceptionCapacity,
		exceptionalEvents:  make(map[string]struct{}),
		applied:            make(map[string]protocol.SignedDelivery),
		appliedDigests:     make(map[string]string),
		generations:        make(map[string]uint64),
	}, nil
}

// VerifierCheckpoint is the complete durable cryptographic trust state. Its
// size is independent of the number of enrolled identities; only exceptional
// events consume additional bounded space.
type VerifierCheckpoint struct {
	MapRoot                 string              `json:"map_root"`
	DeliveryLogCheckpoint   protocol.Checkpoint `json:"delivery_log_checkpoint"`
	ExceptionalEventDigests []string            `json:"exceptional_event_digests,omitempty"`
}

func (a *Agent) VerifierCheckpoint() VerifierCheckpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	digests := make([]string, 0, len(a.exceptionalEvents))
	for digest := range a.exceptionalEvents {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	return VerifierCheckpoint{
		MapRoot:                 a.mapRoot,
		DeliveryLogCheckpoint:   a.deliveryCheckpoint,
		ExceptionalEventDigests: digests,
	}
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

// FailNextDeliveriesBeforeAccepting injects transport failures before the
// agent verifies, retains, or applies the next count delivery attempts.
func (a *Agent) FailNextDeliveriesBeforeAccepting(count uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failBeforeAccepting += count
}

// LoseNextAcknowledgement injects a one-shot failure after the next delivery
// has been verified, applied, and retained in the local checkpoint.
func (a *Agent) LoseNextAcknowledgement() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.loseNextAcknowledgement = true
}

func (a *Agent) StaleCheckpointResponses() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.staleCheckpointResponses
}

// LastDeliveryAttempt returns a copy of the most recent manager-facing push
// summary so tests can assert selective disclosure without exposing proofs.
func (a *Agent) LastDeliveryAttempt() (DeliveryAttempt, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastDeliveryAttempt == nil {
		return DeliveryAttempt{}, false
	}
	out := *a.lastDeliveryAttempt
	out.EntryIndexes = append([]uint64(nil), out.EntryIndexes...)
	out.InclusionProofHashes = append([]int(nil), out.InclusionProofHashes...)
	return out, true
}

// SyncMap validates a batch transactionally. AuthenticatedMapUpdate carries
// the changed identity's complete semantic history so the agent can validate
// it without retaining a per-identity head or continuity state. Objectively
// invalid events are recorded as bounded exceptions before their successor
// root is accepted.
func (a *Agent) SyncMap(ctx context.Context, updates []protocol.AuthenticatedMapUpdate) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	mapRoot := a.mapRoot
	exceptions := cloneExceptions(a.exceptionalEvents)
	var semanticErr error

	for _, mapUpdate := range updates {
		if mapUpdate.PreviousRoot != mapRoot {
			return fmt.Errorf("%w: update starts at root %q, want %q", ErrMapFork, mapUpdate.PreviousRoot, mapRoot)
		}
		update := mapUpdate.KeyHistory
		nextHead, err := protocol.VerifyAuthenticatedMapUpdate(a.config.TenantID, mapRoot, mapUpdate)
		if err != nil {
			if errors.Is(err, protocol.ErrInvalidMapProof) {
				return fmt.Errorf("%w: %v", ErrMapFork, err)
			}
			return fmt.Errorf("%w: %v", keyEventError(update.Event.Event.Kind), err)
		}
		if err := protocol.VerifyKeyHistoryRecords(nextHead, mapUpdate.SemanticHistory); err != nil {
			// Missing or malformed proof material is not evidence that the
			// authenticated event itself is bad. Refuse advancement and allow
			// the manager to retry with a complete history.
			return fmt.Errorf("%w: semantic history proof: %v", keyEventError(update.Event.Event.Kind), err)
		}
		_, exceptionalDigest, err := a.validateHistory(ctx, nextHead, mapUpdate.SemanticHistory, mapUpdate.RotationRecords, true, false)
		if err != nil {
			if errors.Is(err, errExceptionalAncestor) {
				mapRoot = mapUpdate.Root
				continue
			}
			if exceptionalDigest == "" {
				return err
			}
			if _, exists := exceptions[exceptionalDigest]; !exists {
				if len(exceptions) >= a.exceptionCapacity {
					return fmt.Errorf("%w: capacity %d is full", ErrExceptionCapacity, a.exceptionCapacity)
				}
				exceptions[exceptionalDigest] = struct{}{}
			}
			if semanticErr == nil {
				semanticErr = err
			}
		}
		mapRoot = mapUpdate.Root
	}

	a.mapRoot = mapRoot
	a.exceptionalEvents = exceptions
	return semanticErr
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

type validatedHistory struct {
	states      map[string]protocol.ContinuityState
	validFrom   map[string]validityBoundary
	validBefore map[string]validityBoundary
}

// validateHistory reconstructs continuity states and key-validity boundaries
// ephemerally. exceptionalDigest is non-empty only when the supplied,
// authenticated event is objectively invalid and may safely enter the bounded
// exception set; missing proof material is retryable and is never made sticky.
func (a *Agent) validateHistory(
	ctx context.Context,
	head protocol.KeyHistoryHead,
	history []protocol.KeyEventRecord,
	rotationRecords []protocol.DeliveryRecord,
	useAcceptedPrefix bool,
	rejectExceptions bool,
) (validatedHistory, string, error) {
	validated := validatedHistory{
		states:      make(map[string]protocol.ContinuityState, len(history)),
		validFrom:   make(map[string]validityBoundary),
		validBefore: make(map[string]validityBoundary),
	}
	if err := protocol.VerifyKeyHistoryRecords(head, history); err != nil {
		return validated, "", fmt.Errorf("%w: %v", ErrSigningState, err)
	}
	records := make(map[protocol.DeliveryLogReference]protocol.DeliveryRecord, len(rotationRecords))
	for _, record := range rotationRecords {
		if _, err := protocol.VerifyDeliveryRecord(record); err != nil {
			return validated, "", fmt.Errorf("%w: malformed rotation-marker evidence: %v", ErrRotation, err)
		}
		records[record.Reference()] = record
	}

	var currentState protocol.ContinuityState
	var currentDigest string
	var previousMarker *protocol.DeliveryLogReference
	for i, record := range history {
		_, exceptional := a.exceptionalEvents[record.Hash]
		if exceptional {
			if rejectExceptions {
				return validated, "", fmt.Errorf("%w: key event %s is in the exceptional-event set", ErrSigningState, record.Hash)
			}
			return validated, "", fmt.Errorf("%w: %s", errExceptionalAncestor, record.Hash)
		}
		useAcceptedSemantics := useAcceptedPrefix && i < len(history)-1 || rejectExceptions
		keyEvent := record.Event
		switch keyEvent.Kind {
		case protocol.KeyEventEnrollment:
			if i != 0 {
				return validated, record.Hash, fmt.Errorf("%w: identity is already enrolled", ErrEnrollment)
			}
			if keyEvent.Enrollment == nil || keyEvent.Rotation != nil || keyEvent.RotationMarker != nil {
				return validated, record.Hash, fmt.Errorf("%w: malformed enrollment event", ErrEnrollment)
			}
			var state protocol.ContinuityState
			var digest string
			var err error
			if useAcceptedSemantics {
				state, digest, err = a.reconstructAcceptedEnrollment(*keyEvent.Enrollment)
			} else {
				state, digest, err = a.verifyEnrollment(ctx, *keyEvent.Enrollment)
			}
			if err != nil {
				if errors.Is(err, errOIDCEvidenceUnavailable) {
					return validated, "", fmt.Errorf("%w: %v", ErrEnrollment, err)
				}
				return validated, record.Hash, fmt.Errorf("%w: %v", ErrEnrollment, err)
			}
			if state.IdentityID != head.IdentityID || digest != record.ResultingStateDigest {
				return validated, record.Hash, fmt.Errorf("%w: enrollment does not produce committed key-history state", ErrEnrollment)
			}
			currentState, currentDigest = state, digest
			validated.states[digest] = state

		case protocol.KeyEventRotation:
			if i == 0 || keyEvent.Rotation == nil || keyEvent.RotationMarker == nil || keyEvent.Enrollment != nil {
				return validated, record.Hash, fmt.Errorf("%w: malformed rotation event", ErrRotation)
			}
			newState, newDigest, err := a.verifyRotation(*keyEvent.Rotation, currentState, currentDigest)
			if err != nil {
				return validated, record.Hash, fmt.Errorf("%w: %v", ErrRotation, err)
			}
			if newDigest != record.ResultingStateDigest {
				return validated, record.Hash, fmt.Errorf("%w: rotation does not produce committed key-history state", ErrRotation)
			}
			marker := *keyEvent.RotationMarker
			if marker.Hash == "" {
				return validated, record.Hash, fmt.Errorf("%w: rotation marker hash is required", ErrRotation)
			}
			if previousMarker != nil && marker.Index <= previousMarker.Index {
				return validated, record.Hash, fmt.Errorf("%w: rotation marker does not advance past index %d", ErrRotation, previousMarker.Index)
			}
			markerRecord, ok := records[marker]
			if !ok {
				return validated, "", fmt.Errorf("%w: rotation marker record is unavailable", ErrRotation)
			}
			if markerRecord.Event.Kind != protocol.DeliveryLogEventRotation || markerRecord.Event.Rotation == nil || markerRecord.Event.Delivery != nil {
				return validated, record.Hash, fmt.Errorf("%w: referenced marker is not a rotation record", ErrRotation)
			}
			wantRotation, err := protocol.ObjectDigest(*keyEvent.Rotation)
			if err != nil {
				return validated, record.Hash, fmt.Errorf("%w: digest rotation: %v", ErrRotation, err)
			}
			gotRotation, err := protocol.ObjectDigest(*markerRecord.Event.Rotation)
			if err != nil || gotRotation != wantRotation {
				return validated, record.Hash, fmt.Errorf("%w: marker does not contain the committed rotation package", ErrRotation)
			}
			boundary := validityBoundary{Marker: marker, RotationDigest: wantRotation}
			validated.validBefore[currentDigest] = boundary
			validated.validFrom[newDigest] = boundary
			validated.states[newDigest] = newState
			currentState, currentDigest = newState, newDigest
			markerCopy := marker
			previousMarker = &markerCopy

		default:
			return validated, record.Hash, fmt.Errorf("%w: unknown key event kind %q", ErrRotation, keyEvent.Kind)
		}
	}
	if currentDigest != head.CurrentStateDigest {
		return validated, "", fmt.Errorf("%w: reconstructed state does not match current key-history head", ErrSigningState)
	}
	return validated, "", nil
}

// reconstructAcceptedEnrollment uses the accepted root plus absence from the
// exception set as the durable statement that the time-sensitive OIDC checks
// succeeded during map advancement. It repeats deterministic key checks but
// does not require the IdP, its old JWKS, or an unexpired token at delivery
// time.
func (a *Agent) reconstructAcceptedEnrollment(enrollment protocol.EnrollmentPackage) (protocol.ContinuityState, string, error) {
	intent := enrollment.Intent
	if intent.Protocol != protocol.EnrollmentProtocol || intent.TenantID != a.config.TenantID {
		return protocol.ContinuityState{}, "", errors.New("enrollment protocol or tenant mismatch")
	}
	if intent.ExpectedIssuer != a.config.OIDCIssuer || intent.EnrollmentClientID != a.config.OIDCClientID {
		return protocol.ContinuityState{}, "", errors.New("enrollment issuer or client does not match provisioned tenant configuration")
	}
	if enrollment.IdentityID == "" || protocol.DigestBytes(enrollment.ContinuityPublicKey) != intent.ContinuityKeyDigest {
		return protocol.ContinuityState{}, "", errors.New("accepted enrollment key binding is malformed")
	}
	if err := protocol.Verify(enrollment.ContinuityPublicKey, "enrollment-proof-of-possession/v1", intent, enrollment.ProofOfPossession); err != nil {
		return protocol.ContinuityState{}, "", fmt.Errorf("continuity-key proof of possession: %w", err)
	}
	state := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            a.config.TenantID,
		IdentityID:          enrollment.IdentityID,
		Generation:          0,
		ContinuityPublicKey: append([]byte(nil), enrollment.ContinuityPublicKey...),
	}
	digest, err := state.Digest()
	if err != nil {
		return protocol.ContinuityState{}, "", err
	}
	return state, digest, nil
}

func (a *Agent) verifyRotation(rotation protocol.RotationPackage, currentState protocol.ContinuityState, currentDigest string) (protocol.ContinuityState, string, error) {
	intent := rotation.Intent
	if intent.Protocol != protocol.RotationProtocol || intent.TenantID != a.config.TenantID {
		return protocol.ContinuityState{}, "", errors.New("rotation protocol or tenant mismatch")
	}
	if intent.IdentityID != currentState.IdentityID || intent.PreviousStateDigest != currentDigest {
		return protocol.ContinuityState{}, "", errors.New("rotation does not continue current key-history state")
	}
	if intent.NewGeneration != currentState.Generation+1 {
		return protocol.ContinuityState{}, "", errors.New("rotation generation is not the next generation")
	}
	if protocol.DigestBytes(rotation.NewContinuityPublicKey) != intent.NewContinuityKeyDigest {
		return protocol.ContinuityState{}, "", errors.New("successor key does not match signed digest")
	}
	if err := protocol.Verify(currentState.ContinuityPublicKey, "continuity-rotation-old-key/v1", intent, rotation.SignatureByOldKey); err != nil {
		return protocol.ContinuityState{}, "", fmt.Errorf("old key did not authorize rotation: %w", err)
	}
	if err := protocol.Verify(rotation.NewContinuityPublicKey, "continuity-rotation-new-key/v1", intent, rotation.ProofByNewKey); err != nil {
		return protocol.ContinuityState{}, "", fmt.Errorf("new key did not prove possession: %w", err)
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
		return protocol.ContinuityState{}, "", err
	}
	return newState, newDigest, nil
}

// AdvanceDeliveryLog pins an RFC 6962 append-only continuation without
// applying its delivery events. Disclosed records are deliberately not
// retained; a later delivery must re-supply every leaf it relies on.
func (a *Agent) AdvanceDeliveryLog(update protocol.DeliveryLogUpdate) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.advanceDeliveryLogLocked(update)
	return err
}

func (a *Agent) advanceDeliveryLogLocked(update protocol.DeliveryLogUpdate) (map[uint64]observedLogRecord, error) {
	if err := protocol.VerifyDeliveryLogUpdate(a.deliveryCheckpoint, update); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLogFork, err)
	}

	observed := make(map[uint64]observedLogRecord, len(update.Entries))
	for _, entry := range update.Entries {
		next := observeRecord(entry.Record)
		if previous, exists := observed[entry.Record.Index]; exists && previous != next {
			return nil, fmt.Errorf("%w: disclosed record at index %d conflicts within proof", ErrLogFork, entry.Record.Index)
		}
		observed[entry.Record.Index] = next
	}

	a.deliveryCheckpoint = update.Checkpoint
	return observed, nil
}

// Deliver is the manager-facing push operation. When the manager constructs a
// proof from an obsolete checkpoint, the agent returns its newer checkpoint so
// the manager can validate that checkpoint and retry. A nil result is the ack.
func (a *Agent) Deliver(record protocol.DeliveryRecord, deliveryProof protocol.DeliveryProof) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	attempt := summarizeDeliveryAttempt(record, deliveryProof.Log)
	a.lastDeliveryAttempt = &attempt
	if a.failBeforeAccepting > 0 {
		a.failBeforeAccepting--
		return ErrDeliveryUnavailable
	}
	observed, err := a.advanceDeliveryLogLocked(deliveryProof.Log)
	if err != nil {
		a.staleCheckpointResponses++
		return &CheckpointStaleError{checkpoint: a.deliveryCheckpoint, cause: err}
	}
	if err := a.receiveIncludedDeliveryLocked(record, deliveryProof.Identity, observed); err != nil {
		return err
	}
	if a.loseNextAcknowledgement {
		a.loseNextAcknowledgement = false
		return ErrAcknowledgementLost
	}
	return nil
}

func summarizeDeliveryAttempt(record protocol.DeliveryRecord, update protocol.DeliveryLogUpdate) DeliveryAttempt {
	attempt := DeliveryAttempt{
		RecordIndex:            record.Index,
		Checkpoint:             update.Checkpoint,
		ConsistencyProofHashes: len(update.ConsistencyProof),
		EntryIndexes:           make([]uint64, len(update.Entries)),
		InclusionProofHashes:   make([]int, len(update.Entries)),
	}
	for i := range update.Entries {
		attempt.EntryIndexes[i] = update.Entries[i].Record.Index
		attempt.InclusionProofHashes[i] = len(update.Entries[i].InclusionProof)
	}
	return attempt
}

// ReceiveDelivery exposes strict verifier behavior to low-level and compromise
// tests.
// Unlike Deliver, it reports an invalid checkpoint transition as ErrLogFork
// rather than participating in the manager/agent checkpoint-recovery exchange.
func (a *Agent) ReceiveDelivery(record protocol.DeliveryRecord, deliveryProof protocol.DeliveryProof) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	observed, err := a.advanceDeliveryLogLocked(deliveryProof.Log)
	if err != nil {
		return err
	}
	return a.receiveIncludedDeliveryLocked(record, deliveryProof.Identity, observed)
}

func (a *Agent) receiveIncludedDeliveryLocked(record protocol.DeliveryRecord, identityProof protocol.IdentityTrustProof, observed map[uint64]observedLogRecord) error {
	if err := a.verifyIncludedRecord(record, observed); err != nil {
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
	if identityProof.Map.Head.IdentityID != attestation.IdentityID {
		return fmt.Errorf("%w: identity proof is for a different signing identity", ErrSigningState)
	}
	if err := protocol.VerifyKeyHistoryMapProof(a.config.TenantID, a.mapRoot, identityProof.Map); err != nil {
		return fmt.Errorf("%w: current map membership: %v", ErrSigningState, err)
	}
	history, _, err := a.validateHistory(context.Background(), identityProof.Map.Head, identityProof.History, identityProof.RotationRecords, true, true)
	if err != nil {
		if errors.Is(err, ErrEnrollment) || errors.Is(err, ErrRotation) {
			return fmt.Errorf("%w: identity history: %v", ErrSigningState, err)
		}
		return err
	}
	state, ok := history.states[attestation.SigningStateDigest]
	if !ok || state.IdentityID != attestation.IdentityID {
		return fmt.Errorf("%w: unknown state for signing identity", ErrSigningState)
	}
	if boundary, rotated := history.validFrom[attestation.SigningStateDigest]; rotated {
		if !boundaryVerified(boundary, observed) {
			return fmt.Errorf("%w: rotation marker establishing state is not proven", ErrSigningState)
		}
		if record.Index <= boundary.Marker.Index {
			return fmt.Errorf("%w: state is not valid before rotation marker %d", ErrSigningState, boundary.Marker.Index)
		}
	}
	if boundary, retired := history.validBefore[attestation.SigningStateDigest]; retired {
		if !boundaryVerified(boundary, observed) {
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

func (a *Agent) verifyIncludedRecord(record protocol.DeliveryRecord, observed map[uint64]observedLogRecord) error {
	if record.Index >= a.deliveryCheckpoint.Size {
		return fmt.Errorf("%w: delivery index %d is beyond accepted size %d", ErrLogFork, record.Index, a.deliveryCheckpoint.Size)
	}
	if _, err := protocol.VerifyDeliveryRecord(record); err != nil {
		return fmt.Errorf("%w: %v", ErrLogFork, err)
	}
	accepted, ok := observed[record.Index]
	if !ok || accepted != observeRecord(record) {
		return fmt.Errorf("%w: delivery record is absent from accepted log history", ErrLogFork)
	}
	return nil
}

func boundaryVerified(boundary validityBoundary, records map[uint64]observedLogRecord) bool {
	observed, ok := records[boundary.Marker.Index]
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

func cloneExceptions(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
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
