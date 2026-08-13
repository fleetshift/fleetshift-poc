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
	RecordIndex                  uint64
	Checkpoint                   protocol.Checkpoint
	ConsistencyProofHashes       int
	EntryIndexes                 []uint64
	InclusionProofHashes         []int
	MapSiblingBitmapBytes        int
	MapSiblingHashes             int
	IdentityEventSequences       []uint64
	IdentityInclusionProofHashes []int
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
	exceptionalEvents  map[string]protocol.ExceptionalEvent

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
		exceptionalEvents:  make(map[string]protocol.ExceptionalEvent),
		applied:            make(map[string]protocol.SignedDelivery),
		appliedDigests:     make(map[string]string),
		generations:        make(map[string]uint64),
	}, nil
}

// VerifierCheckpoint is the complete durable cryptographic trust state. Its
// size is independent of the number of enrolled identities; only exceptional
// events consume additional bounded space.
type VerifierCheckpoint struct {
	MapRoot               string                      `json:"map_root"`
	DeliveryLogCheckpoint protocol.Checkpoint         `json:"delivery_log_checkpoint"`
	Exceptions            []protocol.ExceptionalEvent `json:"exceptions,omitempty"`
}

func (a *Agent) VerifierCheckpoint() VerifierCheckpoint {
	a.mu.Lock()
	defer a.mu.Unlock()
	exceptions := make([]protocol.ExceptionalEvent, 0, len(a.exceptionalEvents))
	for _, exception := range a.exceptionalEvents {
		exceptions = append(exceptions, exception)
	}
	sort.Slice(exceptions, func(i, j int) bool {
		if exceptions[i].IdentityID != exceptions[j].IdentityID {
			return exceptions[i].IdentityID < exceptions[j].IdentityID
		}
		if exceptions[i].Sequence != exceptions[j].Sequence {
			return exceptions[i].Sequence < exceptions[j].Sequence
		}
		return exceptions[i].EventDigest < exceptions[j].EventDigest
	})
	return VerifierCheckpoint{
		MapRoot:               a.mapRoot,
		DeliveryLogCheckpoint: a.deliveryCheckpoint,
		Exceptions:            exceptions,
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
	out.IdentityEventSequences = append([]uint64(nil), out.IdentityEventSequences...)
	out.IdentityInclusionProofHashes = append([]int(nil), out.IdentityInclusionProofHashes...)
	return out, true
}

// SyncMap validates a batch transactionally. Each rotation carries only its
// authenticated immediate predecessor, which is sufficient to reconstruct the
// old state and validate the new transition. Objectively invalid events are
// recorded as bounded, principal-indexed exceptions before their successor
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
		if hasIdentityException(exceptions, update.Event.IdentityID) {
			// Descendants of an unresolved exception cannot become authority.
			// Their structural heads may still advance without consuming more
			// exception entries or blocking unrelated identities.
			mapRoot = mapUpdate.Root
			continue
		}
		exceptional, err := a.validateMapEvent(ctx, nextHead, mapUpdate)
		if err != nil {
			if exceptional == nil {
				return err
			}
			if _, exists := exceptions[exceptional.EventDigest]; !exists {
				if len(exceptions) >= a.exceptionCapacity {
					return fmt.Errorf("%w: capacity %d is full", ErrExceptionCapacity, a.exceptionCapacity)
				}
				exceptions[exceptional.EventDigest] = *exceptional
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

func (a *Agent) validateMapEvent(ctx context.Context, nextHead protocol.KeyHistoryHead, update protocol.AuthenticatedMapUpdate) (*protocol.ExceptionalEvent, error) {
	record := update.KeyHistory.Event
	keyEvent := record.Event
	exception := func(err error) (*protocol.ExceptionalEvent, error) {
		return &protocol.ExceptionalEvent{
			IdentityID:           record.IdentityID,
			Sequence:             record.Sequence,
			EventDigest:          record.Hash,
			ResultingStateDigest: record.ResultingStateDigest,
		}, err
	}

	switch keyEvent.Kind {
	case protocol.KeyEventEnrollment:
		if update.KeyHistory.PreviousHead.Size != 0 {
			return exception(fmt.Errorf("%w: identity is already enrolled", ErrEnrollment))
		}
		if update.Predecessor != nil || update.RotationRecord != nil {
			return nil, fmt.Errorf("%w: enrollment carries unexpected predecessor or marker evidence", ErrEnrollment)
		}
		if keyEvent.Enrollment == nil || keyEvent.Rotation != nil || keyEvent.RotationMarker != nil {
			return exception(fmt.Errorf("%w: malformed enrollment event", ErrEnrollment))
		}
		state, digest, err := a.verifyEnrollment(ctx, *keyEvent.Enrollment)
		if err != nil {
			if errors.Is(err, errOIDCEvidenceUnavailable) {
				return nil, fmt.Errorf("%w: %v", ErrEnrollment, err)
			}
			return exception(fmt.Errorf("%w: %v", ErrEnrollment, err))
		}
		if state.IdentityID != record.IdentityID || digest != record.ResultingStateDigest || digest != nextHead.CurrentStateDigest {
			return exception(fmt.Errorf("%w: enrollment does not produce committed key-history state", ErrEnrollment))
		}
		return nil, nil

	case protocol.KeyEventRotation:
		previousHead := update.KeyHistory.PreviousHead
		if previousHead.Size == 0 || keyEvent.Rotation == nil || keyEvent.RotationMarker == nil || keyEvent.Enrollment != nil {
			return exception(fmt.Errorf("%w: malformed rotation event", ErrRotation))
		}
		if update.Predecessor == nil {
			return nil, fmt.Errorf("%w: predecessor-event proof is required", ErrRotation)
		}
		if err := protocol.VerifyKeyEventInclusionProof(previousHead, *update.Predecessor); err != nil {
			return nil, fmt.Errorf("%w: predecessor-event proof: %v", ErrRotation, err)
		}
		predecessor := update.Predecessor.Event
		if predecessor.Sequence+1 != previousHead.Size || predecessor.ResultingStateDigest != previousHead.CurrentStateDigest {
			return nil, fmt.Errorf("%w: predecessor proof does not select the current prior state", ErrRotation)
		}
		previousState, previousDigest, err := a.reconstructAcceptedState(predecessor)
		if err != nil || previousDigest != previousHead.CurrentStateDigest {
			return nil, fmt.Errorf("%w: reconstruct accepted predecessor: %v", ErrRotation, err)
		}
		_, newDigest, err := a.verifyRotation(*keyEvent.Rotation, previousState, previousDigest)
		if err != nil {
			return exception(fmt.Errorf("%w: %v", ErrRotation, err))
		}
		if newDigest != record.ResultingStateDigest || newDigest != nextHead.CurrentStateDigest {
			return exception(fmt.Errorf("%w: rotation does not produce committed key-history state", ErrRotation))
		}
		marker := *keyEvent.RotationMarker
		if marker.Hash == "" {
			return exception(fmt.Errorf("%w: rotation marker hash is required", ErrRotation))
		}
		if previousMarker := predecessor.Event.RotationMarker; previousMarker != nil && marker.Index <= previousMarker.Index {
			return exception(fmt.Errorf("%w: rotation marker does not advance past index %d", ErrRotation, previousMarker.Index))
		}
		if update.RotationRecord == nil || update.RotationRecord.Reference() != marker {
			return nil, fmt.Errorf("%w: exact rotation-marker record is required", ErrRotation)
		}
		if err := validateRotationMarkerRecord(*keyEvent.Rotation, marker, *update.RotationRecord); err != nil {
			return exception(fmt.Errorf("%w: %v", ErrRotation, err))
		}
		return nil, nil

	default:
		return exception(fmt.Errorf("%w: unknown key event kind %q", ErrRotation, keyEvent.Kind))
	}
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

// reconstructAcceptedState derives one state from its selectively disclosed
// event. The accepted map root plus absence of a principal exception records
// that time-sensitive enrollment and predecessor authorization checks already
// succeeded when this event entered the map.
func (a *Agent) reconstructAcceptedState(record protocol.KeyEventRecord) (protocol.ContinuityState, string, error) {
	switch event := record.Event; event.Kind {
	case protocol.KeyEventEnrollment:
		if record.Sequence != 0 || event.Enrollment == nil || event.Rotation != nil || event.RotationMarker != nil {
			return protocol.ContinuityState{}, "", errors.New("malformed accepted enrollment event")
		}
		state, digest, err := a.reconstructAcceptedEnrollment(*event.Enrollment)
		if err != nil {
			return protocol.ContinuityState{}, "", err
		}
		if state.IdentityID != record.IdentityID || digest != record.ResultingStateDigest {
			return protocol.ContinuityState{}, "", errors.New("accepted enrollment does not produce committed state")
		}
		return state, digest, nil

	case protocol.KeyEventRotation:
		if record.Sequence == 0 || event.Rotation == nil || event.RotationMarker == nil || event.Enrollment != nil {
			return protocol.ContinuityState{}, "", errors.New("malformed accepted rotation event")
		}
		rotation := *event.Rotation
		intent := rotation.Intent
		if intent.Protocol != protocol.RotationProtocol || intent.TenantID != a.config.TenantID || intent.IdentityID != record.IdentityID {
			return protocol.ContinuityState{}, "", errors.New("accepted rotation protocol, tenant, or identity mismatch")
		}
		if protocol.DigestBytes(rotation.NewContinuityPublicKey) != intent.NewContinuityKeyDigest {
			return protocol.ContinuityState{}, "", errors.New("accepted successor key does not match committed digest")
		}
		if err := protocol.Verify(rotation.NewContinuityPublicKey, "continuity-rotation-new-key/v1", intent, rotation.ProofByNewKey); err != nil {
			return protocol.ContinuityState{}, "", fmt.Errorf("accepted successor proof of possession: %w", err)
		}
		state := protocol.ContinuityState{
			Protocol:            protocol.ContinuityStateProtocol,
			TenantID:            a.config.TenantID,
			IdentityID:          intent.IdentityID,
			Generation:          intent.NewGeneration,
			ContinuityPublicKey: append([]byte(nil), rotation.NewContinuityPublicKey...),
			PreviousStateDigest: intent.PreviousStateDigest,
		}
		digest, err := state.Digest()
		if err != nil {
			return protocol.ContinuityState{}, "", err
		}
		if digest != record.ResultingStateDigest {
			return protocol.ContinuityState{}, "", errors.New("accepted rotation does not produce committed state")
		}
		return state, digest, nil

	default:
		return protocol.ContinuityState{}, "", fmt.Errorf("unknown accepted key event kind %q", event.Kind)
	}
}

func validateRotationMarkerRecord(rotation protocol.RotationPackage, marker protocol.DeliveryLogReference, record protocol.DeliveryRecord) error {
	if _, err := protocol.VerifyDeliveryRecord(record); err != nil {
		return fmt.Errorf("malformed rotation marker: %w", err)
	}
	if record.Reference() != marker {
		return errors.New("rotation marker reference does not match supplied record")
	}
	if record.Event.Kind != protocol.DeliveryLogEventRotation || record.Event.Rotation == nil || record.Event.Delivery != nil {
		return errors.New("referenced marker is not a rotation record")
	}
	want, err := protocol.ObjectDigest(rotation)
	if err != nil {
		return fmt.Errorf("digest committed rotation: %w", err)
	}
	got, err := protocol.ObjectDigest(*record.Event.Rotation)
	if err != nil || got != want {
		return errors.New("marker does not contain the committed rotation package")
	}
	return nil
}

func validityBoundaryForEvent(record protocol.KeyEventRecord) (validityBoundary, bool, error) {
	event := record.Event
	if event.Kind == protocol.KeyEventEnrollment {
		return validityBoundary{}, false, nil
	}
	if event.Kind != protocol.KeyEventRotation || event.Rotation == nil || event.RotationMarker == nil || event.Enrollment != nil {
		return validityBoundary{}, false, errors.New("key event is not a well-formed rotation")
	}
	if event.RotationMarker.Hash == "" {
		return validityBoundary{}, false, errors.New("rotation marker hash is required")
	}
	digest, err := protocol.ObjectDigest(*event.Rotation)
	if err != nil {
		return validityBoundary{}, false, fmt.Errorf("digest rotation: %w", err)
	}
	return validityBoundary{Marker: *event.RotationMarker, RotationDigest: digest}, true, nil
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

	attempt := summarizeDeliveryAttempt(record, deliveryProof)
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

func summarizeDeliveryAttempt(record protocol.DeliveryRecord, deliveryProof protocol.DeliveryProof) DeliveryAttempt {
	update := deliveryProof.Log
	attempt := DeliveryAttempt{
		RecordIndex:                  record.Index,
		Checkpoint:                   update.Checkpoint,
		ConsistencyProofHashes:       len(update.ConsistencyProof),
		EntryIndexes:                 make([]uint64, len(update.Entries)),
		InclusionProofHashes:         make([]int, len(update.Entries)),
		MapSiblingBitmapBytes:        len(deliveryProof.Identity.Map.SiblingBitmap),
		MapSiblingHashes:             len(deliveryProof.Identity.Map.SiblingHashes),
		IdentityEventSequences:       []uint64{deliveryProof.Identity.SigningEvent.Event.Sequence},
		IdentityInclusionProofHashes: []int{len(deliveryProof.Identity.SigningEvent.InclusionProof)},
	}
	if deliveryProof.Identity.SuccessorEvent != nil {
		attempt.IdentityEventSequences = append(attempt.IdentityEventSequences, deliveryProof.Identity.SuccessorEvent.Event.Sequence)
		attempt.IdentityInclusionProofHashes = append(attempt.IdentityInclusionProofHashes, len(deliveryProof.Identity.SuccessorEvent.InclusionProof))
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
	if hasIdentityException(a.exceptionalEvents, attestation.IdentityID) {
		return fmt.Errorf("%w: signing identity has an unresolved exceptional event", ErrSigningState)
	}
	head := identityProof.Map.Head
	if err := protocol.VerifyKeyEventInclusionProof(head, identityProof.SigningEvent); err != nil {
		return fmt.Errorf("%w: signing-event proof: %v", ErrSigningState, err)
	}
	signingEvent := identityProof.SigningEvent.Event
	if signingEvent.ResultingStateDigest != attestation.SigningStateDigest {
		return fmt.Errorf("%w: selected event does not produce claimed signing state", ErrSigningState)
	}
	state, stateDigest, err := a.reconstructAcceptedState(signingEvent)
	if err != nil || stateDigest != attestation.SigningStateDigest || state.IdentityID != attestation.IdentityID {
		return fmt.Errorf("%w: reconstruct signing state: %v", ErrSigningState, err)
	}

	if boundary, ok, err := validityBoundaryForEvent(signingEvent); err != nil {
		return fmt.Errorf("%w: establishing boundary: %v", ErrSigningState, err)
	} else if ok {
		if !boundaryVerified(boundary, observed) {
			return fmt.Errorf("%w: rotation marker establishing state is not proven", ErrSigningState)
		}
		if record.Index <= boundary.Marker.Index {
			return fmt.Errorf("%w: state is not valid before rotation marker %d", ErrSigningState, boundary.Marker.Index)
		}
	}

	wantsSuccessor := signingEvent.Sequence+1 < head.Size
	if wantsSuccessor && identityProof.SuccessorEvent == nil {
		return fmt.Errorf("%w: immediate successor event is required for historical state", ErrSigningState)
	}
	if !wantsSuccessor && identityProof.SuccessorEvent != nil {
		return fmt.Errorf("%w: current signing state has an unexpected successor event", ErrSigningState)
	}
	if identityProof.SuccessorEvent != nil {
		if err := protocol.VerifyKeyEventInclusionProof(head, *identityProof.SuccessorEvent); err != nil {
			return fmt.Errorf("%w: successor-event proof: %v", ErrSigningState, err)
		}
		successor := identityProof.SuccessorEvent.Event
		if successor.Sequence != signingEvent.Sequence+1 || successor.Event.Kind != protocol.KeyEventRotation || successor.Event.Rotation == nil {
			return fmt.Errorf("%w: supplied event is not the immediate rotation successor", ErrSigningState)
		}
		if successor.Event.Rotation.Intent.PreviousStateDigest != stateDigest {
			return fmt.Errorf("%w: successor does not retire the claimed signing state", ErrSigningState)
		}
		if _, _, err := a.reconstructAcceptedState(successor); err != nil {
			return fmt.Errorf("%w: reconstruct successor state: %v", ErrSigningState, err)
		}
		boundary, ok, err := validityBoundaryForEvent(successor)
		if err != nil || !ok {
			return fmt.Errorf("%w: retiring boundary: %v", ErrSigningState, err)
		}
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

func hasIdentityException(exceptions map[string]protocol.ExceptionalEvent, identityID string) bool {
	for _, exception := range exceptions {
		if exception.IdentityID == identityID {
			return true
		}
	}
	return false
}

func cloneExceptions(in map[string]protocol.ExceptionalEvent) map[string]protocol.ExceptionalEvent {
	out := make(map[string]protocol.ExceptionalEvent, len(in))
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
