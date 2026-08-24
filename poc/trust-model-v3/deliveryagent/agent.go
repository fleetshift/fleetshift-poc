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
	MapUpdates                   int
	IdentityEventSequences       []uint64
	IdentityInclusionProofHashes []int
}

type Config struct {
	TenantID          string
	TargetID          string
	TrustManifest     protocol.TenantTrustManifest
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
	if config.TenantID == "" || config.TargetID == "" {
		return nil, errors.New("tenant and target are required")
	}
	if config.TrustManifest.TenantID != config.TenantID || config.TrustManifest.OIDCIssuer == "" || config.TrustManifest.EnrollmentClientID == "" {
		return nil, errors.New("provisioned tenant trust manifest with issuer and enrollment client is required")
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

// SyncMap validates a batch of map updates. Objectively invalid or currently
// unverifiable events are recorded as bounded principal exceptions so they do
// not block unrelated identities. Hard structural errors abort without
// committing the successor root.
func (a *Agent) SyncMap(ctx context.Context, updates []protocol.AuthenticatedMapUpdate) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	semanticErr, err := a.applyMapUpdatesLocked(ctx, updates)
	if err != nil {
		return err
	}
	return semanticErr
}

func (a *Agent) applyMapUpdatesLocked(ctx context.Context, updates []protocol.AuthenticatedMapUpdate) (semanticErr error, hardErr error) {
	mapRoot := a.mapRoot
	exceptions := cloneExceptions(a.exceptionalEvents)

	for _, mapUpdate := range updates {
		if mapUpdate.PreviousRoot != mapRoot {
			return nil, fmt.Errorf("%w: update starts at root %q, want %q", ErrMapFork, mapUpdate.PreviousRoot, mapRoot)
		}
		update := mapUpdate.KeyHistory
		nextHead, err := protocol.VerifyAuthenticatedMapUpdate(a.config.TenantID, mapRoot, mapUpdate)
		if err != nil {
			if errors.Is(err, protocol.ErrInvalidMapProof) {
				return nil, fmt.Errorf("%w: %v", ErrMapFork, err)
			}
			return nil, fmt.Errorf("%w: %v", keyEventError(update.Event.Event.Kind), err)
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
				return nil, err
			}
			if _, exists := exceptions[exceptional.EventDigest]; !exists {
				if len(exceptions) >= a.exceptionCapacity {
					return nil, fmt.Errorf("%w: capacity %d is full", ErrExceptionCapacity, a.exceptionCapacity)
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
	return semanticErr, nil
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
		if err != nil {
			return nil, fmt.Errorf("%w: reconstruct accepted predecessor: %v", ErrRotation, err)
		}
		if previousDigest != previousHead.CurrentStateDigest {
			return nil, fmt.Errorf("%w: reconstructed predecessor state does not match the accepted head", ErrRotation)
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
		if a.deliveryCheckpoint.Size > marker.Index {
			if err := verifyEagerMarkerInclusion(a.deliveryCheckpoint, marker, *update.RotationRecord, update.MarkerLogCheckpoint, update.MarkerLogInclusion); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrRotation, err)
			}
		}
		return nil, nil

	default:
		return exception(fmt.Errorf("%w: unknown key event kind %q", ErrRotation, keyEvent.Kind))
	}
}

func (a *Agent) verifyEnrollment(ctx context.Context, enrollment protocol.EnrollmentPackage) (protocol.ContinuityState, string, error) {
	intent := enrollment.Intent
	if err := a.config.TrustManifest.MatchesEnrollmentIntent(intent); err != nil {
		return protocol.ContinuityState{}, "", err
	}
	if err := protocol.VerifyEnrollmentProofOfPossession(enrollment); err != nil {
		return protocol.ContinuityState{}, "", err
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
	return protocol.EnrolledContinuityState(a.config.TenantID, identityID, enrollment.ContinuityPublicKey)
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
		authorization := rotation.Authorization
		if authorization.Protocol != protocol.RotationProtocol || authorization.TenantID != a.config.TenantID || authorization.IdentityID != record.IdentityID {
			return protocol.ContinuityState{}, "", errors.New("accepted rotation protocol, tenant, or identity mismatch")
		}
		if err := protocol.Verify(rotation.NewContinuityPublicKey, "continuity-rotation-new-key/v1", authorization, rotation.ProofByNewKey); err != nil {
			return protocol.ContinuityState{}, "", fmt.Errorf("accepted successor proof of possession: %w", err)
		}
		state, digest, err := protocol.ReconstructSuccessorState(rotation)
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
	if record.Event.Kind != protocol.DeliveryLogEventRotation || record.Event.Marker == nil || record.Event.Commitment != nil {
		return errors.New("referenced marker is not a rotation record")
	}
	if record.Rotation == nil {
		return errors.New("rotation marker is missing its authorization package")
	}
	if err := protocol.VerifyRotationMatchesMarker(*record.Rotation, *record.Event.Marker); err != nil {
		return err
	}
	want, err := protocol.RotationAuthorizationDigest(rotation)
	if err != nil {
		return fmt.Errorf("digest committed rotation: %w", err)
	}
	got, err := protocol.RotationAuthorizationDigest(*record.Rotation)
	if err != nil || got != want {
		return errors.New("marker does not contain the committed rotation package")
	}
	return nil
}

func verifyEagerMarkerInclusion(checkpoint protocol.Checkpoint, marker protocol.DeliveryLogReference, record protocol.DeliveryRecord, proofCheckpoint *protocol.Checkpoint, inclusion []string) error {
	if proofCheckpoint == nil || *proofCheckpoint != checkpoint {
		return errors.New("eager marker inclusion must be proven under the accepted delivery-log checkpoint")
	}
	update := protocol.DeliveryLogUpdate{
		From:       checkpoint,
		Checkpoint: checkpoint,
		Entries: []protocol.DeliveryLogEntryProof{{
			Record:         record,
			InclusionProof: inclusion,
		}},
	}
	if err := protocol.VerifyDeliveryLogUpdate(checkpoint, update); err != nil {
		return fmt.Errorf("eager marker inclusion: %w", err)
	}
	if record.Index != marker.Index || record.Hash != marker.Hash {
		return errors.New("eager marker inclusion is for a different leaf")
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
	digest, err := protocol.RotationAuthorizationDigest(*event.Rotation)
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
	if err := a.config.TrustManifest.MatchesEnrollmentIntent(enrollment.Intent); err != nil {
		return protocol.ContinuityState{}, "", err
	}
	if enrollment.IdentityID == "" {
		return protocol.ContinuityState{}, "", errors.New("accepted enrollment key binding is malformed")
	}
	if err := protocol.VerifyEnrollmentProofOfPossession(enrollment); err != nil {
		return protocol.ContinuityState{}, "", err
	}
	return protocol.EnrolledContinuityState(a.config.TenantID, enrollment.IdentityID, enrollment.ContinuityPublicKey)
}

func (a *Agent) verifyRotation(rotation protocol.RotationPackage, currentState protocol.ContinuityState, currentDigest string) (protocol.ContinuityState, string, error) {
	authorization := rotation.Authorization
	if authorization.Protocol != protocol.RotationProtocol || authorization.TenantID != a.config.TenantID {
		return protocol.ContinuityState{}, "", errors.New("rotation protocol or tenant mismatch")
	}
	if authorization.IdentityID != currentState.IdentityID || authorization.PreviousStateDigest != currentDigest {
		return protocol.ContinuityState{}, "", errors.New("rotation does not continue current key-history state")
	}
	if rotation.NewGeneration != currentState.Generation+1 {
		return protocol.ContinuityState{}, "", errors.New("rotation generation is not the next generation")
	}
	if err := protocol.VerifyRotationAuthorization(rotation, currentState.ContinuityPublicKey); err != nil {
		return protocol.ContinuityState{}, "", err
	}
	newState, newDigest, err := protocol.ReconstructSuccessorState(rotation)
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
	retained := a.deliveryCheckpoint
	observed, err := a.advanceDeliveryLogLocked(deliveryProof.Log)
	if err != nil {
		if isStaleLogProof(deliveryProof.Log, retained) {
			a.staleCheckpointResponses++
			return &CheckpointStaleError{checkpoint: retained, cause: err}
		}
		return err
	}
	if isStaleLogProof(deliveryProof.Log, retained) {
		// RFC 6962 consistency from the empty tree is empty. A proof built from
		// that older cache can therefore verify as an equal-size no-op against
		// the retained head. Report the retained checkpoint so the manager can
		// reconstruct without applying the included delivery again.
		a.staleCheckpointResponses++
		return &CheckpointStaleError{
			checkpoint: retained,
			cause:      fmt.Errorf("constructed from checkpoint size %d", deliveryProof.Log.From.Size),
		}
	}
	if _, err := a.applyMapUpdatesLocked(context.Background(), deliveryProof.MapUpdates); err != nil {
		return err
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

func isStaleLogProof(update protocol.DeliveryLogUpdate, retained protocol.Checkpoint) bool {
	if update.From.Size >= retained.Size {
		return false
	}
	// A proof constructed from an older manager cache of the same branch either
	// already matches the retained head (lost acknowledgement) or extends past it.
	return update.Checkpoint == retained || update.Checkpoint.Size > retained.Size
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
		MapUpdates:                   len(deliveryProof.MapUpdates),
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
	if _, err := a.applyMapUpdatesLocked(context.Background(), deliveryProof.MapUpdates); err != nil {
		return err
	}
	return a.receiveIncludedDeliveryLocked(record, deliveryProof.Identity, observed)
}

func (a *Agent) receiveIncludedDeliveryLocked(record protocol.DeliveryRecord, identityProof protocol.IdentityTrustProof, observed map[uint64]observedLogRecord) error {
	if err := a.verifyIncludedRecord(record, observed); err != nil {
		return err
	}

	if record.Event.Kind != protocol.DeliveryLogEventDelivery || record.Event.Commitment == nil || record.Event.Marker != nil {
		return fmt.Errorf("%w: log record is not a well-formed delivery", ErrAttestation)
	}
	if record.Delivery == nil {
		return fmt.Errorf("%w: delivery package is missing", ErrAttestation)
	}
	delivery := *record.Delivery
	if err := protocol.VerifyDeliveryMatchesCommitment(delivery, *record.Event.Commitment); err != nil {
		return fmt.Errorf("%w: %v", ErrAttestation, err)
	}
	attestation := delivery.Attestation
	if attestation.Protocol != protocol.DeliveryProtocol || attestation.TenantID != a.config.TenantID || attestation.TargetID != a.config.TargetID {
		return fmt.Errorf("%w: protocol, tenant, or target mismatch", ErrAttestation)
	}
	if attestation.Action != protocol.ActionPut && attestation.Action != protocol.ActionRemove {
		return fmt.Errorf("%w: unsupported action %q", ErrAttestation, attestation.Action)
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
	if err != nil {
		return fmt.Errorf("%w: reconstruct signing state: %v", ErrSigningState, err)
	}
	if stateDigest != attestation.SigningStateDigest || state.IdentityID != attestation.IdentityID {
		return fmt.Errorf("%w: reconstructed signing state does not match the attestation", ErrSigningState)
	}

	if boundary, ok, err := validityBoundaryForEvent(signingEvent); err != nil {
		return fmt.Errorf("%w: establishing boundary: %v", ErrSigningState, err)
	} else if ok {
		if !boundaryVerified(boundary, observed) {
			return fmt.Errorf("%w: rotation marker establishing state is not proven", ErrSigningState)
		}
		if record.Index <= boundary.Marker.Index {
			return fmt.Errorf("%w: state is not valid at or before rotation marker %d", ErrSigningState, boundary.Marker.Index)
		}
	}

	wantsSuccessor := signingEvent.Sequence+1 < head.Size
	if wantsSuccessor && identityProof.SuccessorEvent == nil {
		return fmt.Errorf("%w: immediate successor event is required for historical state", ErrSigningState)
	}
	if !wantsSuccessor && identityProof.SuccessorEvent != nil {
		return fmt.Errorf("%w: current signing state has an unexpected successor event", ErrSigningState)
	}
	if !wantsSuccessor && head.CurrentStateDigest != attestation.SigningStateDigest {
		return fmt.Errorf("%w: current history head does not commit the claimed signing state", ErrSigningState)
	}
	if identityProof.SuccessorEvent != nil {
		if err := protocol.VerifyKeyEventInclusionProof(head, *identityProof.SuccessorEvent); err != nil {
			return fmt.Errorf("%w: successor-event proof: %v", ErrSigningState, err)
		}
		successor := identityProof.SuccessorEvent.Event
		if successor.Sequence != signingEvent.Sequence+1 || successor.Event.Kind != protocol.KeyEventRotation || successor.Event.Rotation == nil {
			return fmt.Errorf("%w: supplied event is not the immediate rotation successor", ErrSigningState)
		}
		if successor.Event.Rotation.Authorization.PreviousStateDigest != stateDigest {
			return fmt.Errorf("%w: successor does not retire the claimed signing state", ErrSigningState)
		}
		if _, _, err := a.reconstructAcceptedState(successor); err != nil {
			return fmt.Errorf("%w: reconstruct successor state: %v", ErrSigningState, err)
		}
		boundary, ok, err := validityBoundaryForEvent(successor)
		if err != nil {
			return fmt.Errorf("%w: retiring boundary: %v", ErrSigningState, err)
		}
		if !ok {
			return fmt.Errorf("%w: successor does not establish a retiring boundary", ErrSigningState)
		}
		if !boundaryVerified(boundary, observed) {
			return fmt.Errorf("%w: rotation marker retiring state is not proven", ErrSigningState)
		}
		if record.Index >= boundary.Marker.Index {
			return fmt.Errorf("%w: state retired at rotation marker %d", ErrSigningState, boundary.Marker.Index)
		}
	}
	if err := protocol.VerifyDeliverySignature(delivery, state.ContinuityPublicKey); err != nil {
		return fmt.Errorf("%w: %v", ErrSignature, err)
	}

	digest, err := protocol.DeliveryPackageDigest(delivery)
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
	if record.Event.Kind != protocol.DeliveryLogEventRotation || record.Event.Marker == nil || record.Rotation == nil {
		return observed
	}
	digest, err := protocol.RotationAuthorizationDigest(*record.Rotation)
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
