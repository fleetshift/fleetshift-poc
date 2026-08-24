// Package resourcemanager models FleetShift's central resource-manager role.
// It performs ordinary authorization, stores ordered records, and couriers
// evidence, but it is not a signing authority for user provenance.
package resourcemanager

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/sparsemap"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
)

const (
	ActionEnroll  = "enroll"
	ActionRotate  = "rotate"
	ActionDeliver = "deliver"
)

var (
	ErrUnauthorized            = errors.New("resource-manager authorization denied")
	ErrAgentUnavailable        = errors.New("delivery agent is unavailable")
	ErrInvalidRequestSignature = errors.New("request signature verification failed")
)

type AuthorizationRequest struct {
	TenantID string
	CallerID string
	Action   string
	TargetID string
}

type Authorizer func(AuthorizationRequest) error

// DeliveryAgent is the manager-side view of a target delivery agent. A nil
// error is the acknowledgement that the agent durably accepted the supplied
// log checkpoint and delivery.
type DeliveryAgent interface {
	Deliver(record protocol.DeliveryRecord, proof protocol.DeliveryProof) error
	MapRoot() string
}

// staleCheckpointError is returned by an agent when a request was constructed
// from an older manager-side checkpoint than the agent has already retained.
// Keeping this as a behavioral interface avoids coupling the manager to one
// in-process delivery-agent implementation.
type staleCheckpointError interface {
	error
	LatestCheckpoint() protocol.Checkpoint
}

type agentRoute struct {
	mu sync.Mutex

	agent      DeliveryAgent
	checkpoint protocol.Checkpoint
}

// RotationCommit is the resource manager's atomic result for a rotation. The
// marker's position in the delivery log is the key-validity boundary; the
// map update commits that exact marker into the identity's key-history head.
type RotationCommit struct {
	Marker    protocol.DeliveryRecord
	MapUpdate protocol.AuthenticatedMapUpdate
}

type historyHeadVersion struct {
	mapRevision uint64
	head        protocol.KeyHistoryHead
}

type preparedKeyHistoryUpdate struct {
	update        protocol.AuthenticatedMapUpdate
	historyTree   *merklelog.Tree
	historyAppend *merklelog.PendingAppend
	mapAppend     *sparsemap.PendingSet
}

type Manager struct {
	mu sync.RWMutex

	tenantID   string
	authorizer Authorizer

	mapRoot        string
	mapUpdates     []protocol.AuthenticatedMapUpdate
	mapTree        *sparsemap.Tree
	mapRevisions   map[string]uint64
	histories      map[string]protocol.KeyHistoryHead
	headVersions   map[string][]historyHeadVersion
	historyRecords map[string][]protocol.KeyEventRecord
	historyTrees   map[string]*merklelog.Tree
	stateEvents    map[string]map[string]uint64
	deliveries     []protocol.DeliveryRecord
	deliveryTree   *merklelog.Tree
	agents         map[string]*agentRoute
}

func New(tenantID string, authorizer Authorizer) *Manager {
	if authorizer == nil {
		authorizer = func(AuthorizationRequest) error { return nil }
	}
	emptyMapRoot, err := protocol.KeyHistoryMapRoot(tenantID, nil)
	if err != nil {
		panic(err) // The fixed map representation is always JSON encodable.
	}
	mapTree := sparsemap.New(tenantID)
	storedMapRoot, err := protocol.EncodeHash(mapTree.Root())
	if err != nil || storedMapRoot != emptyMapRoot {
		panic("protocol and sparse-map store disagree on the empty root")
	}
	return &Manager{
		tenantID:       tenantID,
		authorizer:     authorizer,
		mapRoot:        emptyMapRoot,
		mapTree:        mapTree,
		mapRevisions:   map[string]uint64{emptyMapRoot: 0},
		histories:      make(map[string]protocol.KeyHistoryHead),
		headVersions:   make(map[string][]historyHeadVersion),
		historyRecords: make(map[string][]protocol.KeyEventRecord),
		historyTrees:   make(map[string]*merklelog.Tree),
		stateEvents:    make(map[string]map[string]uint64),
		deliveryTree:   merklelog.New(),
		agents:         make(map[string]*agentRoute),
	}
}

// RegisterAgent installs the delivery route for one target. The manager starts
// with an empty acknowledged checkpoint; an already-running agent can correct
// that view on the first push.
func (m *Manager) RegisterAgent(targetID string, agent DeliveryAgent) error {
	if targetID == "" || agent == nil {
		return errors.New("target ID and delivery agent are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.agents[targetID]; exists {
		return fmt.Errorf("delivery agent for target %q is already registered", targetID)
	}
	m.agents[targetID] = &agentRoute{
		agent:      agent,
		checkpoint: protocol.EmptyCheckpoint(),
	}
	return nil
}

// AgentCheckpoint returns the latest checkpoint acknowledged by the agent for
// targetID. It intentionally does not query the agent's local state.
func (m *Manager) AgentCheckpoint(targetID string) (protocol.Checkpoint, bool) {
	m.mu.RLock()
	route, ok := m.agents[targetID]
	m.mu.RUnlock()
	if !ok {
		return protocol.Checkpoint{}, false
	}
	route.mu.Lock()
	defer route.mu.Unlock()
	return route.checkpoint, true
}

func (m *Manager) SubmitEnrollment(enrollment protocol.EnrollmentPackage) (protocol.AuthenticatedMapUpdate, error) {
	if enrollment.Intent.TenantID != m.tenantID || enrollment.IdentityID == "" {
		return protocol.AuthenticatedMapUpdate{}, fmt.Errorf("enrollment tenant or identity does not match manager tenant")
	}
	if err := protocol.VerifyEnrollmentProofOfPossession(enrollment); err != nil {
		return protocol.AuthenticatedMapUpdate{}, fmt.Errorf("%w: %v", ErrInvalidRequestSignature, err)
	}
	if err := m.authorizer(AuthorizationRequest{TenantID: m.tenantID, CallerID: enrollment.IdentityID, Action: ActionEnroll}); err != nil {
		return protocol.AuthenticatedMapUpdate{}, fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.histories[enrollment.IdentityID]; exists {
		return protocol.AuthenticatedMapUpdate{}, errors.New("identity already has a key history")
	}
	return m.appendEnrollmentLocked(enrollment)
}

func (m *Manager) SubmitRotation(callerID string, rotation protocol.RotationPackage) (RotationCommit, error) {
	if rotation.Authorization.TenantID != m.tenantID || rotation.Authorization.IdentityID != callerID {
		return RotationCommit{}, fmt.Errorf("%w: rotation caller or tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorizer(AuthorizationRequest{
		TenantID: m.tenantID,
		CallerID: callerID,
		Action:   ActionRotate,
	}); err != nil {
		return RotationCommit{}, fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	currentHead, ok := m.histories[callerID]
	if !ok || currentHead.CurrentStateDigest != rotation.Authorization.PreviousStateDigest {
		return RotationCommit{}, errors.New("rotation does not continue manager's current key history")
	}
	oldKey, err := m.currentPublicKeyLocked(callerID)
	if err != nil {
		return RotationCommit{}, err
	}
	if err := protocol.VerifyRotationAuthorization(rotation, oldKey); err != nil {
		return RotationCommit{}, fmt.Errorf("%w: %v", ErrInvalidRequestSignature, err)
	}
	if _, digest, err := protocol.ReconstructSuccessorState(rotation); err != nil || digest != rotation.Authorization.NewStateDigest {
		return RotationCommit{}, fmt.Errorf("%w: successor state does not match signed digest", ErrInvalidRequestSignature)
	}
	return m.appendRotationLocked(rotation)
}

func (m *Manager) SubmitDelivery(callerID string, delivery protocol.SignedDelivery) (protocol.DeliveryRecord, error) {
	attestation := delivery.Attestation
	if attestation.TenantID != m.tenantID || attestation.IdentityID != callerID {
		return protocol.DeliveryRecord{}, fmt.Errorf("%w: delivery caller or tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorizer(AuthorizationRequest{
		TenantID: m.tenantID,
		CallerID: callerID,
		Action:   ActionDeliver,
		TargetID: attestation.TargetID,
	}); err != nil {
		return protocol.DeliveryRecord{}, fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	m.mu.Lock()
	publicKey, err := m.publicKeyForStateLocked(attestation.IdentityID, attestation.SigningStateDigest)
	m.mu.Unlock()
	if err != nil {
		return protocol.DeliveryRecord{}, err
	}
	if err := protocol.VerifyDeliverySignature(delivery, publicKey); err != nil {
		return protocol.DeliveryRecord{}, fmt.Errorf("%w: %v", ErrInvalidRequestSignature, err)
	}
	record, err := m.appendDelivery(delivery)
	if err != nil {
		return protocol.DeliveryRecord{}, err
	}
	if err := m.pushDelivery(record); err != nil {
		return record, fmt.Errorf("push delivery to target %q: %w", attestation.TargetID, err)
	}
	return record, nil
}

// RetryDelivery pushes an already committed delivery without appending a new
// log record or repeating the original caller authorization decision.
func (m *Manager) RetryDelivery(index uint64) error {
	m.mu.RLock()
	size := len(m.deliveries)
	if index >= uint64(size) {
		m.mu.RUnlock()
		return fmt.Errorf("delivery-log index %d is beyond size %d", index, size)
	}
	record := cloneDeliveryRecord(m.deliveries[index])
	m.mu.RUnlock()
	if record.Event.Kind != protocol.DeliveryLogEventDelivery || record.Delivery == nil {
		return fmt.Errorf("delivery-log index %d is not a delivery", index)
	}
	if err := m.pushDelivery(record); err != nil {
		return fmt.Errorf("retry delivery-log index %d: %w", index, err)
	}
	return nil
}

func (m *Manager) pushDelivery(record protocol.DeliveryRecord) error {
	if record.Delivery == nil {
		return errors.New("delivery record has no signed delivery")
	}
	targetID := record.Delivery.Attestation.TargetID
	m.mu.RLock()
	route, ok := m.agents[targetID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w for target %q", ErrAgentUnavailable, targetID)
	}

	// The target delivery contract permits only one in-flight delivery per
	// fulfillment. Serializing this POC's target route also ensures checkpoint
	// construction and acknowledgement recording cannot race one another.
	route.mu.Lock()
	defer route.mu.Unlock()
	for {
		mapRoot := route.agent.MapRoot()
		m.mu.RLock()
		mapUpdates := m.cloneMapUpdatesAfterLocked(mapRoot)
		proofRoot := mapRoot
		if len(mapUpdates) > 0 {
			proofRoot = mapUpdates[len(mapUpdates)-1].Root
		}
		identityProof, identityErr := m.identityTrustProofAtLocked(
			record.Delivery.Attestation.IdentityID,
			record.Delivery.Attestation.SigningStateDigest,
			proofRoot,
		)
		if identityErr != nil {
			m.mu.RUnlock()
			return fmt.Errorf("construct identity trust proof: %w", identityErr)
		}
		indexes := deliveryProofIndexes(record, identityProof)
		update, err := deliveryLogUpdate(m.deliveryTree, m.deliveries, route.checkpoint, indexes...)
		if err == nil {
			err = m.attachMarkerInclusionsLocked(mapUpdates, update.Checkpoint)
		}
		m.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("construct proof from acknowledged agent checkpoint: %w", err)
		}

		if err := route.agent.Deliver(record, protocol.DeliveryProof{MapUpdates: mapUpdates, Log: update, Identity: identityProof}); err != nil {
			var stale staleCheckpointError
			if !errors.As(err, &stale) {
				return err
			}
			latest := stale.LatestCheckpoint()
			if latest.Size <= route.checkpoint.Size {
				return err
			}
			if err := m.validateAgentCheckpoint(latest); err != nil {
				return fmt.Errorf("agent reported an invalid newer checkpoint: %w", err)
			}
			route.checkpoint = latest
			continue
		}

		// A successful call is the acknowledgement. The manager records exactly
		// the checkpoint whose consistency and inclusion proofs were delivered.
		route.checkpoint = update.Checkpoint
		return nil
	}
}

func (m *Manager) validateAgentCheckpoint(checkpoint protocol.Checkpoint) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	want, err := checkpointAt(m.deliveryTree, checkpoint.Size)
	if err != nil {
		return err
	}
	if checkpoint != want {
		return errors.New("checkpoint is not on the retained delivery-log branch")
	}
	return nil
}

// deliveryProofIndexes selects the delivery plus the rotation markers that
// bound its signing state under the verifier's accepted identity history. The
// consistency proof commits all undisclosed intervening leaves.
func deliveryProofIndexes(record protocol.DeliveryRecord, identityProof protocol.IdentityTrustProof) []uint64 {
	if record.Delivery == nil {
		return []uint64{record.Index}
	}
	indexes := make([]uint64, 0, 3)
	if marker := identityProof.SigningEvent.Event.Event.RotationMarker; marker != nil {
		indexes = appendUniqueIndex(indexes, marker.Index)
	}
	if identityProof.SuccessorEvent != nil {
		if marker := identityProof.SuccessorEvent.Event.Event.RotationMarker; marker != nil {
			indexes = appendUniqueIndex(indexes, marker.Index)
		}
	}
	return appendUniqueIndex(indexes, record.Index)
}

func appendUniqueIndex(indexes []uint64, index uint64) []uint64 {
	for _, existing := range indexes {
		if existing == index {
			return indexes
		}
	}
	return append(indexes, index)
}

func (m *Manager) DeliveryCheckpoint() protocol.Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	checkpoint, err := checkpointAt(m.deliveryTree, m.deliveryTree.Size())
	if err != nil {
		panic(err) // In-memory tree state is maintained under the same lock.
	}
	return checkpoint
}

// MapUpdatesAfter returns the ordinary catch-up sequence rooted at root. The
// sequence itself is not another authenticated log: each update independently
// proves that it starts at the caller's root and changes exactly one map leaf.
func (m *Manager) MapUpdatesAfter(root string) []protocol.AuthenticatedMapUpdate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cloneMapUpdatesAfterLocked(root)
}

func (m *Manager) cloneMapUpdatesAfterLocked(root string) []protocol.AuthenticatedMapUpdate {
	if root == m.mapRoot {
		return nil
	}
	// mapRevisions stores the SMT revision at which root became current. That
	// value is also the slice index of the next catch-up update because the
	// empty map is revision 0 and each committed leaf replacement appends one
	// AuthenticatedMapUpdate.
	start, ok := m.mapRevisions[root]
	if !ok || start >= uint64(len(m.mapUpdates)) {
		return nil
	}
	updates := m.mapUpdates[start:]
	out := make([]protocol.AuthenticatedMapUpdate, len(updates))
	for i := range updates {
		out[i] = cloneAuthenticatedMapUpdate(updates[i])
	}
	return out
}

// MapUpdatesAfterCheckpoint is MapUpdatesAfter plus eager marker-inclusion
// proofs under logCheckpoint, used when that checkpoint is already past a
// referenced rotation marker.
func (m *Manager) MapUpdatesAfterCheckpoint(root string, logCheckpoint protocol.Checkpoint) []protocol.AuthenticatedMapUpdate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	updates := m.cloneMapUpdatesAfterLocked(root)
	if err := m.attachMarkerInclusionsLocked(updates, logCheckpoint); err != nil {
		return nil
	}
	return updates
}

func (m *Manager) attachMarkerInclusionsLocked(updates []protocol.AuthenticatedMapUpdate, logCheckpoint protocol.Checkpoint) error {
	if logCheckpoint.Size == 0 {
		return nil
	}
	want, err := checkpointAt(m.deliveryTree, logCheckpoint.Size)
	if err != nil {
		return err
	}
	if want != logCheckpoint {
		return errors.New("log checkpoint is not on the retained delivery-log branch")
	}
	for i := range updates {
		marker := updates[i].KeyHistory.Event.Event.RotationMarker
		if marker == nil || logCheckpoint.Size <= marker.Index {
			continue
		}
		if updates[i].RotationRecord == nil {
			return fmt.Errorf("rotation at marker %d is missing its log record", marker.Index)
		}
		inclusion, err := m.deliveryTree.InclusionProof(marker.Index, logCheckpoint.Size)
		if err != nil {
			return fmt.Errorf("construct eager marker inclusion for index %d: %w", marker.Index, err)
		}
		checkpoint := logCheckpoint
		updates[i].MarkerLogCheckpoint = &checkpoint
		updates[i].MarkerLogInclusion = protocol.EncodeProof(inclusion)
	}
	return nil
}

// DeliveryLogUpdate returns a compact consistency proof from checkpoint to the
// current tenant log and inclusion proofs for only the requested indexes.
func (m *Manager) DeliveryLogUpdate(checkpoint protocol.Checkpoint, indexes ...uint64) (protocol.DeliveryLogUpdate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deliveryLogUpdate(m.deliveryTree, m.deliveries, checkpoint, indexes...)
}

// IdentityTrustProof returns the manager-assembled proof for one signing state:
// its event, optional immediate successor, and the current authenticated head.
func (m *Manager) IdentityTrustProof(identityID, signingStateDigest string) (protocol.IdentityTrustProof, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityTrustProofAtLocked(identityID, signingStateDigest, m.mapRoot)
}

// IdentityTrustProofAt assembles a proof for a verifier that intentionally
// retains an older accepted map root.
func (m *Manager) IdentityTrustProofAt(identityID, signingStateDigest, root string) (protocol.IdentityTrustProof, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityTrustProofAtLocked(identityID, signingStateDigest, root)
}

func (m *Manager) identityTrustProofAtLocked(identityID, signingStateDigest, root string) (protocol.IdentityTrustProof, error) {
	revision, ok := m.mapRevisions[root]
	if !ok {
		return protocol.IdentityTrustProof{}, fmt.Errorf("map root %q is not retained", root)
	}
	versions := m.headVersions[identityID]
	position := sort.Search(len(versions), func(i int) bool {
		return versions[i].mapRevision > revision
	})
	if position == 0 {
		return protocol.IdentityTrustProof{}, fmt.Errorf("identity %q has no key-history head under map root %q", identityID, root)
	}
	head := versions[position-1].head
	mapKey := protocol.KeyHistoryMapKey(m.tenantID, identityID)
	bitmap, siblings, err := m.mapTree.CompressedProofAt(revision, mapKey)
	if err != nil {
		return protocol.IdentityTrustProof{}, fmt.Errorf("read map membership path at revision %d: %w", revision, err)
	}
	mapProof, err := protocol.NewKeyHistoryMapProofFromProof(m.tenantID, root, head, bitmap, siblings)
	if err != nil {
		return protocol.IdentityTrustProof{}, err
	}
	if head.Size > uint64(len(m.historyRecords[identityID])) {
		return protocol.IdentityTrustProof{}, fmt.Errorf("retained history for %q has %d events, head requires %d", identityID, len(m.historyRecords[identityID]), head.Size)
	}
	sequence, ok := m.stateEvents[identityID][signingStateDigest]
	if !ok || sequence >= head.Size || m.historyRecords[identityID][sequence].ResultingStateDigest != signingStateDigest {
		return protocol.IdentityTrustProof{}, fmt.Errorf("signing state %q is not present under map root %q", signingStateDigest, root)
	}
	signingEvent, err := m.keyEventProofLocked(head, sequence)
	if err != nil {
		return protocol.IdentityTrustProof{}, err
	}
	var successor *protocol.KeyEventInclusionProof
	if sequence+1 < head.Size {
		proof, err := m.keyEventProofLocked(head, sequence+1)
		if err != nil {
			return protocol.IdentityTrustProof{}, err
		}
		successor = &proof
	}
	return protocol.IdentityTrustProof{
		Map:            mapProof,
		SigningEvent:   signingEvent,
		SuccessorEvent: successor,
	}, nil
}

// keyEventProofLocked performs one indexed event read plus logarithmic Merkle
// node reads. It deliberately does not load or replay the identity's event
// bodies, matching the retrieval shape expected from the server's proof store.
func (m *Manager) keyEventProofLocked(head protocol.KeyHistoryHead, sequence uint64) (protocol.KeyEventInclusionProof, error) {
	records := m.historyRecords[head.IdentityID]
	if sequence >= head.Size || sequence >= uint64(len(records)) {
		return protocol.KeyEventInclusionProof{}, fmt.Errorf("key-event sequence %d is unavailable under history size %d", sequence, head.Size)
	}
	tree := m.historyTrees[head.IdentityID]
	if tree == nil || head.Size > tree.Size() {
		return protocol.KeyEventInclusionProof{}, fmt.Errorf("key-history proof nodes for %q at size %d are unavailable", head.IdentityID, head.Size)
	}
	inclusion, err := tree.InclusionProof(sequence, head.Size)
	if err != nil {
		return protocol.KeyEventInclusionProof{}, err
	}
	eventProof := protocol.KeyEventInclusionProof{
		Event:          cloneKeyEventRecord(records[sequence]),
		InclusionProof: protocol.EncodeProof(inclusion),
	}
	if err := protocol.VerifyKeyEventInclusionProof(head, eventProof); err != nil {
		return protocol.KeyEventInclusionProof{}, fmt.Errorf("constructed key-event proof did not verify: %w", err)
	}
	return eventProof, nil
}

func deliveryLogUpdate(tree *merklelog.Tree, records []protocol.DeliveryRecord, previous protocol.Checkpoint, indexes ...uint64) (protocol.DeliveryLogUpdate, error) {
	if previous.Size > tree.Size() {
		return protocol.DeliveryLogUpdate{}, fmt.Errorf("checkpoint size %d is beyond log size %d", previous.Size, tree.Size())
	}
	wantPrevious, err := checkpointAt(tree, previous.Size)
	if err != nil {
		return protocol.DeliveryLogUpdate{}, err
	}
	if previous != wantPrevious {
		return protocol.DeliveryLogUpdate{}, errors.New("checkpoint is not on the retained delivery-log branch")
	}
	current, err := checkpointAt(tree, tree.Size())
	if err != nil {
		return protocol.DeliveryLogUpdate{}, err
	}
	consistency, err := tree.ConsistencyProof(previous.Size, current.Size)
	if err != nil {
		return protocol.DeliveryLogUpdate{}, fmt.Errorf("construct delivery-log consistency proof: %w", err)
	}
	update := protocol.DeliveryLogUpdate{
		From:             previous,
		Checkpoint:       current,
		ConsistencyProof: protocol.EncodeProof(consistency),
		Entries:          make([]protocol.DeliveryLogEntryProof, 0, len(indexes)),
	}
	seen := make(map[uint64]struct{}, len(indexes))
	for _, index := range indexes {
		if _, duplicate := seen[index]; duplicate {
			return protocol.DeliveryLogUpdate{}, fmt.Errorf("delivery-log index %d requested more than once", index)
		}
		seen[index] = struct{}{}
		if index >= uint64(len(records)) || index >= current.Size {
			return protocol.DeliveryLogUpdate{}, fmt.Errorf("delivery-log index %d is beyond size %d", index, current.Size)
		}
		inclusion, err := tree.InclusionProof(index, current.Size)
		if err != nil {
			return protocol.DeliveryLogUpdate{}, fmt.Errorf("construct inclusion proof for index %d: %w", index, err)
		}
		update.Entries = append(update.Entries, protocol.DeliveryLogEntryProof{
			Record:         cloneDeliveryRecord(records[index]),
			InclusionProof: protocol.EncodeProof(inclusion),
		})
	}
	return update, nil
}

func (m *Manager) Compromised() *CompromisedManager {
	return &CompromisedManager{manager: m}
}

func (m *Manager) appendEnrollmentLocked(enrollment protocol.EnrollmentPackage) (protocol.AuthenticatedMapUpdate, error) {
	_, stateDigest, err := protocol.EnrolledContinuityState(m.tenantID, enrollment.IdentityID, enrollment.ContinuityPublicKey)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, fmt.Errorf("digest enrolled state: %w", err)
	}
	prepared, err := m.prepareKeyHistoryUpdateLocked(enrollment.IdentityID, stateDigest, protocol.KeyEvent{
		Kind:       protocol.KeyEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	if err := m.commitKeyHistoryLocked(enrollment.IdentityID, prepared); err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	return cloneAuthenticatedMapUpdate(prepared.update), nil
}

// appendRotationLocked computes the marker and map update before mutating
// either structure, then commits both under the same sequencer lock. A
// production store would use an equivalent transaction or ordered durable
// protocol.
func (m *Manager) appendRotationLocked(rotation protocol.RotationPackage) (RotationCommit, error) {
	stateDigest, err := rotationStateDigest(rotation)
	if err != nil {
		return RotationCommit{}, err
	}
	marker, err := m.newRotationMarkerLocked(rotation)
	if err != nil {
		return RotationCommit{}, fmt.Errorf("build rotation marker: %w", err)
	}
	prepared, err := m.prepareKeyHistoryUpdateLocked(rotation.Authorization.IdentityID, stateDigest, protocol.KeyEvent{
		Kind:           protocol.KeyEventRotation,
		Rotation:       cloneRotation(&rotation),
		RotationMarker: pointerTo(marker.Reference()),
	})
	if err != nil {
		return RotationCommit{}, err
	}
	// The marker is appended atomically below, so it is not yet discoverable in
	// m.deliveries when the selective semantic proof is built.
	markerEvidence := cloneDeliveryRecord(marker)
	prepared.update.RotationRecord = &markerEvidence

	if err := m.appendDeliveryRecordLocked(marker); err != nil {
		return RotationCommit{}, fmt.Errorf("append rotation marker: %w", err)
	}
	if err := m.commitKeyHistoryLocked(rotation.Authorization.IdentityID, prepared); err != nil {
		return RotationCommit{}, err
	}
	return RotationCommit{Marker: cloneDeliveryRecord(marker), MapUpdate: cloneAuthenticatedMapUpdate(prepared.update)}, nil
}

func (m *Manager) newRotationMarkerLocked(rotation protocol.RotationPackage) (protocol.DeliveryRecord, error) {
	return protocol.NewRotationLogRecord(m.deliveryTree.Size(), cloneRotationValue(rotation))
}

func (m *Manager) appendRotationMapUpdateLocked(rotation protocol.RotationPackage, marker protocol.DeliveryLogReference) (protocol.AuthenticatedMapUpdate, error) {
	stateDigest, err := rotationStateDigest(rotation)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	prepared, err := m.prepareKeyHistoryUpdateLocked(rotation.Authorization.IdentityID, stateDigest, protocol.KeyEvent{
		Kind:           protocol.KeyEventRotation,
		Rotation:       cloneRotation(&rotation),
		RotationMarker: pointerTo(marker),
	})
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	if err := m.commitKeyHistoryLocked(rotation.Authorization.IdentityID, prepared); err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	return cloneAuthenticatedMapUpdate(prepared.update), nil
}

func (m *Manager) prepareKeyHistoryUpdateLocked(identityID, stateDigest string, keyEvent protocol.KeyEvent) (preparedKeyHistoryUpdate, error) {
	previousHead, exists := m.histories[identityID]
	if !exists {
		previousHead = protocol.EmptyKeyHistoryHead(identityID)
	}
	historyTree := m.historyTrees[identityID]
	if historyTree == nil {
		if exists {
			return preparedKeyHistoryUpdate{}, fmt.Errorf("key-history proof store for %q is unavailable", identityID)
		}
		historyTree = merklelog.New()
	}
	if historyTree.Size() != previousHead.Size {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("key-history proof store for %q has size %d, head has size %d", identityID, historyTree.Size(), previousHead.Size)
	}
	previousHistoryRoot, err := historyTree.Root()
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("read previous key-history root: %w", err)
	}
	previousCheckpoint, err := protocol.NewCheckpoint(previousHead.Size, previousHistoryRoot)
	if err != nil || previousCheckpoint.Root != previousHead.Root {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("key-history proof store for %q does not match retained head", identityID)
	}

	event, leafHash, err := protocol.NewKeyEventRecord(previousHead, stateDigest, keyEvent)
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("build key event: %w", err)
	}
	historyAppend, err := historyTree.BeginAppendHash(leafHash)
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("prepare key-history append: %w", err)
	}
	successorHistoryRoot, err := historyAppend.Root()
	if err != nil {
		return preparedKeyHistoryUpdate{}, err
	}
	inclusion, err := historyAppend.InclusionProof(event.Sequence, historyAppend.Size())
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("build key-event inclusion proof: %w", err)
	}
	consistency, err := historyAppend.ConsistencyProof(previousHead.Size, historyAppend.Size())
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("build key-history consistency proof: %w", err)
	}
	historyUpdate, err := protocol.NewKeyHistoryUpdateFromAppend(previousHead, event, successorHistoryRoot, inclusion, consistency)
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("build key-history update: %w", err)
	}

	mapKey := protocol.KeyHistoryMapKey(m.tenantID, identityID)
	siblingBitmap, siblingHashes, err := m.mapTree.CompressedProof(mapKey)
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("read authenticated-map path: %w", err)
	}
	var previousLeaf *protocol.KeyHistoryHead
	if exists {
		leaf := previousHead
		previousLeaf = &leaf
	}
	mapUpdate, err := protocol.NewAuthenticatedMapUpdateFromProof(
		m.tenantID,
		m.mapRoot,
		previousLeaf,
		historyUpdate,
		siblingBitmap,
		siblingHashes,
	)
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("build authenticated-map update: %w", err)
	}
	nextMapValue, err := protocol.KeyHistoryMapValueHash(historyUpdate.Head)
	if err != nil {
		return preparedKeyHistoryUpdate{}, err
	}
	mapAppend, err := m.mapTree.BeginSet(mapKey, nextMapValue)
	if err != nil {
		return preparedKeyHistoryUpdate{}, fmt.Errorf("prepare authenticated-map write: %w", err)
	}
	prospectiveMapRoot, err := protocol.EncodeHash(mapAppend.Root())
	if err != nil || prospectiveMapRoot != mapUpdate.Root {
		return preparedKeyHistoryUpdate{}, errors.New("authenticated-map proof and node store produced different successor roots")
	}

	if previousHead.Size > 0 {
		predecessor, err := m.keyEventProofLocked(previousHead, previousHead.Size-1)
		if err != nil {
			return preparedKeyHistoryUpdate{}, fmt.Errorf("build predecessor-event proof: %w", err)
		}
		mapUpdate.Predecessor = &predecessor
	}
	if marker := keyEvent.RotationMarker; marker != nil && marker.Index < uint64(len(m.deliveries)) {
		record := m.deliveries[marker.Index]
		if record.Hash == marker.Hash {
			markerEvidence := cloneDeliveryRecord(record)
			mapUpdate.RotationRecord = &markerEvidence
		}
	}
	return preparedKeyHistoryUpdate{
		update:        mapUpdate,
		historyTree:   historyTree,
		historyAppend: historyAppend,
		mapAppend:     mapAppend,
	}, nil
}

func (m *Manager) commitKeyHistoryLocked(identityID string, prepared preparedKeyHistoryUpdate) error {
	update := prepared.update
	head := update.KeyHistory.Head
	event := cloneKeyEventRecord(update.KeyHistory.Event)
	if update.PreviousRoot != m.mapRoot || event.IdentityID != identityID || head.IdentityID != identityID {
		return errors.New("prepared key-history update no longer matches manager state")
	}
	if _, err := protocol.VerifyAuthenticatedMapUpdate(m.tenantID, m.mapRoot, update); err != nil {
		return fmt.Errorf("verify prepared authenticated-map update: %w", err)
	}
	if current := m.historyTrees[identityID]; current != nil && current != prepared.historyTree {
		return errors.New("key-history proof store changed before commit")
	}
	if prepared.historyTree == nil || prepared.historyAppend == nil || prepared.mapAppend == nil {
		return errors.New("prepared key-history storage transaction is incomplete")
	}
	if prepared.historyTree.Size() != event.Sequence || prepared.historyAppend.Size() != head.Size {
		return errors.New("prepared key-history append has the wrong size")
	}
	historyRoot, err := prepared.historyAppend.Root()
	if err != nil {
		return fmt.Errorf("read prepared key-history root: %w", err)
	}
	historyCheckpoint, err := protocol.NewCheckpoint(head.Size, historyRoot)
	if err != nil || historyCheckpoint.Root != head.Root {
		return errors.New("prepared key-history proof store does not match successor head")
	}
	mapRoot, err := protocol.EncodeHash(prepared.mapAppend.Root())
	if err != nil || mapRoot != update.Root || prepared.mapAppend.Revision() != m.mapTree.Revision()+1 {
		return errors.New("prepared sparse-map write does not match successor root or revision")
	}

	// Both transactions have been completely validated under the sequencer
	// lock. Production storage performs these commits with the marker append in
	// one database transaction (or an equivalent ordered durable protocol).
	if err := prepared.historyAppend.Commit(); err != nil {
		return fmt.Errorf("commit key-history proof nodes: %w", err)
	}
	if err := prepared.mapAppend.Commit(); err != nil {
		return fmt.Errorf("commit authenticated-map proof nodes: %w", err)
	}

	m.historyTrees[identityID] = prepared.historyTree
	m.mapUpdates = append(m.mapUpdates, cloneAuthenticatedMapUpdate(update))
	m.mapRoot = update.Root
	m.mapRevisions[update.Root] = m.mapTree.Revision()
	m.histories[identityID] = head
	m.headVersions[identityID] = append(m.headVersions[identityID], historyHeadVersion{
		mapRevision: m.mapTree.Revision(),
		head:        head,
	})
	m.historyRecords[identityID] = append(m.historyRecords[identityID], event)
	if m.stateEvents[identityID] == nil {
		m.stateEvents[identityID] = make(map[string]uint64)
	}
	if _, exists := m.stateEvents[identityID][event.ResultingStateDigest]; !exists {
		m.stateEvents[identityID][event.ResultingStateDigest] = event.Sequence
	}
	return nil
}

func (m *Manager) appendDelivery(delivery protocol.SignedDelivery) (protocol.DeliveryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := protocol.NewDeliveryLogRecord(m.deliveryTree.Size(), delivery)
	if err != nil {
		return protocol.DeliveryRecord{}, fmt.Errorf("append delivery record: %w", err)
	}
	if err := m.appendDeliveryRecordLocked(record); err != nil {
		return protocol.DeliveryRecord{}, fmt.Errorf("append delivery Merkle leaf: %w", err)
	}
	return cloneDeliveryRecord(record), nil
}

func (m *Manager) appendDeliveryRecordLocked(record protocol.DeliveryRecord) error {
	if record.Index != m.deliveryTree.Size() || record.Index != uint64(len(m.deliveries)) {
		return fmt.Errorf("delivery-log record index %d does not continue size %d", record.Index, m.deliveryTree.Size())
	}
	leafHash, err := protocol.VerifyDeliveryRecord(record)
	if err != nil {
		return err
	}
	index, appendedHash, err := m.deliveryTree.AppendHash(leafHash)
	if err != nil {
		return err
	}
	if index != record.Index || !bytes.Equal(appendedHash, leafHash) {
		return errors.New("delivery-log tree appended a different leaf")
	}
	m.deliveries = append(m.deliveries, cloneDeliveryRecord(record))
	return nil
}

// CompromisedManager is an explicit attack harness. It exposes what arbitrary
// code running as the resource manager could place in its own stores or route
// to an agent while still lacking the user's private key.
type CompromisedManager struct {
	manager *Manager
}

func (c *CompromisedManager) AppendEnrollment(enrollment protocol.EnrollmentPackage) protocol.AuthenticatedMapUpdate {
	c.manager.mu.Lock()
	defer c.manager.mu.Unlock()
	record, err := c.manager.appendEnrollmentLocked(enrollment)
	if err != nil {
		panic(err)
	}
	return record
}

func (c *CompromisedManager) AppendRotation(rotation protocol.RotationPackage) RotationCommit {
	c.manager.mu.Lock()
	defer c.manager.mu.Unlock()
	commit, err := c.manager.appendRotationLocked(rotation)
	if err != nil {
		panic(err)
	}
	return commit
}

func (c *CompromisedManager) AppendRotationMarker(rotation protocol.RotationPackage) protocol.DeliveryRecord {
	c.manager.mu.Lock()
	defer c.manager.mu.Unlock()
	marker, err := c.manager.newRotationMarkerLocked(rotation)
	if err != nil {
		panic(err)
	}
	if err := c.manager.appendDeliveryRecordLocked(marker); err != nil {
		panic(err)
	}
	return cloneDeliveryRecord(marker)
}

// AppendRotationMapUpdate creates a structurally valid authenticated-map update
// that names an attacker-selected existing or future log record as its marker.
// Agents use this to prove that an index or checkpoint alone is not a cutoff.
func (c *CompromisedManager) AppendRotationMapUpdate(rotation protocol.RotationPackage, marker protocol.DeliveryLogReference) protocol.AuthenticatedMapUpdate {
	c.manager.mu.Lock()
	defer c.manager.mu.Unlock()
	update, err := c.manager.appendRotationMapUpdateLocked(rotation, marker)
	if err != nil {
		panic(err)
	}
	return update
}

func (c *CompromisedManager) AppendDelivery(delivery protocol.SignedDelivery) protocol.DeliveryRecord {
	record, err := c.manager.appendDelivery(delivery)
	if err != nil {
		panic(err)
	}
	return record
}

// ForgeDeliveryAt constructs a valid structural record on an older checkpoint
// without changing the manager's main branch. An established agent should
// reject it because it is absent from that agent's retained log history.
func (c *CompromisedManager) ForgeDeliveryAt(checkpoint protocol.Checkpoint, delivery protocol.SignedDelivery) (protocol.DeliveryRecord, protocol.DeliveryLogUpdate) {
	c.manager.mu.RLock()
	defer c.manager.mu.RUnlock()
	if checkpoint.Size > uint64(len(c.manager.deliveries)) {
		panic("fork checkpoint is beyond manager delivery log")
	}
	forkTree := merklelog.New()
	forkRecords := make([]protocol.DeliveryRecord, 0, checkpoint.Size+1)
	for _, retained := range c.manager.deliveries[:checkpoint.Size] {
		leafHash, err := protocol.VerifyDeliveryRecord(retained)
		if err != nil {
			panic(err)
		}
		if _, _, err := forkTree.AppendHash(leafHash); err != nil {
			panic(err)
		}
		forkRecords = append(forkRecords, cloneDeliveryRecord(retained))
	}
	record, err := protocol.NewDeliveryLogRecord(checkpoint.Size, delivery)
	if err != nil {
		panic(err)
	}
	leafHash, err := protocol.VerifyDeliveryRecord(record)
	if err != nil {
		panic(err)
	}
	if _, _, err := forkTree.AppendHash(leafHash); err != nil {
		panic(err)
	}
	forkRecords = append(forkRecords, record)
	update, err := deliveryLogUpdate(forkTree, forkRecords, checkpoint, record.Index)
	if err != nil {
		panic(err)
	}
	return cloneDeliveryRecord(record), update
}

// ForgeEnrollmentFromEmptyMap constructs a structurally valid authenticated-
// map branch from the empty root without changing the manager's main branch.
func (c *CompromisedManager) ForgeEnrollmentFromEmptyMap(enrollment protocol.EnrollmentPackage) protocol.AuthenticatedMapUpdate {
	_, stateDigest, err := protocol.EnrolledContinuityState(enrollment.Intent.TenantID, enrollment.IdentityID, enrollment.ContinuityPublicKey)
	if err != nil {
		panic(err)
	}
	update, err := protocol.NewKeyHistoryUpdate(protocol.EmptyKeyHistoryHead(enrollment.IdentityID), nil, stateDigest, protocol.KeyEvent{
		Kind:       protocol.KeyEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
	if err != nil {
		panic(err)
	}
	mapUpdate, err := protocol.NewAuthenticatedMapUpdate(enrollment.Intent.TenantID, nil, update)
	if err != nil {
		panic(err)
	}
	return mapUpdate
}

func rotationStateDigest(rotation protocol.RotationPackage) (string, error) {
	_, digest, err := protocol.ReconstructSuccessorState(rotation)
	if err == nil {
		return digest, nil
	}
	if rotation.Authorization.NewStateDigest == "" {
		return "", err
	}
	// A compromised manager may still structurally commit a package whose
	// successor digest does not match the signed authorization.
	return rotation.Authorization.NewStateDigest, nil
}

func (m *Manager) currentPublicKeyLocked(identityID string) ([]byte, error) {
	records := m.historyRecords[identityID]
	if len(records) == 0 {
		return nil, fmt.Errorf("identity %q has no key events", identityID)
	}
	return protocol.ContinuityPublicKeyFromEvent(records[len(records)-1].Event)
}

func (m *Manager) publicKeyForStateLocked(identityID, stateDigest string) ([]byte, error) {
	sequences, ok := m.stateEvents[identityID]
	if !ok {
		return nil, fmt.Errorf("identity %q has no key events", identityID)
	}
	sequence, ok := sequences[stateDigest]
	if !ok || sequence >= uint64(len(m.historyRecords[identityID])) {
		return nil, fmt.Errorf("signing state %q is not present for identity %q", stateDigest, identityID)
	}
	return protocol.ContinuityPublicKeyFromEvent(m.historyRecords[identityID][sequence].Event)
}

func checkpointAt(tree *merklelog.Tree, size uint64) (protocol.Checkpoint, error) {
	root, err := tree.RootAt(size)
	if err != nil {
		return protocol.Checkpoint{}, err
	}
	return protocol.NewCheckpoint(size, root)
}

func cloneEnrollment(in *protocol.EnrollmentPackage) *protocol.EnrollmentPackage {
	if in == nil {
		return nil
	}
	out := *in
	out.ContinuityPublicKey = append([]byte(nil), in.ContinuityPublicKey...)
	out.ProofOfPossession = append([]byte(nil), in.ProofOfPossession...)
	return &out
}

func cloneRotation(in *protocol.RotationPackage) *protocol.RotationPackage {
	if in == nil {
		return nil
	}
	out := cloneRotationValue(*in)
	return &out
}

func cloneRotationValue(in protocol.RotationPackage) protocol.RotationPackage {
	out := in
	out.NewContinuityPublicKey = append([]byte(nil), in.NewContinuityPublicKey...)
	out.SignatureByOldKey = append([]byte(nil), in.SignatureByOldKey...)
	out.ProofByNewKey = append([]byte(nil), in.ProofByNewKey...)
	return out
}

func cloneDelivery(in protocol.SignedDelivery) protocol.SignedDelivery {
	out := in
	out.Content = append([]byte(nil), in.Content...)
	out.Signature = append([]byte(nil), in.Signature...)
	return out
}

func cloneKeyEvent(in protocol.KeyEvent) protocol.KeyEvent {
	out := in
	out.Enrollment = cloneEnrollment(in.Enrollment)
	out.Rotation = cloneRotation(in.Rotation)
	if in.RotationMarker != nil {
		marker := *in.RotationMarker
		out.RotationMarker = &marker
	}
	return out
}

func cloneKeyHistoryUpdate(in *protocol.KeyHistoryUpdate) *protocol.KeyHistoryUpdate {
	if in == nil {
		return nil
	}
	out := *in
	out.Event.Event = cloneKeyEvent(in.Event.Event)
	out.InclusionProof = append([]string(nil), in.InclusionProof...)
	out.ConsistencyProof = append([]string(nil), in.ConsistencyProof...)
	return &out
}

func cloneKeyEventRecord(in protocol.KeyEventRecord) protocol.KeyEventRecord {
	out := in
	out.Event = cloneKeyEvent(in.Event)
	return out
}

func cloneKeyEventInclusionProof(in *protocol.KeyEventInclusionProof) *protocol.KeyEventInclusionProof {
	if in == nil {
		return nil
	}
	out := *in
	out.Event = cloneKeyEventRecord(in.Event)
	out.InclusionProof = append([]string(nil), in.InclusionProof...)
	return &out
}

func cloneAuthenticatedMapUpdate(in protocol.AuthenticatedMapUpdate) protocol.AuthenticatedMapUpdate {
	out := in
	if in.PreviousHead != nil {
		previousHead := *in.PreviousHead
		out.PreviousHead = &previousHead
	}
	out.KeyHistory = *cloneKeyHistoryUpdate(&in.KeyHistory)
	out.SiblingBitmap = append([]byte(nil), in.SiblingBitmap...)
	out.SiblingHashes = append([]string(nil), in.SiblingHashes...)
	out.Predecessor = cloneKeyEventInclusionProof(in.Predecessor)
	if in.RotationRecord != nil {
		record := cloneDeliveryRecord(*in.RotationRecord)
		out.RotationRecord = &record
	}
	if in.MarkerLogCheckpoint != nil {
		checkpoint := *in.MarkerLogCheckpoint
		out.MarkerLogCheckpoint = &checkpoint
	}
	out.MarkerLogInclusion = append([]string(nil), in.MarkerLogInclusion...)
	return out
}

func cloneDeliveryRecord(in protocol.DeliveryRecord) protocol.DeliveryRecord {
	out := in
	if in.Event.Commitment != nil {
		commitment := *in.Event.Commitment
		out.Event.Commitment = &commitment
	}
	if in.Event.Marker != nil {
		marker := *in.Event.Marker
		out.Event.Marker = &marker
	}
	if in.Delivery != nil {
		delivery := cloneDelivery(*in.Delivery)
		out.Delivery = &delivery
	}
	out.Rotation = cloneRotation(in.Rotation)
	return out
}

func pointerTo[T any](value T) *T {
	return &value
}
