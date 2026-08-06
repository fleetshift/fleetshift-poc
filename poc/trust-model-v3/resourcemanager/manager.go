// Package resourcemanager models FleetShift's central resource-manager role.
// It performs ordinary authorization, stores ordered records, and couriers
// evidence, but it is not a signing authority for user provenance.
package resourcemanager

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
)

const (
	ActionEnroll  = "enroll"
	ActionRotate  = "rotate"
	ActionDeliver = "deliver"
)

var (
	ErrUnauthorized     = errors.New("resource-manager authorization denied")
	ErrAgentUnavailable = errors.New("delivery agent is unavailable")
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

type Manager struct {
	mu sync.RWMutex

	tenantID   string
	authorizer Authorizer

	mapRoot        string
	mapUpdates     []protocol.AuthenticatedMapUpdate
	histories      map[string]protocol.KeyHistoryHead
	historyRecords map[string][]protocol.KeyEventRecord
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
	return &Manager{
		tenantID:       tenantID,
		authorizer:     authorizer,
		mapRoot:        emptyMapRoot,
		histories:      make(map[string]protocol.KeyHistoryHead),
		historyRecords: make(map[string][]protocol.KeyEventRecord),
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
	if rotation.Intent.TenantID != m.tenantID || rotation.Intent.IdentityID != callerID {
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
	if !ok || currentHead.CurrentStateDigest != rotation.Intent.PreviousStateDigest {
		return RotationCommit{}, errors.New("rotation does not continue manager's current key history")
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
	if record.Event.Kind != protocol.DeliveryLogEventDelivery || record.Event.Delivery == nil {
		return fmt.Errorf("delivery-log index %d is not a delivery", index)
	}
	if err := m.pushDelivery(record); err != nil {
		return fmt.Errorf("retry delivery-log index %d: %w", index, err)
	}
	return nil
}

func (m *Manager) pushDelivery(record protocol.DeliveryRecord) error {
	if record.Event.Delivery == nil {
		return errors.New("delivery record has no signed delivery")
	}
	targetID := record.Event.Delivery.Attestation.TargetID
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
		identityProof, identityErr := m.identityTrustProofAtLocked(record.Event.Delivery.Attestation.IdentityID, mapRoot)
		indexes := deliveryProofIndexes(record, identityProof.History)
		update, err := deliveryLogUpdate(m.deliveryTree, m.deliveries, route.checkpoint, indexes...)
		m.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("construct proof from acknowledged agent checkpoint: %w", err)
		}
		if identityErr != nil {
			return fmt.Errorf("construct identity trust proof: %w", identityErr)
		}

		if err := route.agent.Deliver(record, protocol.DeliveryProof{Log: update, Identity: identityProof}); err != nil {
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
func deliveryProofIndexes(record protocol.DeliveryRecord, history []protocol.KeyEventRecord) []uint64 {
	delivery := record.Event.Delivery
	if delivery == nil {
		return []uint64{record.Index}
	}
	indexes := make([]uint64, 0, 3)
	for i := range history {
		if history[i].ResultingStateDigest != delivery.Attestation.SigningStateDigest {
			continue
		}
		if marker := history[i].Event.RotationMarker; marker != nil {
			indexes = appendUniqueIndex(indexes, marker.Index)
		}
		if i+1 < len(history) {
			if marker := history[i+1].Event.RotationMarker; marker != nil {
				indexes = appendUniqueIndex(indexes, marker.Index)
			}
		}
		break
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
	if root == m.mapRoot {
		return nil
	}
	start := -1
	for i := range m.mapUpdates {
		if m.mapUpdates[i].PreviousRoot == root {
			start = i
			break
		}
	}
	if start < 0 {
		return nil
	}
	updates := m.mapUpdates[start:]
	out := make([]protocol.AuthenticatedMapUpdate, len(updates))
	for i := range updates {
		out[i] = cloneAuthenticatedMapUpdate(updates[i])
	}
	return out
}

// DeliveryLogUpdate returns a compact consistency proof from checkpoint to the
// current tenant log and inclusion proofs for only the requested indexes.
func (m *Manager) DeliveryLogUpdate(checkpoint protocol.Checkpoint, indexes ...uint64) (protocol.DeliveryLogUpdate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deliveryLogUpdate(m.deliveryTree, m.deliveries, checkpoint, indexes...)
}

// IdentityTrustProof returns the manager-assembled proof that identityID's
// complete continuity history is the current leaf under the authenticated map.
// The agent can validate it without retaining any per-identity state.
func (m *Manager) IdentityTrustProof(identityID string) (protocol.IdentityTrustProof, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityTrustProofAtLocked(identityID, m.mapRoot)
}

// IdentityTrustProofAt assembles a proof for a verifier that intentionally
// retains an older accepted map root.
func (m *Manager) IdentityTrustProofAt(identityID, root string) (protocol.IdentityTrustProof, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identityTrustProofAtLocked(identityID, root)
}

func (m *Manager) identityTrustProofAtLocked(identityID, root string) (protocol.IdentityTrustProof, error) {
	heads := make(map[string]protocol.KeyHistoryHead)
	var history []protocol.KeyEventRecord
	emptyRoot, err := protocol.KeyHistoryMapRoot(m.tenantID, nil)
	if err != nil {
		return protocol.IdentityTrustProof{}, err
	}
	if root != emptyRoot {
		found := false
		for _, update := range m.mapUpdates {
			head := update.KeyHistory.Head
			heads[head.IdentityID] = head
			if head.IdentityID == identityID {
				history = cloneKeyEventRecords(update.SemanticHistory)
			}
			if update.Root == root {
				found = true
				break
			}
		}
		if !found {
			return protocol.IdentityTrustProof{}, fmt.Errorf("map root %q is not retained", root)
		}
	}
	mapProof, err := protocol.NewKeyHistoryMapProof(m.tenantID, heads, identityID)
	if err != nil {
		return protocol.IdentityTrustProof{}, err
	}
	return protocol.IdentityTrustProof{
		Map:             mapProof,
		History:         history,
		RotationRecords: m.rotationRecordsLocked(history),
	}, nil
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
	state := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            m.tenantID,
		IdentityID:          enrollment.IdentityID,
		Generation:          0,
		ContinuityPublicKey: append([]byte(nil), enrollment.ContinuityPublicKey...),
	}
	stateDigest, err := state.Digest()
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, fmt.Errorf("digest enrolled state: %w", err)
	}
	mapUpdate, head, err := m.prepareKeyHistoryUpdateLocked(enrollment.IdentityID, stateDigest, protocol.KeyEvent{
		Kind:       protocol.KeyEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	m.commitKeyHistoryLocked(enrollment.IdentityID, head, mapUpdate)
	return cloneAuthenticatedMapUpdate(mapUpdate), nil
}

// appendRotationLocked computes the marker and map update before mutating
// either structure, then commits both under the same sequencer lock. A
// production store would use an equivalent transaction or ordered durable
// protocol.
func (m *Manager) appendRotationLocked(rotation protocol.RotationPackage) (RotationCommit, error) {
	_, stateDigest, err := successorState(rotation)
	if err != nil {
		return RotationCommit{}, err
	}
	marker, err := m.newRotationMarkerLocked(rotation)
	if err != nil {
		return RotationCommit{}, fmt.Errorf("build rotation marker: %w", err)
	}
	mapUpdate, head, err := m.prepareKeyHistoryUpdateLocked(rotation.Intent.IdentityID, stateDigest, protocol.KeyEvent{
		Kind:           protocol.KeyEventRotation,
		Rotation:       cloneRotation(&rotation),
		RotationMarker: pointerTo(marker.Reference()),
	})
	if err != nil {
		return RotationCommit{}, err
	}
	// The marker is appended atomically below, so it is not yet discoverable in
	// m.deliveries when the otherwise self-contained semantic proof is built.
	mapUpdate.RotationRecords = append(mapUpdate.RotationRecords, cloneDeliveryRecord(marker))

	if err := m.appendDeliveryRecordLocked(marker); err != nil {
		return RotationCommit{}, fmt.Errorf("append rotation marker: %w", err)
	}
	m.commitKeyHistoryLocked(rotation.Intent.IdentityID, head, mapUpdate)
	return RotationCommit{Marker: cloneDeliveryRecord(marker), MapUpdate: cloneAuthenticatedMapUpdate(mapUpdate)}, nil
}

func (m *Manager) newRotationMarkerLocked(rotation protocol.RotationPackage) (protocol.DeliveryRecord, error) {
	return protocol.NewDeliveryRecord(m.deliveryTree.Size(), protocol.DeliveryLogEvent{
		Kind:     protocol.DeliveryLogEventRotation,
		Rotation: cloneRotation(&rotation),
	})
}

func (m *Manager) appendRotationMapUpdateLocked(rotation protocol.RotationPackage, marker protocol.DeliveryLogReference) (protocol.AuthenticatedMapUpdate, error) {
	_, stateDigest, err := successorState(rotation)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	mapUpdate, head, err := m.prepareKeyHistoryUpdateLocked(rotation.Intent.IdentityID, stateDigest, protocol.KeyEvent{
		Kind:           protocol.KeyEventRotation,
		Rotation:       cloneRotation(&rotation),
		RotationMarker: pointerTo(marker),
	})
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, err
	}
	m.commitKeyHistoryLocked(rotation.Intent.IdentityID, head, mapUpdate)
	return cloneAuthenticatedMapUpdate(mapUpdate), nil
}

func (m *Manager) prepareKeyHistoryUpdateLocked(identityID, stateDigest string, keyEvent protocol.KeyEvent) (protocol.AuthenticatedMapUpdate, protocol.KeyHistoryHead, error) {
	previousHead, ok := m.histories[identityID]
	if !ok {
		previousHead = protocol.EmptyKeyHistoryHead(identityID)
	}
	historyUpdate, err := protocol.NewKeyHistoryUpdate(previousHead, m.historyRecords[identityID], stateDigest, keyEvent)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, protocol.KeyHistoryHead{}, fmt.Errorf("build key-history update: %w", err)
	}
	mapUpdate, err := protocol.NewAuthenticatedMapUpdate(m.tenantID, m.histories, historyUpdate)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, protocol.KeyHistoryHead{}, fmt.Errorf("build authenticated-map update: %w", err)
	}
	if mapUpdate.PreviousRoot != m.mapRoot {
		return protocol.AuthenticatedMapUpdate{}, protocol.KeyHistoryHead{}, fmt.Errorf("authenticated-map state mismatch: proof starts at %q, manager has %q", mapUpdate.PreviousRoot, m.mapRoot)
	}
	mapUpdate.SemanticHistory = append(cloneKeyEventRecords(m.historyRecords[identityID]), cloneKeyEventRecord(historyUpdate.Event))
	mapUpdate.RotationRecords = m.rotationRecordsLocked(mapUpdate.SemanticHistory)
	return mapUpdate, historyUpdate.Head, nil
}

func (m *Manager) commitKeyHistoryLocked(identityID string, head protocol.KeyHistoryHead, update protocol.AuthenticatedMapUpdate) {
	m.mapUpdates = append(m.mapUpdates, cloneAuthenticatedMapUpdate(update))
	m.mapRoot = update.Root
	m.histories[identityID] = head
	m.historyRecords[identityID] = append(m.historyRecords[identityID], cloneKeyEventRecord(update.KeyHistory.Event))
}

func (m *Manager) rotationRecordsLocked(history []protocol.KeyEventRecord) []protocol.DeliveryRecord {
	records := make([]protocol.DeliveryRecord, 0)
	seen := make(map[protocol.DeliveryLogReference]struct{})
	for _, event := range history {
		marker := event.Event.RotationMarker
		if marker == nil {
			continue
		}
		if _, exists := seen[*marker]; exists {
			continue
		}
		if marker.Index >= uint64(len(m.deliveries)) {
			continue
		}
		record := m.deliveries[marker.Index]
		if record.Hash != marker.Hash {
			continue
		}
		records = append(records, cloneDeliveryRecord(record))
		seen[*marker] = struct{}{}
	}
	return records
}

func (m *Manager) appendDelivery(delivery protocol.SignedDelivery) (protocol.DeliveryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := protocol.NewDeliveryRecord(m.deliveryTree.Size(), protocol.DeliveryLogEvent{
		Kind:     protocol.DeliveryLogEventDelivery,
		Delivery: pointerTo(cloneDelivery(delivery)),
	})
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
	record, err := protocol.NewDeliveryRecord(checkpoint.Size, protocol.DeliveryLogEvent{
		Kind:     protocol.DeliveryLogEventDelivery,
		Delivery: pointerTo(cloneDelivery(delivery)),
	})
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
	state := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            enrollment.Intent.TenantID,
		IdentityID:          enrollment.IdentityID,
		ContinuityPublicKey: append([]byte(nil), enrollment.ContinuityPublicKey...),
	}
	stateDigest, err := state.Digest()
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

func successorState(rotation protocol.RotationPackage) (protocol.ContinuityState, string, error) {
	state := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            rotation.Intent.TenantID,
		IdentityID:          rotation.Intent.IdentityID,
		Generation:          rotation.Intent.NewGeneration,
		ContinuityPublicKey: append([]byte(nil), rotation.NewContinuityPublicKey...),
		PreviousStateDigest: rotation.Intent.PreviousStateDigest,
	}
	digest, err := state.Digest()
	if err != nil {
		return protocol.ContinuityState{}, "", fmt.Errorf("digest successor state: %w", err)
	}
	return state, digest, nil
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
	out := *in
	out.NewContinuityPublicKey = append([]byte(nil), in.NewContinuityPublicKey...)
	out.SignatureByOldKey = append([]byte(nil), in.SignatureByOldKey...)
	out.ProofByNewKey = append([]byte(nil), in.ProofByNewKey...)
	return &out
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

func cloneKeyEventRecords(in []protocol.KeyEventRecord) []protocol.KeyEventRecord {
	out := make([]protocol.KeyEventRecord, len(in))
	for i := range in {
		out[i] = cloneKeyEventRecord(in[i])
	}
	return out
}

func cloneAuthenticatedMapUpdate(in protocol.AuthenticatedMapUpdate) protocol.AuthenticatedMapUpdate {
	out := in
	if in.PreviousHead != nil {
		previousHead := *in.PreviousHead
		out.PreviousHead = &previousHead
	}
	out.KeyHistory = *cloneKeyHistoryUpdate(&in.KeyHistory)
	out.SiblingHashes = append([]string(nil), in.SiblingHashes...)
	out.SemanticHistory = cloneKeyEventRecords(in.SemanticHistory)
	out.RotationRecords = make([]protocol.DeliveryRecord, len(in.RotationRecords))
	for i := range in.RotationRecords {
		out.RotationRecords[i] = cloneDeliveryRecord(in.RotationRecords[i])
	}
	return out
}

func cloneDeliveryRecord(in protocol.DeliveryRecord) protocol.DeliveryRecord {
	out := in
	if in.Event.Delivery != nil {
		delivery := cloneDelivery(*in.Event.Delivery)
		out.Event.Delivery = &delivery
	}
	out.Event.Rotation = cloneRotation(in.Event.Rotation)
	return out
}

func pointerTo[T any](value T) *T {
	return &value
}
