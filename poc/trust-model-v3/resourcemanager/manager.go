// Package resourcemanager models FleetShift's central resource-manager role.
// It performs ordinary authorization, stores ordered records, and couriers
// evidence, but it is not a signing authority for user provenance.
package resourcemanager

import (
	"errors"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
)

const (
	ActionEnroll  = "enroll"
	ActionRotate  = "rotate"
	ActionDeliver = "deliver"
)

var ErrUnauthorized = errors.New("resource-manager authorization denied")

type AuthorizationRequest struct {
	TenantID string
	CallerID string
	Action   string
	TargetID string
}

type Authorizer func(AuthorizationRequest) error

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

	mapRoot    string
	mapUpdates []protocol.AuthenticatedMapUpdate
	histories  map[string]protocol.KeyHistoryHead
	states     map[string]protocol.ContinuityState
	deliveries []protocol.DeliveryRecord
}

func New(tenantID string, authorizer Authorizer) *Manager {
	if authorizer == nil {
		authorizer = func(AuthorizationRequest) error { return nil }
	}
	emptyMapRoot, err := protocol.KeyHistoryMapRoot(nil)
	if err != nil {
		panic(err) // The fixed map representation is always JSON encodable.
	}
	return &Manager{
		tenantID:   tenantID,
		authorizer: authorizer,
		mapRoot:    emptyMapRoot,
		histories:  make(map[string]protocol.KeyHistoryHead),
		states:     make(map[string]protocol.ContinuityState),
	}
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
	return m.appendDelivery(delivery)
}

func (m *Manager) DeliveryCheckpoint() protocol.Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deliveryCheckpoint(m.deliveries)
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

func (m *Manager) DeliveryRecordsAfter(checkpoint protocol.Checkpoint) []protocol.DeliveryRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if checkpoint.Size >= uint64(len(m.deliveries)) {
		return nil
	}
	records := m.deliveries[checkpoint.Size:]
	out := make([]protocol.DeliveryRecord, len(records))
	for i := range records {
		out[i] = cloneDeliveryRecord(records[i])
	}
	return out
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
	m.commitKeyHistoryLocked(enrollment.IdentityID, head, state, mapUpdate)
	return cloneAuthenticatedMapUpdate(mapUpdate), nil
}

// appendRotationLocked computes the marker and map update before mutating
// either structure, then commits both under the same sequencer lock. A
// production store would use an equivalent transaction or ordered durable
// protocol.
func (m *Manager) appendRotationLocked(rotation protocol.RotationPackage) (RotationCommit, error) {
	state, stateDigest, err := successorState(rotation)
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

	m.deliveries = append(m.deliveries, marker)
	m.commitKeyHistoryLocked(rotation.Intent.IdentityID, head, state, mapUpdate)
	return RotationCommit{Marker: cloneDeliveryRecord(marker), MapUpdate: cloneAuthenticatedMapUpdate(mapUpdate)}, nil
}

func (m *Manager) newRotationMarkerLocked(rotation protocol.RotationPackage) (protocol.DeliveryRecord, error) {
	return protocol.NewDeliveryRecord(deliveryCheckpoint(m.deliveries), protocol.DeliveryLogEvent{
		Kind:     protocol.DeliveryLogEventRotation,
		Rotation: cloneRotation(&rotation),
	})
}

func (m *Manager) appendRotationMapUpdateLocked(rotation protocol.RotationPackage, marker protocol.DeliveryLogReference) (protocol.AuthenticatedMapUpdate, error) {
	state, stateDigest, err := successorState(rotation)
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
	m.commitKeyHistoryLocked(rotation.Intent.IdentityID, head, state, mapUpdate)
	return cloneAuthenticatedMapUpdate(mapUpdate), nil
}

func (m *Manager) prepareKeyHistoryUpdateLocked(identityID, stateDigest string, keyEvent protocol.KeyEvent) (protocol.AuthenticatedMapUpdate, protocol.KeyHistoryHead, error) {
	previousHead, ok := m.histories[identityID]
	if !ok {
		previousHead = protocol.EmptyKeyHistoryHead(identityID)
	}
	historyUpdate, err := protocol.NewKeyHistoryUpdate(previousHead, stateDigest, keyEvent)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, protocol.KeyHistoryHead{}, fmt.Errorf("build key-history update: %w", err)
	}
	mapUpdate, err := protocol.NewAuthenticatedMapUpdate(m.histories, historyUpdate)
	if err != nil {
		return protocol.AuthenticatedMapUpdate{}, protocol.KeyHistoryHead{}, fmt.Errorf("build authenticated-map update: %w", err)
	}
	if mapUpdate.PreviousRoot != m.mapRoot {
		return protocol.AuthenticatedMapUpdate{}, protocol.KeyHistoryHead{}, fmt.Errorf("authenticated-map state mismatch: proof starts at %q, manager has %q", mapUpdate.PreviousRoot, m.mapRoot)
	}
	return mapUpdate, historyUpdate.Head, nil
}

func (m *Manager) commitKeyHistoryLocked(identityID string, head protocol.KeyHistoryHead, state protocol.ContinuityState, update protocol.AuthenticatedMapUpdate) {
	m.mapUpdates = append(m.mapUpdates, cloneAuthenticatedMapUpdate(update))
	m.mapRoot = update.Root
	m.histories[identityID] = head
	m.states[identityID] = cloneState(state)
}

func (m *Manager) appendDelivery(delivery protocol.SignedDelivery) (protocol.DeliveryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := protocol.NewDeliveryRecord(deliveryCheckpoint(m.deliveries), protocol.DeliveryLogEvent{
		Kind:     protocol.DeliveryLogEventDelivery,
		Delivery: pointerTo(cloneDelivery(delivery)),
	})
	if err != nil {
		return protocol.DeliveryRecord{}, fmt.Errorf("append delivery record: %w", err)
	}
	m.deliveries = append(m.deliveries, record)
	return cloneDeliveryRecord(record), nil
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
	c.manager.deliveries = append(c.manager.deliveries, marker)
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
func (c *CompromisedManager) ForgeDeliveryAt(checkpoint protocol.Checkpoint, delivery protocol.SignedDelivery) protocol.DeliveryRecord {
	record, err := protocol.NewDeliveryRecord(checkpoint, protocol.DeliveryLogEvent{
		Kind:     protocol.DeliveryLogEventDelivery,
		Delivery: pointerTo(cloneDelivery(delivery)),
	})
	if err != nil {
		panic(err)
	}
	return record
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
	update, err := protocol.NewKeyHistoryUpdate(protocol.EmptyKeyHistoryHead(enrollment.IdentityID), stateDigest, protocol.KeyEvent{
		Kind:       protocol.KeyEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
	if err != nil {
		panic(err)
	}
	mapUpdate, err := protocol.NewAuthenticatedMapUpdate(nil, update)
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

func deliveryCheckpoint(records []protocol.DeliveryRecord) protocol.Checkpoint {
	if len(records) == 0 {
		return protocol.EmptyCheckpoint()
	}
	return records[len(records)-1].Checkpoint()
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

func cloneState(in protocol.ContinuityState) protocol.ContinuityState {
	out := in
	out.ContinuityPublicKey = append([]byte(nil), in.ContinuityPublicKey...)
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
	return &out
}

func cloneAuthenticatedMapUpdate(in protocol.AuthenticatedMapUpdate) protocol.AuthenticatedMapUpdate {
	out := in
	if in.PreviousHead != nil {
		previousHead := *in.PreviousHead
		out.PreviousHead = &previousHead
	}
	out.KeyHistory = *cloneKeyHistoryUpdate(&in.KeyHistory)
	out.SiblingHashes = append([]string(nil), in.SiblingHashes...)
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
