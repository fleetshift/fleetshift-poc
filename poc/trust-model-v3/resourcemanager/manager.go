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

type Manager struct {
	mu sync.RWMutex

	tenantID   string
	authorizer Authorizer
	trust      []protocol.TrustRecord
	deliveries []protocol.DeliveryRecord
}

func New(tenantID string, authorizer Authorizer) *Manager {
	if authorizer == nil {
		authorizer = func(AuthorizationRequest) error { return nil }
	}
	return &Manager{tenantID: tenantID, authorizer: authorizer}
}

func (m *Manager) SubmitEnrollment(enrollment protocol.EnrollmentPackage) (protocol.TrustRecord, error) {
	if enrollment.Intent.TenantID != m.tenantID {
		return protocol.TrustRecord{}, fmt.Errorf("enrollment tenant %q does not match manager tenant %q", enrollment.Intent.TenantID, m.tenantID)
	}
	if err := m.authorizer(AuthorizationRequest{TenantID: m.tenantID, Action: ActionEnroll}); err != nil {
		return protocol.TrustRecord{}, fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	return m.appendTrust(protocol.TrustEvent{
		Kind:       protocol.TrustEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
}

func (m *Manager) SubmitRotation(callerID string, rotation protocol.RotationPackage) (protocol.TrustRecord, error) {
	if rotation.Intent.TenantID != m.tenantID || rotation.Intent.IdentityID != callerID {
		return protocol.TrustRecord{}, fmt.Errorf("%w: rotation caller or tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorizer(AuthorizationRequest{
		TenantID: m.tenantID,
		CallerID: callerID,
		Action:   ActionRotate,
	}); err != nil {
		return protocol.TrustRecord{}, fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	return m.appendTrust(protocol.TrustEvent{
		Kind:     protocol.TrustEventRotation,
		Rotation: cloneRotation(&rotation),
	})
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

func (m *Manager) TrustCheckpoint() protocol.Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return trustCheckpoint(m.trust)
}

func (m *Manager) DeliveryCheckpoint() protocol.Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return deliveryCheckpoint(m.deliveries)
}

func (m *Manager) TrustRecordsAfter(checkpoint protocol.Checkpoint) []protocol.TrustRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if checkpoint.Size >= uint64(len(m.trust)) {
		return nil
	}
	records := m.trust[checkpoint.Size:]
	out := make([]protocol.TrustRecord, len(records))
	for i := range records {
		out[i] = cloneTrustRecord(records[i])
	}
	return out
}

func (m *Manager) Compromised() *CompromisedManager {
	return &CompromisedManager{manager: m}
}

func (m *Manager) appendTrust(event protocol.TrustEvent) (protocol.TrustRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := protocol.NewTrustRecord(trustCheckpoint(m.trust), event)
	if err != nil {
		return protocol.TrustRecord{}, fmt.Errorf("append trust record: %w", err)
	}
	m.trust = append(m.trust, record)
	return cloneTrustRecord(record), nil
}

func (m *Manager) appendDelivery(delivery protocol.SignedDelivery) (protocol.DeliveryRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := protocol.NewDeliveryRecord(deliveryCheckpoint(m.deliveries), cloneDelivery(delivery))
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

func (c *CompromisedManager) AppendEnrollment(enrollment protocol.EnrollmentPackage) protocol.TrustRecord {
	record, err := c.manager.appendTrust(protocol.TrustEvent{
		Kind:       protocol.TrustEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
	if err != nil {
		panic(err)
	}
	return record
}

func (c *CompromisedManager) AppendRotation(rotation protocol.RotationPackage) protocol.TrustRecord {
	record, err := c.manager.appendTrust(protocol.TrustEvent{
		Kind:     protocol.TrustEventRotation,
		Rotation: cloneRotation(&rotation),
	})
	if err != nil {
		panic(err)
	}
	return record
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
// reject it because it no longer extends that agent's retained checkpoint.
func (c *CompromisedManager) ForgeDeliveryAt(checkpoint protocol.Checkpoint, delivery protocol.SignedDelivery) protocol.DeliveryRecord {
	record, err := protocol.NewDeliveryRecord(checkpoint, cloneDelivery(delivery))
	if err != nil {
		panic(err)
	}
	return record
}

// ForgeEnrollmentAt constructs a structurally valid trust branch from an
// older checkpoint without changing the manager's main branch.
func (c *CompromisedManager) ForgeEnrollmentAt(checkpoint protocol.Checkpoint, enrollment protocol.EnrollmentPackage) protocol.TrustRecord {
	record, err := protocol.NewTrustRecord(checkpoint, protocol.TrustEvent{
		Kind:       protocol.TrustEventEnrollment,
		Enrollment: cloneEnrollment(&enrollment),
	})
	if err != nil {
		panic(err)
	}
	return record
}

func trustCheckpoint(records []protocol.TrustRecord) protocol.Checkpoint {
	if len(records) == 0 {
		return protocol.EmptyCheckpoint()
	}
	return records[len(records)-1].Checkpoint()
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

func cloneTrustRecord(in protocol.TrustRecord) protocol.TrustRecord {
	out := in
	out.Event.Enrollment = cloneEnrollment(in.Event.Enrollment)
	out.Event.Rotation = cloneRotation(in.Event.Rotation)
	return out
}

func cloneDeliveryRecord(in protocol.DeliveryRecord) protocol.DeliveryRecord {
	out := in
	out.Delivery = cloneDelivery(in.Delivery)
	return out
}
