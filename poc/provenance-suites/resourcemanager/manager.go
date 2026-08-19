// Package resourcemanager models FleetShift's resource-manager role. It
// authenticates callers, performs primary authorization, stores immutable
// TypedEvidence, commits deliveries to an append-only log, and couriers
// evidence to targets. It is not a signing authority for user provenance.
package resourcemanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

const (
	ActionEnroll  = "enroll"
	ActionDeliver = "deliver"
)

var (
	ErrUnauthorized     = errors.New("resource-manager authorization denied")
	ErrAgentUnavailable = errors.New("delivery agent is unavailable")
)

// AuthorizationRequest is the RM's ordinary permission check. Provenance
// does not reproduce this policy at the target.
type AuthorizationRequest struct {
	TenantID protocol.TenantID
	Caller   protocol.Principal
	Action   string
	TargetID string
}

// Authorizer is the platform permission hook.
type Authorizer func(AuthorizationRequest) error

// DeliveryPackage is the couriered mutation a target verifies. Support
// material is replaceable and untrusted until the selected profile checks it.
type DeliveryPackage struct {
	Commitment protocol.DeliveryCommitment
	Evidence   protocol.TypedEvidence
	Support    protocol.SupportMaterial
	Assertion  protocol.TypedAssertion
}

// DeliveryAgent is the manager-side view of a target. A nil error is the
// acknowledgement that the agent durably accepted the work. Enrollment is
// not part of this interface; it is a typed direct-key lifecycle courier.
type DeliveryAgent interface {
	Deliver(pkg DeliveryPackage) error
}

// DirectKeyEnroller is the typed direct-key/v1 enrollment courier. Profiles
// that do not enroll through FleetShift do not implement it.
type DirectKeyEnroller interface {
	AcceptEnrollment(evidence protocol.TypedEvidence) error
}

type agentRoute struct {
	agent DeliveryAgent
}

// Manager is the resource-manager role.
type Manager struct {
	mu sync.Mutex

	tenantID   protocol.TenantID
	authorizer Authorizer
	profile    *directkey.Manager
	log        []protocol.DeliveryCommitment
	assertions map[uint64]protocol.TypedAssertion
	agents     map[string]*agentRoute
	enrollers  []DirectKeyEnroller
}

// New constructs a manager for one FleetShift tenant.
func New(tenantID protocol.TenantID, authorizer Authorizer) *Manager {
	if authorizer == nil {
		authorizer = func(AuthorizationRequest) error { return nil }
	}
	return &Manager{
		tenantID:   tenantID,
		authorizer: authorizer,
		profile:    directkey.NewManager(),
		assertions: make(map[uint64]protocol.TypedAssertion),
		agents:     make(map[string]*agentRoute),
	}
}

// RegisterAgent installs the delivery route for one target.
func (m *Manager) RegisterAgent(targetID string, agent DeliveryAgent) error {
	if targetID == "" || agent == nil {
		return errors.New("target ID and delivery agent are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.agents[targetID]; exists {
		return fmt.Errorf("delivery agent for target %q is already registered", targetID)
	}
	m.agents[targetID] = &agentRoute{agent: agent}
	return nil
}

// RegisterDirectKeyEnroller installs the typed direct-key enrollment courier.
func (m *Manager) RegisterDirectKeyEnroller(enroller DirectKeyEnroller) error {
	if enroller == nil {
		return errors.New("direct-key enroller is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enrollers = append(m.enrollers, enroller)
	return nil
}

// SubmitDirectKeyEnrollment is the typed direct-key/v1 lifecycle API. It is
// not a generic RegisterKey.
func (m *Manager) SubmitDirectKeyEnrollment(ctx context.Context, caller protocol.Principal, evidence protocol.TypedEvidence) error {
	if err := m.authorize(caller, ActionEnroll, ""); err != nil {
		return err
	}
	hints, err := directkey.ParseHints(evidence)
	if err != nil {
		return err
	}
	if caller.Scheme != hints.Scheme || caller.Authority != hints.Authority || caller.Subject != hints.Subject {
		return fmt.Errorf("%w: enrollment principal does not match caller", ErrUnauthorized)
	}
	if err := m.profile.CommitEnrollment(ctx, evidence); err != nil {
		return err
	}
	return m.pushEnrollment(evidence)
}

// SubmitDelivery authorizes the caller, stores evidence, appends a log
// commitment, and pushes the package to the named target.
func (m *Manager) SubmitDelivery(ctx context.Context, caller protocol.Principal, evidence protocol.TypedEvidence, assertion protocol.TypedAssertion) (protocol.DeliveryCommitment, error) {
	authorization, err := decodeAuthorization(assertion)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if authorization.TenantID != m.tenantID {
		return protocol.DeliveryCommitment{}, fmt.Errorf("%w: delivery tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorize(caller, ActionDeliver, authorization.TargetID); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	hints, err := directkey.ParseHints(evidence)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if caller.Scheme != hints.Scheme || caller.Authority != hints.Authority || caller.Subject != hints.Subject {
		return protocol.DeliveryCommitment{}, fmt.Errorf("%w: delivery principal does not match caller", ErrUnauthorized)
	}
	if err := m.profile.CheckDelivery(evidence, assertion); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if err := m.store(ctx, evidence); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	support, err := m.assemble(ctx, evidence)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	commitment, err := m.appendCommitment(authorization, evidence, assertion)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	pkg := DeliveryPackage{Commitment: commitment, Evidence: evidence, Support: support, Assertion: assertion}
	if err := m.pushDelivery(pkg); err != nil {
		return commitment, fmt.Errorf("push delivery to target %q: %w", authorization.TargetID, err)
	}
	return commitment, nil
}

// RetryDelivery pushes an already committed delivery without appending or
// repeating the original caller authorization decision.
func (m *Manager) RetryDelivery(ctx context.Context, index uint64) error {
	m.mu.Lock()
	if index >= uint64(len(m.log)) {
		size := len(m.log)
		m.mu.Unlock()
		return fmt.Errorf("delivery-log index %d is beyond size %d", index, size)
	}
	commitment := m.log[index]
	m.mu.Unlock()

	evidence, ok := m.profile.Evidence(mustEvidenceIdentity(commitment))
	if !ok {
		return fmt.Errorf("committed evidence for index %d is missing", index)
	}
	m.mu.Lock()
	assertion, ok := m.assertions[index]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("committed assertion for index %d is missing", index)
	}
	support, err := m.assemble(ctx, evidence)
	if err != nil {
		return err
	}
	return m.pushDelivery(DeliveryPackage{
		Commitment: commitment,
		Evidence:   evidence,
		Support:    support,
		Assertion:  assertion,
	})
}

func (m *Manager) authorize(caller protocol.Principal, action, targetID string) error {
	if err := m.authorizer(AuthorizationRequest{
		TenantID: m.tenantID,
		Caller:   caller,
		Action:   action,
		TargetID: targetID,
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	return nil
}

func (m *Manager) appendCommitment(authorization protocol.DeliveryAuthorization, evidence protocol.TypedEvidence, assertion protocol.TypedAssertion) (protocol.DeliveryCommitment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	commitment := protocol.DeliveryCommitment{
		Index:         uint64(len(m.log)),
		TargetID:      authorization.TargetID,
		FulfillmentID: authorization.FulfillmentID,
		Generation:    authorization.Generation,
		ContentType:   protocol.ContentTypeDeliveryAuthorizationV1,
		Evidence:      []protocol.TypedEvidence{cloneEvidence(evidence)},
	}
	m.log = append(m.log, commitment)
	m.assertions[commitment.Index] = protocol.TypedAssertion{
		ContentType: assertion.ContentType,
		Bytes:       append([]byte(nil), assertion.Bytes...),
	}
	return commitment, nil
}

func (m *Manager) pushEnrollment(evidence protocol.TypedEvidence) error {
	m.mu.Lock()
	enrollers := append([]DirectKeyEnroller(nil), m.enrollers...)
	m.mu.Unlock()
	if len(enrollers) == 0 {
		return fmt.Errorf("%w: no registered direct-key enrollers", ErrAgentUnavailable)
	}
	for _, enroller := range enrollers {
		if err := enroller.AcceptEnrollment(cloneEvidence(evidence)); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) pushDelivery(pkg DeliveryPackage) error {
	m.mu.Lock()
	route, ok := m.agents[pkg.Commitment.TargetID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w for target %q", ErrAgentUnavailable, pkg.Commitment.TargetID)
	}
	return route.agent.Deliver(pkg)
}

// DeliveryLogSize is the number of appended commitments.
func (m *Manager) DeliveryLogSize() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint64(len(m.log))
}

// Compromised returns an explicit attack harness.
func (m *Manager) Compromised() *CompromisedManager {
	return &CompromisedManager{manager: m}
}

// CompromisedManager exposes what arbitrary code running as the resource
// manager can store and route while still lacking the user's private key.
type CompromisedManager struct {
	manager *Manager
}

// CommitEnrollment stores and couriers enrollment without caller authorization.
func (c *CompromisedManager) CommitEnrollment(ctx context.Context, evidence protocol.TypedEvidence) error {
	if err := c.manager.profile.CommitEnrollment(ctx, evidence); err != nil {
		return err
	}
	return c.manager.pushEnrollment(evidence)
}

// PushDelivery stores and routes a delivery without authorization or the
// RM's own provenance check.
func (c *CompromisedManager) PushDelivery(ctx context.Context, evidence protocol.TypedEvidence, assertion protocol.TypedAssertion) (protocol.DeliveryCommitment, error) {
	authorization, err := decodeAuthorization(assertion)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if err := c.manager.store(ctx, evidence); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	support, err := c.manager.assemble(ctx, evidence)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	commitment, err := c.manager.appendCommitment(authorization, evidence, assertion)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	pkg := DeliveryPackage{Commitment: commitment, Evidence: evidence, Support: support, Assertion: assertion}
	if err := c.manager.pushDelivery(pkg); err != nil {
		return commitment, err
	}
	return commitment, nil
}

func (m *Manager) courier(pt protocol.ProvenanceType) (protocol.ResourceManagerAPI, error) {
	if m.profile.ProvenanceType() != pt {
		return nil, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, pt)
	}
	return m.profile, nil
}

func (m *Manager) store(ctx context.Context, evidence protocol.TypedEvidence) error {
	courier, err := m.courier(evidence.ProvenanceType)
	if err != nil {
		return err
	}
	_, err = courier.StoreEvidence(ctx, evidence)
	return err
}

func (m *Manager) assemble(ctx context.Context, evidence protocol.TypedEvidence) (protocol.SupportMaterial, error) {
	courier, err := m.courier(evidence.ProvenanceType)
	if err != nil {
		return protocol.SupportMaterial{}, err
	}
	return courier.AssembleSupportMaterial(ctx, evidence)
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

func cloneEvidence(in protocol.TypedEvidence) protocol.TypedEvidence {
	out := in
	out.Bytes = append([]byte(nil), in.Bytes...)
	return out
}

func mustEvidenceIdentity(commitment protocol.DeliveryCommitment) protocol.Digest {
	if len(commitment.Evidence) == 0 {
		return ""
	}
	identity, err := commitment.Evidence[0].Identity()
	if err != nil {
		return ""
	}
	return identity
}
