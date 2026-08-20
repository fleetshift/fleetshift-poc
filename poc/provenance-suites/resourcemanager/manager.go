// Package resourcemanager models FleetShift's resource-manager role. It
// authenticates callers, performs primary authorization, stores immutable
// TypedEvidence, commits deliveries to an append-only log, and couriers
// evidence to targets. It is not a signing authority for user provenance.
package resourcemanager

import (
	"context"
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

// DeliveryPackage is the couriered mutation a target verifies. Root and
// supporting items are the same kind of object: one independently
// authenticated assertion plus replaceable support for that evidence.
// The inner assertion is extracted from each statement's evidence by the
// selected profile.
type DeliveryPackage struct {
	Commitment protocol.DeliveryCommitment
	Root       protocol.SignedStatement
	Supporting []protocol.SignedStatement
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
// commitment, and pushes the package to the named target. Optional
// supporting evidence is couriered with the root and is independently
// authenticated. Routing identity comes from DecodeAssertion then
// DecodeDeliveryScope; the RM does not parse evidence bytes itself.
func (m *Manager) SubmitDelivery(ctx context.Context, caller protocol.Principal, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (protocol.DeliveryCommitment, error) {
	courier, err := m.courier(evidence.ProvenanceType)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	assertion, err := courier.DecodeAssertion(evidence)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if scope.TenantID != m.tenantID {
		return protocol.DeliveryCommitment{}, fmt.Errorf("%w: delivery tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorize(caller, ActionDeliver, scope.TargetID); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	hints, err := courier.CheckDelivery(evidence)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if !callerMatches(caller, hints) {
		return protocol.DeliveryCommitment{}, fmt.Errorf("%w: delivery principal does not match caller", ErrUnauthorized)
	}
	for _, item := range supporting {
		itemCourier, err := m.courier(item.ProvenanceType)
		if err != nil {
			return protocol.DeliveryCommitment{}, err
		}
		if _, err := itemCourier.CheckDelivery(item); err != nil {
			return protocol.DeliveryCommitment{}, err
		}
	}
	if err := m.store(ctx, evidence); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	for _, item := range supporting {
		if err := m.store(ctx, item); err != nil {
			return protocol.DeliveryCommitment{}, err
		}
	}
	commitment, err := m.appendCommitment(scope, assertion.PredicateType, evidence, supporting)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	pkg, err := m.deliveryPackage(ctx, commitment, evidence, supporting)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if err := m.pushDelivery(pkg); err != nil {
		return commitment, fmt.Errorf("push delivery to target %q: %w", scope.TargetID, err)
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

	if len(commitment.Evidence) == 0 {
		return fmt.Errorf("committed evidence for index %d is missing", index)
	}
	evidence := cloneEvidence(commitment.Evidence[0])
	var supporting []protocol.TypedEvidence
	for i := 1; i < len(commitment.Evidence); i++ {
		supporting = append(supporting, cloneEvidence(commitment.Evidence[i]))
	}
	pkg, err := m.deliveryPackage(ctx, commitment, evidence, supporting)
	if err != nil {
		return err
	}
	return m.pushDelivery(pkg)
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

func (m *Manager) appendCommitment(scope protocol.DeliveryScope, predicateType protocol.PredicateType, evidence protocol.TypedEvidence, supporting []protocol.TypedEvidence) (protocol.DeliveryCommitment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := []protocol.TypedEvidence{cloneEvidence(evidence)}
	for _, item := range supporting {
		items = append(items, cloneEvidence(item))
	}
	commitment := protocol.DeliveryCommitment{
		Index:         uint64(len(m.log)),
		TargetID:      scope.TargetID,
		FulfillmentID: scope.FulfillmentID,
		Generation:    scope.Generation,
		PredicateType: predicateType,
		Evidence:      items,
	}
	m.log = append(m.log, commitment)
	return commitment, nil
}

func (m *Manager) deliveryPackage(ctx context.Context, commitment protocol.DeliveryCommitment, evidence protocol.TypedEvidence, supporting []protocol.TypedEvidence) (DeliveryPackage, error) {
	root, err := m.assembleStatement(ctx, evidence)
	if err != nil {
		return DeliveryPackage{}, err
	}
	out := make([]protocol.SignedStatement, 0, len(supporting))
	for _, item := range supporting {
		stmt, err := m.assembleStatement(ctx, item)
		if err != nil {
			return DeliveryPackage{}, err
		}
		out = append(out, stmt)
	}
	return DeliveryPackage{
		Commitment: commitment,
		Root:       root,
		Supporting: out,
	}, nil
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
func (c *CompromisedManager) PushDelivery(ctx context.Context, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (protocol.DeliveryCommitment, error) {
	courier, err := c.manager.courier(evidence.ProvenanceType)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	assertion, err := courier.DecodeAssertion(evidence)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	if err := c.manager.store(ctx, evidence); err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	for _, item := range supporting {
		if err := c.manager.store(ctx, item); err != nil {
			return protocol.DeliveryCommitment{}, err
		}
	}
	commitment, err := c.manager.appendCommitment(scope, assertion.PredicateType, evidence, supporting)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
	pkg, err := c.manager.deliveryPackage(ctx, commitment, evidence, supporting)
	if err != nil {
		return protocol.DeliveryCommitment{}, err
	}
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

func (m *Manager) assembleStatement(ctx context.Context, evidence protocol.TypedEvidence) (protocol.SignedStatement, error) {
	support, err := m.assemble(ctx, evidence)
	if err != nil {
		return protocol.SignedStatement{}, err
	}
	return protocol.SignedStatement{
		Evidence: cloneEvidence(evidence),
		Support:  cloneSupport(support),
	}, nil
}

func cloneEvidence(in protocol.TypedEvidence) protocol.TypedEvidence {
	in.Encoded = in.Encoded.Clone()
	return in
}

func cloneSupport(in protocol.SupportMaterial) protocol.SupportMaterial {
	return protocol.SupportMaterial(protocol.Encoded(in).Clone())
}

func callerMatches(caller protocol.Principal, hints protocol.TentativeHints) bool {
	return caller.Scheme == hints.Scheme && caller.Authority == hints.Authority && caller.Subject == hints.Subject
}
