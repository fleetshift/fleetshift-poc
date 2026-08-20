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
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
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
// selected profile. Log binds Root.Evidence's identity to a Merkle-log
// position; it does not authorize content.
type DeliveryPackage struct {
	Log        protocol.LogUpdate
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

// staleCheckpointError is returned by an agent when a request was constructed
// from an older manager-side checkpoint than the agent has already retained.
// Keeping this as a behavioral interface avoids coupling the manager to one
// in-process delivery-agent implementation.
type staleCheckpointError interface {
	error
	LatestCheckpoint() protocol.Checkpoint
}

type storedDelivery struct {
	TargetID   string
	Leaf       protocol.Digest
	Evidence   protocol.TypedEvidence
	Supporting []protocol.TypedEvidence
}

type agentRoute struct {
	mu sync.Mutex

	agent      DeliveryAgent
	checkpoint protocol.Checkpoint
}

// Manager is the resource-manager role.
type Manager struct {
	mu sync.Mutex

	tenantID   protocol.TenantID
	authorizer Authorizer
	profile    *directkey.Manager
	tree       *merklelog.Tree
	deliveries []storedDelivery
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
		tree:       merklelog.New(),
		agents:     make(map[string]*agentRoute),
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

// SubmitDelivery authorizes the caller, stores evidence, appends a log leaf,
// and pushes the package to the named target. Optional supporting evidence is
// couriered with the root and is independently authenticated. Routing identity
// comes from DecodeAssertion then DecodeDeliveryScope; the RM does not parse
// evidence bytes itself.
func (m *Manager) SubmitDelivery(ctx context.Context, caller protocol.Principal, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (protocol.LogUpdate, error) {
	courier, err := m.courier(evidence.ProvenanceType)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	assertion, err := courier.DecodeAssertion(evidence)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	if scope.TenantID != m.tenantID {
		return protocol.LogUpdate{}, fmt.Errorf("%w: delivery tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorize(caller, ActionDeliver, scope.TargetID); err != nil {
		return protocol.LogUpdate{}, err
	}
	hints, err := courier.CheckDelivery(evidence)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	if !callerMatches(caller, hints) {
		return protocol.LogUpdate{}, fmt.Errorf("%w: delivery principal does not match caller", ErrUnauthorized)
	}
	for _, item := range supporting {
		itemCourier, err := m.courier(item.ProvenanceType)
		if err != nil {
			return protocol.LogUpdate{}, err
		}
		if _, err := itemCourier.CheckDelivery(item); err != nil {
			return protocol.LogUpdate{}, err
		}
	}
	if err := m.store(ctx, evidence); err != nil {
		return protocol.LogUpdate{}, err
	}
	for _, item := range supporting {
		if err := m.store(ctx, item); err != nil {
			return protocol.LogUpdate{}, err
		}
	}
	index, err := m.appendDelivery(scope.TargetID, evidence, supporting)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	update, err := m.pushDelivery(ctx, index)
	if err != nil {
		return update, fmt.Errorf("push delivery to target %q: %w", scope.TargetID, err)
	}
	return update, nil
}

// RetryDelivery pushes an already committed delivery without appending or
// repeating the original caller authorization decision.
func (m *Manager) RetryDelivery(ctx context.Context, index uint64) error {
	if _, err := m.pushDelivery(ctx, index); err != nil {
		return fmt.Errorf("retry delivery-log index %d: %w", index, err)
	}
	return nil
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

func (m *Manager) appendDelivery(targetID string, evidence protocol.TypedEvidence, supporting []protocol.TypedEvidence) (uint64, error) {
	identity, err := evidence.Identity()
	if err != nil {
		return 0, err
	}
	leafHash, err := protocol.LeafHash(identity)
	if err != nil {
		return 0, err
	}
	items := make([]protocol.TypedEvidence, 0, len(supporting))
	for _, item := range supporting {
		items = append(items, cloneEvidence(item))
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	index, _, err := m.tree.AppendHash(leafHash)
	if err != nil {
		return 0, err
	}
	m.deliveries = append(m.deliveries, storedDelivery{
		TargetID:   targetID,
		Leaf:       identity,
		Evidence:   cloneEvidence(evidence),
		Supporting: items,
	})
	return index, nil
}

func (m *Manager) deliveryPackage(ctx context.Context, update protocol.LogUpdate, stored storedDelivery) (DeliveryPackage, error) {
	root, err := m.assembleStatement(ctx, stored.Evidence)
	if err != nil {
		return DeliveryPackage{}, err
	}
	out := make([]protocol.SignedStatement, 0, len(stored.Supporting))
	for _, item := range stored.Supporting {
		stmt, err := m.assembleStatement(ctx, item)
		if err != nil {
			return DeliveryPackage{}, err
		}
		out = append(out, stmt)
	}
	return DeliveryPackage{
		Log:        update,
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

func (m *Manager) pushDelivery(ctx context.Context, index uint64) (protocol.LogUpdate, error) {
	m.mu.Lock()
	if index >= uint64(len(m.deliveries)) {
		size := len(m.deliveries)
		m.mu.Unlock()
		return protocol.LogUpdate{}, fmt.Errorf("delivery-log index %d is beyond size %d", index, size)
	}
	stored := cloneStored(m.deliveries[index])
	route, ok := m.agents[stored.TargetID]
	m.mu.Unlock()
	if !ok {
		return protocol.LogUpdate{}, fmt.Errorf("%w for target %q", ErrAgentUnavailable, stored.TargetID)
	}

	// The target delivery contract permits only one in-flight delivery per
	// fulfillment. Serializing this POC's target route also ensures checkpoint
	// construction and acknowledgement recording cannot race one another.
	route.mu.Lock()
	defer route.mu.Unlock()
	var last protocol.LogUpdate
	for {
		m.mu.Lock()
		update, err := m.logUpdateLocked(route.checkpoint, index)
		m.mu.Unlock()
		if err != nil {
			return last, fmt.Errorf("construct proof from acknowledged agent checkpoint: %w", err)
		}
		last = update
		pkg, err := m.deliveryPackage(ctx, update, stored)
		if err != nil {
			return update, err
		}
		if err := route.agent.Deliver(pkg); err != nil {
			var stale staleCheckpointError
			if !errors.As(err, &stale) {
				return update, err
			}
			latest := stale.LatestCheckpoint()
			if latest.Size <= route.checkpoint.Size {
				return update, err
			}
			if err := m.validateAgentCheckpoint(latest); err != nil {
				return update, fmt.Errorf("agent reported an invalid newer checkpoint: %w", err)
			}
			route.checkpoint = latest
			continue
		}

		// A successful call is the acknowledgement. The manager records exactly
		// the checkpoint whose consistency and inclusion proofs were delivered.
		route.checkpoint = update.Checkpoint
		return update, nil
	}
}

func (m *Manager) logUpdateLocked(from protocol.Checkpoint, index uint64) (protocol.LogUpdate, error) {
	if from.Size > m.tree.Size() {
		return protocol.LogUpdate{}, fmt.Errorf("checkpoint size %d is beyond log size %d", from.Size, m.tree.Size())
	}
	wantPrevious, err := m.checkpointAtLocked(from.Size)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	if from != wantPrevious {
		return protocol.LogUpdate{}, errors.New("checkpoint is not on the retained delivery-log branch")
	}
	current, err := m.checkpointAtLocked(m.tree.Size())
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	consistency, err := m.tree.ConsistencyProof(from.Size, current.Size)
	if err != nil {
		return protocol.LogUpdate{}, fmt.Errorf("construct delivery-log consistency proof: %w", err)
	}
	if index >= uint64(len(m.deliveries)) || index >= current.Size {
		return protocol.LogUpdate{}, fmt.Errorf("delivery-log index %d is beyond size %d", index, current.Size)
	}
	inclusion, err := m.tree.InclusionProof(index, current.Size)
	if err != nil {
		return protocol.LogUpdate{}, fmt.Errorf("construct inclusion proof for index %d: %w", index, err)
	}
	return protocol.LogUpdate{
		From:             from,
		Checkpoint:       current,
		ConsistencyProof: protocol.EncodeProof(consistency),
		Index:            index,
		Leaf:             m.deliveries[index].Leaf,
		InclusionProof:   protocol.EncodeProof(inclusion),
	}, nil
}

func (m *Manager) validateAgentCheckpoint(checkpoint protocol.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	want, err := m.checkpointAtLocked(checkpoint.Size)
	if err != nil {
		return err
	}
	if checkpoint != want {
		return errors.New("checkpoint is not on the retained delivery-log branch")
	}
	return nil
}

func (m *Manager) checkpointAtLocked(size uint64) (protocol.Checkpoint, error) {
	root, err := m.tree.RootAt(size)
	if err != nil {
		return protocol.Checkpoint{}, err
	}
	return protocol.NewCheckpoint(size, root)
}

// DeliveryLogSize is the number of appended leaves.
func (m *Manager) DeliveryLogSize() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tree.Size()
}

// AgentCheckpoint is the last acknowledged log checkpoint cached for a target.
// The cache is not authoritative: loss or corruption only affects availability.
func (m *Manager) AgentCheckpoint(targetID string) (protocol.Checkpoint, bool) {
	m.mu.Lock()
	route, ok := m.agents[targetID]
	m.mu.Unlock()
	if !ok {
		return protocol.Checkpoint{}, false
	}
	route.mu.Lock()
	defer route.mu.Unlock()
	return route.checkpoint, true
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
func (c *CompromisedManager) PushDelivery(ctx context.Context, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (protocol.LogUpdate, error) {
	courier, err := c.manager.courier(evidence.ProvenanceType)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	assertion, err := courier.DecodeAssertion(evidence)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	if err := c.manager.store(ctx, evidence); err != nil {
		return protocol.LogUpdate{}, err
	}
	for _, item := range supporting {
		if err := c.manager.store(ctx, item); err != nil {
			return protocol.LogUpdate{}, err
		}
	}
	index, err := c.manager.appendDelivery(scope.TargetID, evidence, supporting)
	if err != nil {
		return protocol.LogUpdate{}, err
	}
	update, err := c.manager.pushDelivery(ctx, index)
	if err != nil {
		return update, err
	}
	return update, nil
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

func cloneStored(in storedDelivery) storedDelivery {
	out := in
	out.Evidence = cloneEvidence(in.Evidence)
	if len(in.Supporting) == 0 {
		out.Supporting = nil
		return out
	}
	out.Supporting = make([]protocol.TypedEvidence, len(in.Supporting))
	for i, item := range in.Supporting {
		out.Supporting[i] = cloneEvidence(item)
	}
	return out
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
