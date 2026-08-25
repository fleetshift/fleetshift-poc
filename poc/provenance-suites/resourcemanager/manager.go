// Package resourcemanager models FleetShift's resource-manager role. It
// authenticates callers, performs primary authorization, stores immutable
// TypedEvidence, assigns evidence-log positions at acceptance, and couriers
// evidence to targets. Delivery and outbox records are a separate dispatch
// identity from those log positions. It is not a signing authority for user
// provenance.
package resourcemanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

var _ protocol.ResourceManagerAPI = (*directkey.Manager)(nil)

const (
	ActionEnroll  = "enroll"
	ActionDeliver = "deliver"
)

var (
	ErrUnauthorized      = errors.New("resource-manager authorization denied")
	ErrAgentUnavailable  = errors.New("delivery agent is unavailable")
	ErrUnknownDispatch   = errors.New("unknown dispatch")
	ErrEvidenceCollision = errors.New("typed evidence identity collision")
	errUnknownDelivery   = errors.New("unknown delivery")
	errUnknownEvidence   = errors.New("unknown evidence identity")
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

// DeliveryID identifies one accepted delivery. It is not an evidence-log
// index and is not a retry handle.
type DeliveryID uint64

// DispatchID identifies one outbox record for a delivery to one target.
type DispatchID uint64

// DeliveryReceipt is returned when the RM has durably accepted evidence,
// assigned any new evidence-log positions, and enqueued outbox work.
type DeliveryReceipt struct {
	DeliveryID  DeliveryID
	DispatchIDs []DispatchID
}

// StoredDelivery is the test-observable delivery record: evidence identities,
// not duplicate bytes or reconstructable proofs.
type StoredDelivery struct {
	ID         DeliveryID
	Root       protocol.Digest
	Supporting []protocol.Digest
}

// DeliveryPackage is the couriered mutation a target verifies. Root and
// supporting items are the same kind of object: one independently
// authenticated assertion plus replaceable support for that evidence.
// The inner assertion is extracted from each statement's evidence by the
// selected profile. EvidenceLog binds Root.Evidence's identity to the
// position assigned when that evidence was accepted; it does not authorize
// content.
type DeliveryPackage struct {
	EvidenceLog protocol.EvidenceLogUpdate
	Root        protocol.SignedStatement
	Supporting  []protocol.SignedStatement
}

// DeliveryAgent is the manager-side view of a target. A nil error is the
// acknowledgement that the agent durably accepted the work. Suite-owned
// events such as enrollment are ordinary Deliver packages.
type DeliveryAgent interface {
	Deliver(pkg DeliveryPackage) error
}

// staleCheckpointError is returned by an agent when a request was constructed
// from an older manager-side checkpoint than the agent has already retained.
// Keeping this as a behavioral interface avoids coupling the manager to one
// in-process delivery-agent implementation.
type staleCheckpointError interface {
	error
	LatestCheckpoint() protocol.Checkpoint
}

type dispatchState int

const (
	dispatchPending dispatchState = iota
	dispatchAcknowledged
)

type storedDelivery struct {
	ID         DeliveryID
	Root       protocol.Digest
	Supporting []protocol.Digest
}

type storedDispatch struct {
	ID         DispatchID
	DeliveryID DeliveryID
	TargetID   string
	State      dispatchState
}

type plannedEvidence struct {
	evidence protocol.TypedEvidence
	identity protocol.Digest
	leafHash []byte
	index    uint64
	isNew    bool
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

	evidenceByID         map[protocol.Digest]protocol.TypedEvidence
	logIndexByEvidenceID map[protocol.Digest]uint64
	deliveries           map[DeliveryID]storedDelivery
	dispatches           map[DispatchID]storedDispatch
	nextDeliveryID       DeliveryID
	nextDispatchID       DispatchID
	agents               map[string]*agentRoute
}

// New constructs a manager for one FleetShift tenant.
func New(tenantID protocol.TenantID, authorizer Authorizer) *Manager {
	if authorizer == nil {
		authorizer = func(AuthorizationRequest) error { return nil }
	}
	return &Manager{
		tenantID:             tenantID,
		authorizer:           authorizer,
		profile:              directkey.NewManager(),
		tree:                 merklelog.New(),
		evidenceByID:         make(map[protocol.Digest]protocol.TypedEvidence),
		logIndexByEvidenceID: make(map[protocol.Digest]uint64),
		deliveries:           make(map[DeliveryID]storedDelivery),
		dispatches:           make(map[DispatchID]storedDispatch),
		agents:               make(map[string]*agentRoute),
	}
}

// RegisterAgent installs the delivery route for one target. The manager starts
// with an empty acknowledged checkpoint; an already-running agent can correct
// that view on the first push. Acceptance may enqueue a dispatch for a target
// before this route exists.
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

// AcceptDirectKeyEnrollment is the typed direct-key/v1 lifecycle API. It is
// not a generic RegisterKey. After the RM's own enrollment check it registers
// the evidence identity in the evidence log (reusing a prior index if this
// exact envelope was already accepted) and enqueues one dispatch per currently
// registered agent. It does not perform network I/O.
func (m *Manager) AcceptDirectKeyEnrollment(_ context.Context, caller protocol.Principal, evidence protocol.TypedEvidence) (DeliveryReceipt, error) {
	if err := m.authorize(caller, ActionEnroll, ""); err != nil {
		return DeliveryReceipt{}, err
	}
	hints, err := directkey.ParseHints(evidence)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	if caller.Scheme != hints.Scheme || caller.Authority != hints.Authority || caller.Subject != hints.Subject {
		return DeliveryReceipt{}, fmt.Errorf("%w: enrollment principal does not match caller", ErrUnauthorized)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitEnrollmentLocked(evidence)
}

// SubmitDirectKeyEnrollment accepts enrollment and dispatches to every
// registered agent. Network I/O happens only in dispatch.
func (m *Manager) SubmitDirectKeyEnrollment(ctx context.Context, caller protocol.Principal, evidence protocol.TypedEvidence) (DeliveryReceipt, error) {
	receipt, err := m.AcceptDirectKeyEnrollment(ctx, caller, evidence)
	if err != nil {
		return receipt, err
	}
	return receipt, m.dispatchAll(ctx, receipt)
}

// AcceptDelivery authorizes the caller, stores evidence, and assigns
// evidence-log positions at first registration. It enqueues outbox work for
// the signed target without performing network I/O. Optional supporting
// evidence is registered independently of the root. Routing identity comes
// from DecodeAssertion then DecodeDeliveryScope; the RM does not parse
// evidence bytes itself.
func (m *Manager) AcceptDelivery(_ context.Context, caller protocol.Principal, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (DeliveryReceipt, error) {
	courier, err := m.courier(evidence.ProvenanceType)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	assertion, err := courier.DecodeAssertion(evidence)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	if scope.TenantID != m.tenantID {
		return DeliveryReceipt{}, fmt.Errorf("%w: delivery tenant mismatch", ErrUnauthorized)
	}
	if err := m.authorize(caller, ActionDeliver, scope.TargetID); err != nil {
		return DeliveryReceipt{}, err
	}
	hints, err := courier.CheckDelivery(evidence)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	if !callerMatches(caller, hints) {
		return DeliveryReceipt{}, fmt.Errorf("%w: delivery principal does not match caller", ErrUnauthorized)
	}
	for _, item := range supporting {
		itemCourier, err := m.courier(item.ProvenanceType)
		if err != nil {
			return DeliveryReceipt{}, err
		}
		if _, err := itemCourier.CheckDelivery(item); err != nil {
			return DeliveryReceipt{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ordered, err := prependRoot(evidence, supporting)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	return m.commitAcceptanceLocked(ordered, scope.TargetID, nil)
}

// SubmitDelivery accepts evidence then dispatches the resulting outbox
// records. A dispatch failure still returns the receipt of the accepted
// mutation so the caller can retry by DispatchID rather than log index.
func (m *Manager) SubmitDelivery(ctx context.Context, caller protocol.Principal, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (DeliveryReceipt, error) {
	receipt, err := m.AcceptDelivery(ctx, caller, evidence, supporting...)
	if err != nil {
		return receipt, err
	}
	return receipt, m.dispatchAll(ctx, receipt)
}

// Dispatch pushes one already accepted outbox record without appending
// evidence or repeating the original caller authorization decision.
// Dispatching an acknowledged record is a no-op.
func (m *Manager) Dispatch(ctx context.Context, id DispatchID) error {
	if err := m.pushDispatch(ctx, id); err != nil {
		return fmt.Errorf("dispatch %d: %w", id, err)
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

func (m *Manager) commitEnrollmentLocked(evidence protocol.TypedEvidence) (DeliveryReceipt, error) {
	transition, err := m.profile.PrepareEnrollment(evidence)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	return m.commitAcceptanceLocked([]protocol.TypedEvidence{evidence}, "", &transition)
}

func (m *Manager) commitAcceptanceLocked(ordered []protocol.TypedEvidence, targetID string, enrollment *directkey.EnrollmentTransition) (DeliveryReceipt, error) {
	planned, err := m.planEvidenceLocked(ordered)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	newHashes := make([][]byte, 0, len(planned))
	for _, item := range planned {
		if item.isNew {
			newHashes = append(newHashes, item.leafHash)
		}
	}
	if len(newHashes) > 0 {
		pending, err := m.tree.BeginAppendHashes(newHashes)
		if err != nil {
			return DeliveryReceipt{}, err
		}
		next := pending.BaseSize()
		for i := range planned {
			if !planned[i].isNew {
				continue
			}
			planned[i].index = next
			next++
		}
		if err := pending.Commit(); err != nil {
			return DeliveryReceipt{}, err
		}
	}
	for _, item := range planned {
		if item.isNew {
			m.evidenceByID[item.identity] = cloneEvidence(item.evidence)
			m.logIndexByEvidenceID[item.identity] = item.index
		}
	}
	if enrollment != nil {
		m.profile.CommitEnrollment(*enrollment)
	}

	rootID := planned[0].identity
	supportingIDs := make([]protocol.Digest, 0, len(planned)-1)
	for _, item := range planned[1:] {
		supportingIDs = append(supportingIDs, item.identity)
	}
	m.nextDeliveryID++
	deliveryID := m.nextDeliveryID
	m.deliveries[deliveryID] = storedDelivery{
		ID:         deliveryID,
		Root:       rootID,
		Supporting: supportingIDs,
	}
	targets := m.dispatchTargetsLocked(targetID)
	dispatchIDs := make([]DispatchID, 0, len(targets))
	for _, target := range targets {
		m.nextDispatchID++
		id := m.nextDispatchID
		m.dispatches[id] = storedDispatch{
			ID:         id,
			DeliveryID: deliveryID,
			TargetID:   target,
			State:      dispatchPending,
		}
		dispatchIDs = append(dispatchIDs, id)
	}
	return DeliveryReceipt{DeliveryID: deliveryID, DispatchIDs: dispatchIDs}, nil
}

func (m *Manager) planEvidenceLocked(ordered []protocol.TypedEvidence) ([]plannedEvidence, error) {
	planned := make([]plannedEvidence, 0, len(ordered))
	for _, evidence := range ordered {
		identity, err := evidence.Identity()
		if err != nil {
			return nil, err
		}
		leafHash, err := protocol.LeafHash(identity)
		if err != nil {
			return nil, err
		}
		item := plannedEvidence{
			evidence: cloneEvidence(evidence),
			identity: identity,
			leafHash: leafHash,
		}
		if existing, ok := m.evidenceByID[identity]; ok {
			if !sameEnvelope(existing, evidence) {
				return nil, fmt.Errorf("%w: %s", ErrEvidenceCollision, identity)
			}
			item.index = m.logIndexByEvidenceID[identity]
			item.isNew = false
		} else {
			item.isNew = true
		}
		planned = append(planned, item)
	}
	return planned, nil
}

func (m *Manager) dispatchTargetsLocked(targetID string) []string {
	if targetID != "" {
		return []string{targetID}
	}
	return m.registeredTargetIDsLocked()
}

func (m *Manager) registeredTargetIDsLocked() []string {
	ids := make([]string, 0, len(m.agents))
	for id := range m.agents {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (m *Manager) dispatchAll(ctx context.Context, receipt DeliveryReceipt) error {
	var first error
	for _, id := range receipt.DispatchIDs {
		if err := m.Dispatch(ctx, id); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Manager) pushDispatch(ctx context.Context, id DispatchID) error {
	m.mu.Lock()
	storedDispatch, ok := m.dispatches[id]
	if !ok {
		m.mu.Unlock()
		return ErrUnknownDispatch
	}
	if storedDispatch.State == dispatchAcknowledged {
		m.mu.Unlock()
		return nil
	}
	stored, ok := m.deliveries[storedDispatch.DeliveryID]
	if !ok {
		m.mu.Unlock()
		return errUnknownDelivery
	}
	route, ok := m.agents[storedDispatch.TargetID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w for target %q", ErrAgentUnavailable, storedDispatch.TargetID)
	}
	return m.pushToRoute(ctx, route, id, stored)
}

func (m *Manager) pushToRoute(ctx context.Context, route *agentRoute, dispatchID DispatchID, stored storedDelivery) error {
	// The target delivery contract permits only one in-flight delivery per
	// fulfillment. Serializing this POC's target route also ensures checkpoint
	// construction and acknowledgement recording cannot race one another.
	route.mu.Lock()
	defer route.mu.Unlock()
	for {
		m.mu.Lock()
		current, ok := m.dispatches[dispatchID]
		if !ok {
			m.mu.Unlock()
			return ErrUnknownDispatch
		}
		if current.State == dispatchAcknowledged {
			m.mu.Unlock()
			return nil
		}
		update, err := m.evidenceLogUpdateLocked(route.checkpoint, stored.Root)
		if err != nil {
			m.mu.Unlock()
			return fmt.Errorf("construct proof from acknowledged agent checkpoint: %w", err)
		}
		pkg, err := m.deliveryPackageLocked(ctx, update, stored)
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if err := route.agent.Deliver(pkg); err != nil {
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
		m.mu.Lock()
		current = m.dispatches[dispatchID]
		current.State = dispatchAcknowledged
		m.dispatches[dispatchID] = current
		m.mu.Unlock()
		return nil
	}
}

func (m *Manager) deliveryPackageLocked(ctx context.Context, update protocol.EvidenceLogUpdate, stored storedDelivery) (DeliveryPackage, error) {
	rootEvidence, err := m.lookupEvidenceLocked(stored.Root)
	if err != nil {
		return DeliveryPackage{}, err
	}
	root, err := m.assembleStatement(ctx, rootEvidence)
	if err != nil {
		return DeliveryPackage{}, err
	}
	out := make([]protocol.SignedStatement, 0, len(stored.Supporting))
	for _, id := range stored.Supporting {
		item, err := m.lookupEvidenceLocked(id)
		if err != nil {
			return DeliveryPackage{}, err
		}
		stmt, err := m.assembleStatement(ctx, item)
		if err != nil {
			return DeliveryPackage{}, err
		}
		out = append(out, stmt)
	}
	return DeliveryPackage{
		EvidenceLog: update,
		Root:        root,
		Supporting:  out,
	}, nil
}

func (m *Manager) evidenceLogUpdateLocked(from protocol.Checkpoint, root protocol.Digest) (protocol.EvidenceLogUpdate, error) {
	if from.Size > m.tree.Size() {
		return protocol.EvidenceLogUpdate{}, fmt.Errorf("checkpoint size %d is beyond log size %d", from.Size, m.tree.Size())
	}
	wantPrevious, err := m.checkpointAtLocked(from.Size)
	if err != nil {
		return protocol.EvidenceLogUpdate{}, err
	}
	if from != wantPrevious {
		return protocol.EvidenceLogUpdate{}, errors.New("checkpoint is not on the retained evidence-log branch")
	}
	current, err := m.checkpointAtLocked(m.tree.Size())
	if err != nil {
		return protocol.EvidenceLogUpdate{}, err
	}
	consistency, err := m.tree.ConsistencyProof(from.Size, current.Size)
	if err != nil {
		return protocol.EvidenceLogUpdate{}, fmt.Errorf("construct evidence-log consistency proof: %w", err)
	}
	index, ok := m.logIndexByEvidenceID[root]
	if !ok {
		return protocol.EvidenceLogUpdate{}, fmt.Errorf("%w: %s", errUnknownEvidence, root)
	}
	if index >= current.Size {
		return protocol.EvidenceLogUpdate{}, fmt.Errorf("evidence-log index %d is beyond size %d", index, current.Size)
	}
	inclusion, err := m.tree.InclusionProof(index, current.Size)
	if err != nil {
		return protocol.EvidenceLogUpdate{}, fmt.Errorf("construct inclusion proof for index %d: %w", index, err)
	}
	return protocol.EvidenceLogUpdate{
		From:             from,
		Checkpoint:       current,
		ConsistencyProof: protocol.EncodeProof(consistency),
		Index:            index,
		Leaf:             root,
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
		return errors.New("checkpoint is not on the retained evidence-log branch")
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

func (m *Manager) lookupEvidenceLocked(id protocol.Digest) (protocol.TypedEvidence, error) {
	evidence, ok := m.evidenceByID[id]
	if !ok {
		return protocol.TypedEvidence{}, fmt.Errorf("%w: %s", errUnknownEvidence, id)
	}
	return cloneEvidence(evidence), nil
}

// EvidenceLogSize is the number of accepted evidence-log leaves.
func (m *Manager) EvidenceLogSize() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tree.Size()
}

// EvidenceLogIndex is the canonical evidence-log position assigned when the
// honest RM first accepted this identity. It is independent of later
// deliveries and outbox entries.
func (m *Manager) EvidenceLogIndex(identity protocol.Digest) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	index, ok := m.logIndexByEvidenceID[identity]
	return index, ok
}

// Evidence returns a stored item by identity.
func (m *Manager) Evidence(identity protocol.Digest) (protocol.TypedEvidence, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	evidence, ok := m.evidenceByID[identity]
	if !ok {
		return protocol.TypedEvidence{}, false
	}
	return cloneEvidence(evidence), true
}

// LookupDelivery returns the stored delivery record.
func (m *Manager) LookupDelivery(id DeliveryID) (StoredDelivery, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, ok := m.deliveries[id]
	if !ok {
		return StoredDelivery{}, false
	}
	out := StoredDelivery{ID: stored.ID, Root: stored.Root}
	if len(stored.Supporting) > 0 {
		out.Supporting = append([]protocol.Digest(nil), stored.Supporting...)
	}
	return out, true
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

// CommitEnrollment stores and delivers enrollment without caller authorization.
// It still uses common evidence registration, exact-envelope deduplication,
// and outbox dispatch.
func (c *CompromisedManager) CommitEnrollment(ctx context.Context, evidence protocol.TypedEvidence) error {
	c.manager.mu.Lock()
	receipt, err := c.manager.commitEnrollmentLocked(evidence)
	c.manager.mu.Unlock()
	if err != nil {
		return err
	}
	return c.manager.dispatchAll(ctx, receipt)
}

// PushDelivery stores and routes a delivery without authorization or the
// RM's own provenance check. Evidence registration, first-index assignment,
// and outbox enqueue still use the honest-service path.
func (c *CompromisedManager) PushDelivery(ctx context.Context, evidence protocol.TypedEvidence, supporting ...protocol.TypedEvidence) (DeliveryReceipt, error) {
	courier, err := c.manager.courier(evidence.ProvenanceType)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	assertion, err := courier.DecodeAssertion(evidence)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	scope, err := protocol.DecodeDeliveryScope(assertion)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	ordered, err := prependRoot(evidence, supporting)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	c.manager.mu.Lock()
	receipt, err := c.manager.commitAcceptanceLocked(ordered, scope.TargetID, nil)
	c.manager.mu.Unlock()
	if err != nil {
		return receipt, err
	}
	return receipt, c.manager.dispatchAll(ctx, receipt)
}

func (m *Manager) courier(pt protocol.ProvenanceType) (protocol.ResourceManagerAPI, error) {
	if m.profile.ProvenanceType() != pt {
		return nil, fmt.Errorf("%w: %s", protocol.ErrUnknownProvenanceType, pt)
	}
	return m.profile, nil
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

func prependRoot(root protocol.TypedEvidence, supporting []protocol.TypedEvidence) ([]protocol.TypedEvidence, error) {
	rootID, err := root.Identity()
	if err != nil {
		return nil, err
	}
	ordered := []protocol.TypedEvidence{cloneEvidence(root)}
	seen := map[protocol.Digest]protocol.TypedEvidence{rootID: ordered[0]}
	for _, item := range supporting {
		id, err := item.Identity()
		if err != nil {
			return nil, err
		}
		if existing, ok := seen[id]; ok {
			if !sameEnvelope(existing, item) {
				return nil, fmt.Errorf("%w: %s", ErrEvidenceCollision, id)
			}
			continue
		}
		cloned := cloneEvidence(item)
		seen[id] = cloned
		ordered = append(ordered, cloned)
	}
	return ordered, nil
}

func sameEnvelope(a, b protocol.TypedEvidence) bool {
	return a.ProvenanceType == b.ProvenanceType && a.MediaType == b.MediaType && bytes.Equal(a.Bytes, b.Bytes)
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
