package deliveryagent

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

type nodePhase uint8

const (
	nodeVerifying nodePhase = iota + 1
	nodeVerified
)

type graphVisitState uint8

const (
	graphUnseen graphVisitState = iota
	graphVisiting
	graphComplete
)

type nodeState struct {
	phase    nodePhase
	verified verifiedNode
}

// verifiedNode shares immutable catalog and authentication data. Semantic
// callbacks inspect it and record relationships without modifying its fields.
type verifiedNode struct {
	identity  protocol.Digest
	statement protocol.SignedStatement
	result    protocol.VerificationResult
}

// verificationSession keeps package lookup, profile selection, successful
// verification results, and semantic dependency state scoped to one delivery.
// Profiles see only protocol.VerifyRequest; they never receive this resolver.
// Trust, catalog items, and completed verification results are immutable shared
// values. Semantic consumers borrow cached nodes and must not mutate them.
type verificationSession struct {
	catalog  *evidenceCatalog
	trust    protocol.TrustConfiguration
	temporal protocol.TemporalVerificationServices
	limits   verificationLimits

	memo      map[protocol.Digest]nodeState
	active    []protocol.Digest
	activeSet map[protocol.Digest]struct{}
	edges     map[protocol.Digest]map[protocol.Digest]struct{}
	edgeCount int
}

func newVerificationSession(catalog *evidenceCatalog, trust protocol.TrustConfiguration, temporal protocol.TemporalVerificationServices) *verificationSession {
	limits := defaultVerificationLimits()
	if catalog != nil {
		limits = catalog.limits
	}
	return &verificationSession{
		catalog:   catalog,
		trust:     trust,
		temporal:  temporal,
		limits:    limits,
		memo:      make(map[protocol.Digest]nodeState),
		activeSet: make(map[protocol.Digest]struct{}),
		edges:     make(map[protocol.Digest]map[protocol.Digest]struct{}),
	}
}

func (s *verificationSession) withNode(
	ctx context.Context,
	identity protocol.Digest,
	evaluate func(verifiedNode) error,
) error {
	if _, active := s.activeSet[identity]; active {
		return fmt.Errorf("%w: identity %s is already active", errVerificationCycle, identity)
	}
	if len(s.active) >= s.limits.maxDepth {
		return fmt.Errorf("%w: active dependency depth exceeds %d", errVerificationWorkLimit, s.limits.maxDepth)
	}
	s.active = append(s.active, identity)
	s.activeSet[identity] = struct{}{}
	defer s.leaveNode(identity)

	verified, err := s.verifyNode(ctx, identity)
	if err != nil {
		return err
	}
	if evaluate == nil {
		return nil
	}
	return evaluate(verified)
}

func (s *verificationSession) leaveNode(identity protocol.Digest) {
	delete(s.activeSet, identity)
	if len(s.active) > 0 && s.active[len(s.active)-1] == identity {
		s.active = s.active[:len(s.active)-1]
		return
	}
	for i := len(s.active) - 1; i >= 0; i-- {
		if s.active[i] == identity {
			s.active = append(s.active[:i], s.active[i+1:]...)
			return
		}
	}
}

// verifyNode authenticates one package item and memoizes only a successful
// result by evidence identity under the session's fixed trust configuration.
// Package position does not select policy. Looking at a candidate never
// records an authorization dependency.
func (s *verificationSession) verifyNode(
	ctx context.Context,
	identity protocol.Digest,
) (verifiedNode, error) {
	if s.catalog == nil {
		return verifiedNode{}, errUnknownCatalogEvidence
	}
	item, exists := s.catalog.byID[identity]
	if !exists {
		return verifiedNode{}, fmt.Errorf("%w: %s", errUnknownCatalogEvidence, identity)
	}
	if state, cached := s.memo[identity]; cached {
		if state.phase == nodeVerifying {
			return verifiedNode{}, fmt.Errorf("%w: identity %s is already being verified", errVerificationCycle, identity)
		}
		return state.verified, nil
	}

	s.memo[identity] = nodeState{phase: nodeVerifying}
	result, err := protocol.SelectAndVerify(ctx, item, s.trust, s.catalog.lookup, s.temporal)
	if err != nil {
		delete(s.memo, identity)
		return verifiedNode{}, err
	}
	verified := verifiedNode{
		identity:  identity,
		statement: item.SignedStatement,
		result:    result,
	}
	s.memo[identity] = nodeState{
		phase:    nodeVerified,
		verified: verified,
	}
	return verified, nil
}

func (s *verificationSession) supportingCandidates(predicate protocol.PredicateType) ([]protocol.Digest, error) {
	if s.catalog == nil {
		return nil, errUnknownCatalogEvidence
	}
	return s.catalog.supportingCandidates(predicate)
}

// recordDependency adds an edge only after common semantic code has selected
// and authenticated the child. Recursive cycles are rejected by withNode;
// cycles between independently cached nodes are rejected by
// validateSelectedGraph before apply.
func (s *verificationSession) recordDependency(parent, child protocol.Digest) error {
	if !s.hasVerifiedIdentity(parent) || !s.hasVerifiedIdentity(child) {
		return fmt.Errorf("%w: dependency endpoints must be verified before edge insertion", protocol.ErrPolicyReevaluation)
	}
	if parent == child {
		return fmt.Errorf("%w: identity %s depends on itself", errVerificationCycle, parent)
	}
	if children := s.edges[parent]; children != nil {
		if _, exists := children[child]; exists {
			return nil
		}
	}
	if s.edgeCount >= s.limits.maxEdges {
		return fmt.Errorf("%w: chosen semantic edge count exceeds %d", errVerificationWorkLimit, s.limits.maxEdges)
	}
	if s.edges[parent] == nil {
		s.edges[parent] = make(map[protocol.Digest]struct{})
	}
	s.edges[parent][child] = struct{}{}
	s.edgeCount++
	return nil
}

func (s *verificationSession) hasVerifiedIdentity(identity protocol.Digest) bool {
	state, exists := s.memo[identity]
	return exists && state.phase == nodeVerified
}

// validateSelectedGraph checks the selected dependency closure with one
// depth-first traversal. It returns the reachable identities in traversal
// order so common action derivation can include them in Basis; callers sort
// the combined basis separately.
func (s *verificationSession) validateSelectedGraph(root protocol.Digest) ([]protocol.Digest, error) {
	states := make(map[protocol.Digest]graphVisitState)
	reachable := make([]protocol.Digest, 0)
	var visit func(protocol.Digest) error
	visit = func(identity protocol.Digest) error {
		switch states[identity] {
		case graphVisiting:
			return fmt.Errorf("%w: identity %s is already on the selected dependency path", errVerificationCycle, identity)
		case graphComplete:
			return nil
		}
		if !s.hasVerifiedIdentity(identity) {
			return fmt.Errorf("%w: selected dependency identity %s is not verified", protocol.ErrPolicyReevaluation, identity)
		}

		states[identity] = graphVisiting
		reachable = append(reachable, identity)
		for child := range s.edges[identity] {
			if err := visit(child); err != nil {
				return err
			}
		}
		states[identity] = graphComplete
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	return reachable, nil
}
