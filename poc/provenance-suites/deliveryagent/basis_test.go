package deliveryagent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestOrdinaryBasisIncludesSelectedAndRetainedEvidence(t *testing.T) {
	for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1} {
		t.Run(string(purpose), func(t *testing.T) {
			user, addon := testProducer(t, "user"), testProducer(t, "addon")
			native := directkey.NewTarget()
			enrollTestTarget(t, native, user)
			enrollTestTarget(t, native, addon)
			userKey, addonKey := enrollmentIdentity(t, user), enrollmentIdentity(t, addon)
			shared := protocol.DigestBytes([]byte("retained shared boundary"))
			retired := protocol.DigestBytes([]byte("retained retirement boundary"))
			dominated := protocol.DigestBytes([]byte("retained dominated retirement boundary"))
			unusedBasis := protocol.DigestBytes([]byte("unused authentication boundary"))
			root := ordinaryTestEvidence(t, user, purpose, ordinaryTestScope(0, protocol.ActionPut))
			relation := lookupRelationItem(t, addon, "kind.example/Cluster", "application/json")
			unused := lookupRelationItem(t, addon, "other.example/Cluster", "application/unused")
			rootID, relationID, unusedID := evidenceIdentity(t, root), evidenceIdentity(t, relation.Evidence), evidenceIdentity(t, unused.Evidence)
			rootBasis := []protocol.Digest{userKey, shared}
			if purpose == protocol.PredicateTypeManagedResourceV1 {
				// Temporal membership precedes semantic selection of this node;
				// its own validity basis must still be merged when it is used.
				rootBasis = append(rootBasis, relationID)
			}
			wrapped := &basisConstraintTarget{TargetAPI: native, decorate: func(request protocol.VerifyRequest, result *protocol.ProvenanceAuthenticationResult) {
				switch evidenceIdentity(t, request.Statement.Evidence) {
				case rootID:
					result.Established = []protocol.AuthenticatedTemporalConstraint{basisLogConstraint(0, rootBasis...)}
				case relationID:
					result.Established = []protocol.AuthenticatedTemporalConstraint{basisLogConstraint(0, addonKey, shared, shared)}
					result.Retired = []protocol.AuthenticatedTemporalConstraint{
						basisLogConstraint(100, retired),
						basisLogConstraint(200, dominated, shared),
					}
				case unusedID:
					result.Established = []protocol.AuthenticatedTemporalConstraint{basisLogConstraint(0, unusedBasis)}
				}
			}}
			counted := &countedTarget{delegate: wrapped}
			session, logs := relationLookupSession(t, loggedTestPackage(t, root, relation.Evidence, unused.Evidence), counted)
			// Independent real authentication supplies a memo entry but no use.
			if _, err := session.verifyNode(context.Background(), unusedID); err != nil {
				t.Fatal(err)
			}
			if purpose == protocol.PredicateTypeManagedResourceV1 {
				if _, err := session.verifyNode(context.Background(), relationID); err != nil {
					t.Fatal(err)
				}
			}
			assertSelectedBasis(t, session)
			agent := newDispatchTestAgent()
			if err := session.withNode(context.Background(), rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
				t.Fatal(err)
			}
			want := []protocol.Digest{rootID, userKey, shared}
			wantBegins, wantEdges := 2, 0
			if purpose == protocol.PredicateTypeManagedResourceV1 {
				want = append(want, relationID, addonKey, retired, dominated)
				wantBegins, wantEdges = 3, 1
			}
			cached := slices.Clone(session.memo[rootID].verified.result.Validity.Basis)
			assertSelectedBasis(t, session, want...)
			if len(session.basisContributors) != wantEdges+1 {
				t.Fatal("temporal basis membership was mistaken for a merged node contribution")
			}
			if counted.beginCalls != wantBegins || counted.finishCalls != wantBegins || session.edgeCount != wantEdges || logs.calls[rootID] != 1 || logs.calls[unusedID] != 1 || logs.calls[relationID] != wantEdges {
				t.Fatal("basis accounting repeated verification or changed selection")
			}
			for _, id := range []protocol.Digest{userKey, addonKey, shared, retired, dominated, unusedBasis} {
				if _, exists := session.catalog.byID[id]; exists {
					t.Fatal("retained boundary unexpectedly appears in couriered evidence")
				}
				if logs.calls[id] != 0 {
					t.Fatal("basis accounting attempted an occurrence proof for retained evidence")
				}
			}
			basis := session.selectedBasis()
			basis[0] = protocol.DigestBytes([]byte("caller mutation"))
			assertSelectedBasis(t, session, want...)
			if !slices.Equal(cached, session.memo[rootID].verified.result.Validity.Basis) {
				t.Fatal("basis finalization mutated cached validity")
			}
		})
	}
}

func TestOrdinaryBasisIsIndependentOfCurrentInclusions(t *testing.T) {
	for _, rootRequired := range []bool{false, true} {
		for _, relationRequired := range []bool{false, true} {
			t.Run(fmt.Sprintf("root logged=%t/relation logged=%t", rootRequired, relationRequired), func(t *testing.T) {
				user := testProducer(t, "user")
				target := directkey.NewTarget()
				enrollTestTarget(t, target, user)
				root := ordinaryTestEvidence(t, user, protocol.PredicateTypeManagedResourceV1, ordinaryTestScope(0, protocol.ActionPut))
				relation := lookupRelationItem(t, user, "kind.example/Cluster", "application/json")
				pkg := loggedTestPackage(t, root, relation.Evidence)
				if !rootRequired {
					pkg.Root.EvidenceLog = nil
				}
				if !relationRequired {
					pkg.Supporting[0].EvidenceLog = nil
				}
				session, logs := relationLookupSession(t, pkg, target)
				for i := range session.trust.AuthorityRegistry[0].DeliveryPolicies {
					policy := &session.trust.AuthorityRegistry[0].DeliveryPolicies[i]
					if policy.Match.PredicateType == protocol.PredicateTypeManagedResourceV1 {
						policy.RequireEvidenceLog = rootRequired
					}
					if policy.Match.PredicateType == protocol.PredicateTypeFulfillmentRelationV1 {
						policy.RequireEvidenceLog = relationRequired
					}
				}
				agent := newDispatchTestAgent()
				if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
					t.Fatal(err)
				}
				rootID, relationID := evidenceIdentity(t, root), evidenceIdentity(t, relation.Evidence)
				assertSelectedBasis(t, session, rootID, relationID)
				if (logs.calls[rootID] == 1) != rootRequired || (logs.calls[relationID] == 1) != relationRequired {
					t.Fatal("basis changed current log requirements")
				}
			})
		}
	}
}

func TestFailedProfileValidityBasisDoesNotContribute(t *testing.T) {
	user := testProducer(t, "user")
	native := directkey.NewTarget()
	enrollTestTarget(t, native, user)
	failed, accepted := protocol.DigestBytes([]byte("failed profile boundary")), enrollmentIdentity(t, user)
	wrapped := &basisConstraintTarget{TargetAPI: native, decorate: func(request protocol.VerifyRequest, result *protocol.ProvenanceAuthenticationResult) {
		if string(request.ProfileConfig.Parameters) == "first" {
			cutoff := basisLogConstraint(0, failed)
			cutoff.Boundary.Log.Inclusive = false
			result.Retired = []protocol.AuthenticatedTemporalConstraint{cutoff}
		} else {
			result.Established = []protocol.AuthenticatedTemporalConstraint{basisLogConstraint(0, accepted)}
		}
	}}
	counted := &countedTarget{delegate: wrapped}
	root := ordinaryTestEvidence(t, user, protocol.PredicateTypeDeploymentV1, ordinaryTestScope(0, protocol.ActionPut))
	session, _ := relationLookupSession(t, loggedTestPackage(t, root), counted)
	first := protocol.ProfileConfig{ProvenanceType: native.ProvenanceType(), Parameters: []byte("first")}
	second := protocol.ProfileConfig{ProvenanceType: native.ProvenanceType(), Parameters: []byte("second")}
	authority := &session.trust.AuthorityRegistry[0]
	authority.ProvenanceProfiles = []protocol.ProfileConfig{first, second}
	for i := range authority.DeliveryPolicies {
		authority.DeliveryPolicies[i].Profiles = []protocol.Digest{profileDigest(first), profileDigest(second)}
	}
	agent := newDispatchTestAgent()
	if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
		t.Fatal(err)
	}
	assertSelectedBasis(t, session, session.catalog.rootID, accepted)
	if counted.beginCalls != 2 || counted.finishCalls != 2 || session.memo[session.catalog.rootID].verified.result.Authenticated.ProfileConfigDigest != profileDigest(second) {
		t.Fatal("test did not exercise a rejected real profile attempt followed by success")
	}
}

func TestSharedDependenciesAccumulateOnceWhileEveryUseChecksSemantics(t *testing.T) {
	user := testProducer(t, "user")
	native := directkey.NewTarget()
	enrollTestTarget(t, native, user)
	root := ordinaryTestEvidence(t, user, protocol.PredicateTypeDeploymentV1, ordinaryTestScope(0, protocol.ActionPut))
	first := lookupRelationItem(t, user, "one.example/Cluster", "application/one")
	shared := lookupRelationItem(t, user, "two.example/Cluster", "application/two")
	var baseline []protocol.Digest
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared first=%t", reverse), func(t *testing.T) {
			counted := &countedTarget{delegate: native}
			session, _ := relationLookupSession(t, loggedTestPackage(t, root, first.Evidence, shared.Evidence), counted)
			firstID, sharedID := evidenceIdentity(t, first.Evidence), evidenceIdentity(t, shared.Evidence)
			agent := newDispatchTestAgent()
			uses := make(map[protocol.Digest]int)
			// A test-only common evaluator adds nested premises that today's
			// predicates cannot express, while keeping every consumer active.
			var use func(protocol.Digest, protocol.Digest) error
			use = func(parent, child protocol.Digest) error {
				return session.withNode(context.Background(), child, func(node verifiedNode) error {
					uses[child]++
					if node.result.Authenticated.Principal.Tenant() != sessionTenant() {
						return protocol.ErrTenantMismatch
					}
					if _, err := protocol.DecodeFulfillmentRelation(node.result.Assertion); err != nil {
						return err
					}
					if child == firstID {
						if err := use(child, sharedID); err != nil {
							return err
						}
					}
					return session.recordDependency(parent, child)
				})
			}
			err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error {
				view, required, err := agent.handleDeploymentLocked(session, root)
				if err != nil || !required {
					return err
				}
				order := []protocol.Digest{firstID, sharedID, firstID}
				if reverse {
					order = []protocol.Digest{sharedID, firstID, firstID}
				}
				for _, id := range order {
					if err := use(root.identity, id); err != nil {
						return err
					}
				}
				return agent.applyLocked(view)
			})
			if err != nil {
				t.Fatal(err)
			}
			assertSelectedBasis(t, session, session.catalog.rootID, firstID, sharedID)
			basis := session.selectedBasis()
			if baseline == nil {
				baseline = basis
			} else if !slices.Equal(basis, baseline) {
				t.Fatal("semantic use order changed the same selected evidence basis")
			}
			if len(session.basisContributors) != 3 {
				t.Fatal("shared dependency contributions were not tracked once per selected node")
			}
			if uses[firstID] != 2 || uses[sharedID] != 3 || session.edgeCount != 3 || counted.beginCalls != 3 || counted.finishCalls != 3 {
				t.Fatal("shared use skipped semantics, repeated authentication, or duplicated edges")
			}
		})
	}
}

func TestRequiredPremiseFailurePreventsEffectsAfterPartialAccumulation(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			want error
		}{
			{"tenant mismatch", protocol.ErrTenantMismatch},
			{"cycle", errVerificationCycle},
			{"edge limit", errVerificationWorkLimit},
		} {
			t.Run(fmt.Sprintf("cached=%t/%s", cached, tc.name), func(t *testing.T) {
				user := testProducer(t, "user")
				target := directkey.NewTarget()
				enrollTestTarget(t, target, user)
				root := ordinaryTestEvidence(t, user, protocol.PredicateTypeDeploymentV1, ordinaryTestScope(0, protocol.ActionPut))
				first := lookupRelationItem(t, user, "one.example/Cluster", "application/one")
				second := lookupRelationItem(t, user, "two.example/Cluster", "application/two")
				counted := &countedTarget{delegate: target}
				session, _ := relationLookupSession(t, loggedTestPackage(t, root, first.Evidence, second.Evidence), counted)
				if tc.name == "edge limit" {
					session.limits.maxEdges = 1
				}
				firstID, secondID := evidenceIdentity(t, first.Evidence), evidenceIdentity(t, second.Evidence)
				if cached {
					for _, id := range []protocol.Digest{session.catalog.rootID, firstID, secondID} {
						if _, err := session.verifyNode(context.Background(), id); err != nil {
							t.Fatal(err)
						}
					}
				}
				agent := newDispatchTestAgent()
				evaluated := false
				err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error {
					view, required, err := agent.handleDeploymentLocked(session, root)
					if err != nil || !required {
						return err
					}
					if err := session.withNode(context.Background(), firstID, func(node verifiedNode) error { return session.recordDependency(root.identity, node.identity) }); err != nil {
						return err
					}
					if err := session.withNode(context.Background(), secondID, func(node verifiedNode) error {
						evaluated = true
						if tc.name == "cycle" {
							return session.withNode(context.Background(), root.identity, nil)
						}
						if tc.name == "tenant mismatch" && node.result.Authenticated.Principal.Tenant() != otherSessionTenant() {
							return protocol.ErrTenantMismatch
						}
						return session.recordDependency(root.identity, node.identity)
					}); err != nil {
						return err
					}
					return agent.applyLocked(view)
				})
				if !errors.Is(err, tc.want) || !evaluated {
					t.Fatalf("required premise: %v, evaluated=%v", err, evaluated)
				}
				// Inspect partial accounting only to prove this was a failure
				// after selection; the failed session yields no action to apply.
				assertSelectedBasis(t, session, session.catalog.rootID, firstID)
				if len(agent.applied) != 0 || len(agent.generations) != 0 || counted.applyCalls != 0 || session.edgeCount != 1 || len(session.active) != 0 || len(session.activeSet) != 0 || counted.beginCalls != 3 {
					t.Fatal("failed premise reached effects or bypassed active evaluation")
				}
			})
		}
	}
}

func TestSuiteOwnedEnrollmentDoesNotAccumulateCommonBasis(t *testing.T) {
	user := testProducer(t, "user")
	target := directkey.NewTarget()
	root, err := user.CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	counted := &countedTarget{delegate: target}
	session, _ := relationLookupSession(t, loggedTestPackage(t, root), counted)
	authority := &session.trust.AuthorityRegistry[0]
	authority.DeliveryPolicies = append(authority.DeliveryPolicies, sessionPolicy(directkey.PredicateTypeEnrollmentV1, authority.ProvenanceProfiles[0]))
	agent := newDispatchTestAgent()
	if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
		t.Fatal(err)
	}
	assertSelectedBasis(t, session)
	if session.basisContributors != nil || session.actionBasis != nil {
		t.Fatal("suite-owned event initialized ordinary basis accounting")
	}
	if counted.finishCalls != 1 || counted.applyCalls != 1 || agent.suiteApplyCount != 1 || len(agent.applied) != 0 || len(agent.generations) != 0 {
		t.Fatal("enrollment did not stay on its suite-owned path")
	}
	if _, exists := target.PublicKey(user.Principal()); !exists {
		t.Fatal("real enrollment mapping was not applied")
	}
}

func assertSelectedBasis(t *testing.T, session *verificationSession, want ...protocol.Digest) {
	t.Helper()
	want = slices.Clone(want)
	slices.Sort(want)
	if got := session.selectedBasis(); !slices.Equal(got, want) {
		t.Fatalf("selected basis=%v, want %v", got, want)
	}
}

func evidenceIdentity(t *testing.T, evidence protocol.TypedEvidence) protocol.Digest {
	t.Helper()
	identity, err := evidence.Identity()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func enrollmentIdentity(t *testing.T, producer *directkey.Producer) protocol.Digest {
	t.Helper()
	// Enrollment signatures are deterministic for this key. This recovers the
	// evidence identity of the mapping installed by enrollTestTarget.
	evidence, err := producer.CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	return evidenceIdentity(t, evidence)
}

func basisLogConstraint(index uint64, basis ...protocol.Digest) protocol.AuthenticatedTemporalConstraint {
	return protocol.AuthenticatedTemporalConstraint{
		Boundary: protocol.AuthenticatedTemporalBoundary{Log: &protocol.AuthenticatedLogBoundary{Position: protocol.LogPosition{Domain: protocol.LogDomainTenantEvidenceV1, Index: index}, Inclusive: true}},
		Basis:    basis,
	}
}

// This decorator models recovered retained boundary facts without implementing
// continuity. Native parsing, signatures, and common temporal checks still run.
type basisConstraintTarget struct {
	protocol.TargetAPI
	decorate func(protocol.VerifyRequest, *protocol.ProvenanceAuthenticationResult)
}

func (t *basisConstraintTarget) BeginVerification(ctx context.Context, request protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	// The fixture interprets first/second parameters to supply distinct
	// recovered constraints. Direct-key itself has no profile parameters.
	nativeRequest := request
	nativeRequest.ProfileConfig.Parameters = nil
	session, err := t.TargetAPI.BeginVerification(ctx, nativeRequest)
	if err != nil {
		return nil, err
	}
	return &basisConstraintSession{ProvenanceVerificationSession: session, request: request, decorate: t.decorate}, nil
}

type basisConstraintSession struct {
	protocol.ProvenanceVerificationSession
	request  protocol.VerifyRequest
	decorate func(protocol.VerifyRequest, *protocol.ProvenanceAuthenticationResult)
}

func (s *basisConstraintSession) Finish(ctx context.Context, inputs protocol.VerifiedProvenanceTemporalInputs) (protocol.ProvenanceAuthenticationResult, error) {
	result, err := s.ProvenanceVerificationSession.Finish(ctx, inputs)
	if err != nil {
		return protocol.ProvenanceAuthenticationResult{}, err
	}
	profile, err := s.request.ProfileConfig.Digest()
	if err != nil {
		return protocol.ProvenanceAuthenticationResult{}, err
	}
	result.Authenticated.ProfileConfigDigest = profile
	s.decorate(s.request, &result)
	return result, nil
}
