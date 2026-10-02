package deliveryagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestDeploymentHandlerPreservesAuthenticatedManifestSequence(t *testing.T) {
	user := testProducer(t, "user")
	target := directkey.NewTarget()
	enrollTestTarget(t, target, user)
	authorization := protocol.DeploymentAuthorization{
		DeliveryScope: ordinaryTestScope(0, protocol.ActionPut),
		Manifests: []protocol.TypedManifest{
			{MediaType: "application/octet-stream", Bytes: []byte{0, 255, 10}},
			{MediaType: "application/json", Bytes: []byte("{ \"region\": \"east\" }\n")},
		},
	}
	assertion, err := authorization.Assertion()
	if err != nil {
		t.Fatal(err)
	}
	root := lookupAssertionItem(t, user, assertion).Evidence
	counted := &countedTarget{delegate: target}
	session, logs := relationLookupSession(t, loggedTestPackage(t, root, catalogItem("unused support", "").Evidence), counted)
	agent := newDispatchTestAgent()
	if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
		t.Fatal(err)
	}
	view, ok := agent.Applied(authorization.FullResourceName)
	if !ok || view.Scope != authorization.DeliveryScope || view.PredicateType != protocol.PredicateTypeDeploymentV1 || len(view.Manifests) != len(authorization.Manifests) {
		t.Fatalf("applied view=%+v", view)
	}
	for i, want := range authorization.Manifests {
		got := view.Manifests[i]
		if got.MediaType != want.MediaType || !bytes.Equal(got.Bytes, want.Bytes) {
			t.Fatalf("manifest %d changed: %+v", i, got)
		}
	}
	if counted.parseCalls != 1 || counted.beginCalls != 1 || counted.finishCalls != 1 || session.catalog.cursor != 0 || session.edgeCount != 0 || logs.calls[session.catalog.supporting[0]] != 0 {
		t.Fatal("deployment performed supporting work")
	}
}

func TestOrdinaryChecksPrecedeSupportingWork(t *testing.T) {
	for _, tc := range []struct {
		name               string
		edit               func(*protocol.DeliveryScope)
		principalPartition protocol.TenantPartition
		stale              bool
		want               error
	}{
		{name: "missing scheme", edit: func(s *protocol.DeliveryScope) { s.Tenant.Scheme = "" }, want: protocol.ErrMalformedEvidence},
		{name: "missing authority", edit: func(s *protocol.DeliveryScope) { s.Tenant.Authority = "" }, want: protocol.ErrMalformedEvidence},
		{name: "missing target", edit: func(s *protocol.DeliveryScope) { s.TargetID = "" }, want: protocol.ErrMalformedEvidence},
		{name: "missing name", edit: func(s *protocol.DeliveryScope) { s.FullResourceName = "" }, want: protocol.ErrMalformedEvidence},
		{name: "missing action", edit: func(s *protocol.DeliveryScope) { s.Action = "" }, want: protocol.ErrMalformedEvidence},
		{name: "unsupported action", edit: func(s *protocol.DeliveryScope) { s.Action = "invented" }, want: protocol.ErrMalformedEvidence},
		{name: "wrong target", edit: func(s *protocol.DeliveryScope) { s.TargetID = "elsewhere" }, want: protocol.ErrPolicyReevaluation},
		{name: "wrong tenant", edit: func(s *protocol.DeliveryScope) { s.Tenant.Partition = "elsewhere" }, want: protocol.ErrPolicyReevaluation},
		{name: "wrong principal tenant", principalPartition: "elsewhere", want: protocol.ErrTenantMismatch},
		{name: "stale generation", stale: true, want: ErrGeneration},
	} {
		for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1} {
			t.Run(tc.name+"/"+string(purpose), func(t *testing.T) {
				principal := protocol.Principal{Scheme: sessionTenant().Scheme, Authority: sessionTenant().Authority, Subject: "user", TenantPartition: tc.principalPartition}
				signer, err := directkey.NewProducer(principal)
				if err != nil {
					t.Fatal(err)
				}
				target := directkey.NewTarget()
				enrollTestTarget(t, target, signer)
				scope := ordinaryTestScope(1, protocol.ActionPut)
				if tc.edit != nil {
					tc.edit(&scope)
				}
				root := ordinaryTestEvidence(t, signer, purpose, scope)
				// An unusable supporting envelope exposes any premature scan.
				decoy := catalogItem("unusable support", "")
				counted := &countedTarget{delegate: target}
				session, logs := relationLookupSession(t, loggedTestPackage(t, root, decoy.Evidence), counted)
				agent := newDispatchTestAgent()
				if tc.stale {
					agent.generations[scope.FullResourceName] = 2
				}
				err = session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
				if !errors.Is(err, tc.want) {
					t.Fatalf("error=%v, want %v", err, tc.want)
				}
				if counted.parseCalls != 1 || counted.beginCalls != 1 || counted.finishCalls != 1 || counted.applyCalls != 0 || session.catalog.cursor != 0 || session.edgeCount != 0 || logs.calls[session.catalog.supporting[0]] != 0 || len(agent.applied) != 0 {
					t.Fatal("rejected root performed supporting work or effects")
				}
				if !tc.stale && len(agent.generations) != 0 {
					t.Fatal("rejection retained a generation")
				}
			})
		}
	}
}

func TestCompletedOrdinaryRetriesSkipSupportingAndManifestWork(t *testing.T) {
	for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1} {
		t.Run(string(purpose), func(t *testing.T) {
			user, addon := testProducer(t, "user"), testProducer(t, "addon")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			enrollTestTarget(t, target, addon)
			agent := newDispatchTestAgent()
			for _, step := range []struct {
				generation uint64
				action     string
				retry      bool
			}{
				{0, protocol.ActionPut, false}, {0, protocol.ActionPut, true},
				{1, protocol.ActionRemove, false}, {1, protocol.ActionRemove, true},
			} {
				root := ordinaryTestEvidence(t, user, purpose, ordinaryTestScope(step.generation, step.action))
				pkg := loggedTestPackage(t, root)
				if !step.retry && purpose == protocol.PredicateTypeManagedResourceV1 {
					pkg = loggedTestPackage(t, root, lookupRelationItem(t, addon, "kind.example/Cluster", "application/json").Evidence)
				}
				if step.retry {
					// Missing relation and unusable deployment manifests must not
					// prevent acknowledgement of the already completed immutable key.
					assertion := rawOrdinaryAssertion(t, purpose, ordinaryTestScope(step.generation, step.action))
					var body map[string]any
					if err := json.Unmarshal(assertion.Bytes, &body); err != nil {
						t.Fatal(err)
					}
					body["manifests"] = []map[string]any{{"bytes": "Y2hhbmdlZA=="}}
					body["spec"] = map[string]any{"changed": true}
					var err error
					assertion.Bytes, err = protocol.MarshalCanonical(body)
					if err != nil {
						t.Fatal(err)
					}
					root = lookupAssertionItem(t, user, assertion).Evidence
					pkg = loggedTestPackage(t, root, catalogItem("unused retry support", "").Evidence)
				}
				counted := &countedTarget{delegate: target}
				session, logs := relationLookupSession(t, pkg, counted)
				err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
				if err != nil {
					t.Fatal(err)
				}
				if agent.generations[ordinaryTestScope(0, protocol.ActionPut).FullResourceName] != step.generation {
					t.Fatal("generation marker lost")
				}
				view, applied := agent.Applied(ordinaryTestScope(0, protocol.ActionPut).FullResourceName)
				if applied != (step.action == protocol.ActionPut) {
					t.Fatal("retry changed removed/live state")
				}
				if applied && (len(view.Manifests) != 1 || string(view.Manifests[0].Bytes) != `{"region":"east"}`) {
					t.Fatal("retry replaced accepted content")
				}
				if step.retry && (counted.parseCalls != 1 || counted.beginCalls != 1 || counted.finishCalls != 1 || session.catalog.cursor != 0 || session.edgeCount != 0 || logs.calls[session.catalog.supporting[0]] != 0) {
					t.Fatal("retry performed supporting work")
				}
			}
		})
	}
}

func ordinaryTestScope(generation uint64, action string) protocol.DeliveryScope {
	return protocol.DeliveryScope{Tenant: sessionTenant(), TargetID: "target-test", FullResourceName: "resources.example/test-resource", Generation: generation, Action: action}
}

func TestCompletedOrdinaryKeyStillRequiresRootVerificationAndValidScope(t *testing.T) {
	for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1} {
		t.Run(string(purpose), func(t *testing.T) {
			user, addon := testProducer(t, "user"), testProducer(t, "addon")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			enrollTestTarget(t, target, addon)
			agent := newDispatchTestAgent()
			scope := ordinaryTestScope(1, protocol.ActionPut)
			root := ordinaryTestEvidence(t, user, purpose, scope)
			session, _ := relationLookupSession(t, loggedTestPackage(t, root, lookupRelationItem(t, addon, "kind.example/Cluster", "application/json").Evidence), target)
			if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
				t.Fatal(err)
			}

			for _, tc := range []struct {
				name   string
				edit   func(*protocol.DeliveryScope)
				tamper bool
				want   error
			}{
				{"invalid signature", nil, true, protocol.ErrVerificationFailed},
				{"invalid action", func(s *protocol.DeliveryScope) { s.Action = "invented" }, false, protocol.ErrMalformedEvidence},
				{"wrong target", func(s *protocol.DeliveryScope) { s.TargetID = "elsewhere" }, false, protocol.ErrPolicyReevaluation},
			} {
				t.Run(tc.name, func(t *testing.T) {
					retryScope := scope
					if tc.edit != nil {
						tc.edit(&retryScope)
					}
					retry := ordinaryTestEvidence(t, user, purpose, retryScope)
					if tc.tamper {
						var body directkey.SignatureBody
						if err := json.Unmarshal(retry.Bytes, &body); err != nil {
							t.Fatal(err)
						}
						body.Signature[0] ^= 1
						var err error
						retry.Bytes, err = protocol.MarshalCanonical(body)
						if err != nil {
							t.Fatal(err)
						}
					}
					// Inclusion remains valid for the tampered envelope; the native
					// verifier must reject its signature before the completed-key check.
					session, _ := relationLookupSession(t, loggedTestPackage(t, retry), target)
					err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
					if !errors.Is(err, tc.want) {
						t.Fatalf("retry error=%v, want %v", err, tc.want)
					}
					view, ok := agent.Applied(scope.FullResourceName)
					if !ok || agent.generations[scope.FullResourceName] != 1 || string(view.Manifests[0].Bytes) != `{"region":"east"}` {
						t.Fatal("invalid retry changed accepted state")
					}
				})
			}
		})
	}
}

func TestHigherManagedGenerationStillRequiresRelation(t *testing.T) {
	user, addon := testProducer(t, "user"), testProducer(t, "addon")
	target := directkey.NewTarget()
	enrollTestTarget(t, target, user)
	enrollTestTarget(t, target, addon)
	agent := newDispatchTestAgent()
	for _, step := range []struct {
		generation uint64
		action     string
		support    bool
		want       error
	}{
		{0, protocol.ActionPut, false, ErrFulfillmentRelationRequired},
		{0, protocol.ActionPut, true, nil},
		{1, protocol.ActionPut, false, ErrFulfillmentRelationRequired},
		{1, protocol.ActionPut, true, nil},
		{2, protocol.ActionRemove, false, ErrFulfillmentRelationRequired},
		{2, protocol.ActionRemove, true, nil},
		{1, protocol.ActionPut, false, ErrGeneration},
		{3, protocol.ActionPut, false, ErrFulfillmentRelationRequired},
	} {
		root := ordinaryTestEvidence(t, user, protocol.PredicateTypeManagedResourceV1, ordinaryTestScope(step.generation, step.action))
		pkg := loggedTestPackage(t, root)
		if step.support {
			pkg = loggedTestPackage(t, root, lookupRelationItem(t, addon, "kind.example/Cluster", "application/json").Evidence)
		}
		session, _ := relationLookupSession(t, pkg, target)
		before, existed := agent.generations[ordinaryTestScope(0, protocol.ActionPut).FullResourceName]
		err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
		if !errors.Is(err, step.want) {
			t.Fatalf("generation %d/%s: %v, want %v", step.generation, step.action, err, step.want)
		}
		current, exists := agent.generations[ordinaryTestScope(0, protocol.ActionPut).FullResourceName]
		if step.want != nil && (current != before || exists != existed) {
			t.Fatal("rejected work changed generation marker")
		}
		if step.want == nil && (!exists || current != step.generation) {
			t.Fatal("successful work did not advance generation")
		}
	}
	if _, applied := agent.Applied(ordinaryTestScope(0, protocol.ActionPut).FullResourceName); applied {
		t.Fatal("stale or unsupported work resurrected removed resource")
	}
}

func TestReservedRootPredicatesCannotBeClaimedByProfile(t *testing.T) {
	for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeFulfillmentRelationV1, protocol.PredicateTypeTrustConfigUpdateV1} {
		t.Run(string(purpose), func(t *testing.T) {
			user := testProducer(t, "user")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			assertion := rawRelationAssertion(t, "kind.example/Cluster", "application/json")
			assertion.PredicateType = purpose
			root := lookupAssertionItem(t, user, assertion).Evidence
			counted := &countedTarget{delegate: &claimingReservedTarget{TargetAPI: target}}
			session, _ := relationLookupSession(t, loggedTestPackage(t, root), counted)
			// Permit source verification for this common-owned predicate so
			// rejection is tested at dispatch, not hidden by source policy.
			if purpose == protocol.PredicateTypeTrustConfigUpdateV1 {
				authority := &session.trust.AuthorityRegistry[0]
				authority.DeliveryPolicies = append(authority.DeliveryPolicies, sessionPolicy(purpose, authority.ProvenanceProfiles[0]))
			}
			agent := newDispatchTestAgent()
			err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
			if !errors.Is(err, protocol.ErrUnknownPredicateType) {
				t.Fatalf("reserved root: %v", err)
			}
			if counted.finishCalls != 1 || counted.applyCalls != 0 || agent.suiteApplyCount != 0 || len(agent.applied) != 0 || len(agent.generations) != 0 {
				t.Fatal("reserved predicate failed to authenticate or reached effects")
			}
		})
	}
}

// Only ownership differs; all parsing and verification use the real profile.
type claimingReservedTarget struct{ protocol.TargetAPI }

func (*claimingReservedTarget) Owns(protocol.PredicateType) bool { return true }

func rawOrdinaryAssertion(t *testing.T, purpose protocol.PredicateType, scope protocol.DeliveryScope) protocol.TypedAssertion {
	t.Helper()
	// Raw wire content deliberately bypasses typed producer admission so target
	// scope validation and missing support remain observable through real signatures.
	base, err := protocol.MarshalCanonical(scope)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(base, &body); err != nil {
		t.Fatal(err)
	}
	body["resource_type"] = resourceTypeForTest(t, "kind.example/Cluster")
	body["spec"] = map[string]any{"region": "east"}
	body["manifests"] = []map[string]any{{"media_type": "application/json", "bytes": "eyJyZWdpb24iOiJlYXN0In0="}}
	encoded, err := protocol.MarshalCanonical(body)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.TypedAssertion{PredicateType: purpose, Bytes: encoded}
}

func ordinaryTestEvidence(t *testing.T, signer *directkey.Producer, purpose protocol.PredicateType, scope protocol.DeliveryScope) protocol.TypedEvidence {
	t.Helper()
	return lookupAssertionItem(t, signer, rawOrdinaryAssertion(t, purpose, scope)).Evidence
}
