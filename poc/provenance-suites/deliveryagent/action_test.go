package deliveryagent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

func TestDeliverRetainsCompleteAuthorizedActionAndDetachedObservation(t *testing.T) {
	for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1} {
		t.Run(string(purpose), func(t *testing.T) {
			user, addon := testProducer(t, "user"), testProducer(t, "addon")
			agent, err := New(Config{Tenant: sessionTenant(), ProviderTenant: sessionTenant(), TargetID: "target-test"})
			if err != nil {
				t.Fatal(err)
			}
			trust := sessionTestTrust()
			authority := &trust.AuthorityRegistry[0]
			authority.DeliveryPolicies = append(authority.DeliveryPolicies, sessionPolicy(protocol.PredicateTypeManagedResourceV1, authority.ProvenanceProfiles[0]))
			if err := agent.Bootstrap(trust); err != nil {
				t.Fatal(err)
			}
			enrollTestTarget(t, agent.profile, user)
			enrollTestTarget(t, agent.profile, addon)
			scope := ordinaryTestScope(0, protocol.ActionPut)
			root := ordinaryTestEvidence(t, user, purpose, scope)
			relation := lookupRelationItem(t, addon, "kind.example/Cluster", "application/json")
			// Unused couriered evidence is structurally sound but cannot be
			// parsed by an installed implementation, and must supply no basis.
			pkg := loggedTestPackage(t, root, relation.Evidence, catalogItem("unused", "").Evidence)
			if err := agent.Deliver(pkg); err != nil {
				t.Fatal(err)
			}
			want := AuthorizedAction{
				RootEvidence: evidenceIdentity(t, root), Principal: user.Principal(), Scope: scope, PredicateType: purpose,
				Manifests: []protocol.TypedManifest{{MediaType: "application/json", Bytes: []byte(`{"region":"east"}`)}},
				Basis:     []protocol.Digest{evidenceIdentity(t, root)},
			}
			if purpose == protocol.PredicateTypeManagedResourceV1 {
				want.Basis = append(want.Basis, evidenceIdentity(t, relation.Evidence))
			}
			slices.Sort(want.Basis)
			view, ok := agent.Applied(scope.FullResourceName)
			if !ok {
				t.Fatal("delivery did not retain its action")
			}
			assertAuthorizedAction(t, view.AuthorizedAction, want)
			if agent.Checkpoint() != pkg.EvidenceLog.Checkpoint || agent.SuiteApplyCount() != 0 {
				t.Fatal("ordinary action changed checkpoint or suite-owned dispatch")
			}
			view.RootEvidence = "changed"
			view.Principal.Subject = "changed"
			view.Scope.Generation++
			view.PredicateType = "changed"
			view.Manifests[0].MediaType = "changed"
			view.Manifests[0].Bytes[0] ^= 1
			view.Basis[0] = "changed"
			again, _ := agent.Applied(scope.FullResourceName)
			assertAuthorizedAction(t, again.AuthorizedAction, want)
		})
	}
}

func TestRemovalActionIsCompleteBeforeApplyAndOnlyGenerationRemains(t *testing.T) {
	for _, purpose := range []protocol.PredicateType{protocol.PredicateTypeDeploymentV1, protocol.PredicateTypeManagedResourceV1} {
		t.Run(string(purpose), func(t *testing.T) {
			user, addon := testProducer(t, "user"), testProducer(t, "addon")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			enrollTestTarget(t, target, addon)
			agent := newDispatchTestAgent()
			scope := ordinaryTestScope(2, protocol.ActionRemove)
			root := ordinaryTestEvidence(t, user, purpose, scope)
			relation := lookupRelationItem(t, addon, "kind.example/Cluster", "application/json")
			session, _ := relationLookupSession(t, loggedTestPackage(t, root, relation.Evidence), target)
			var action AuthorizedAction
			err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error {
				var required bool
				var err error
				if purpose == protocol.PredicateTypeDeploymentV1 {
					action, required, err = agent.handleDeploymentLocked(session, root)
				} else {
					action, required, err = agent.handleManagedResourceLocked(session, root)
				}
				if err == nil && !required {
					t.Fatal("new removal was treated as completed")
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			want := AuthorizedAction{
				RootEvidence: evidenceIdentity(t, root), Principal: user.Principal(), Scope: scope, PredicateType: purpose,
				Manifests: []protocol.TypedManifest{{MediaType: "application/json", Bytes: []byte(`{"region":"east"}`)}},
				Basis:     []protocol.Digest{evidenceIdentity(t, root)},
			}
			if purpose == protocol.PredicateTypeManagedResourceV1 {
				want.Basis = append(want.Basis, evidenceIdentity(t, relation.Evidence))
			}
			slices.Sort(want.Basis)
			assertAuthorizedAction(t, action, want)
			if len(agent.generations) != 0 || len(agent.applied) != 0 {
				t.Fatal("derivation performed effects")
			}
			if err := agent.applyLocked(action); err != nil {
				t.Fatal(err)
			}
			if _, exists := agent.Applied(scope.FullResourceName); exists || agent.generations[scope.FullResourceName] != scope.Generation || len(agent.applied) != 0 {
				t.Fatal("removal retained a live action or lost its generation")
			}
			// A fresh session authenticates the retry root, but no support is
			// necessary and the handler must produce no replacement action.
			retry, _ := relationLookupSession(t, loggedTestPackage(t, root), target)
			err = retry.withNode(context.Background(), retry.catalog.rootID, func(root verifiedNode) error {
				var action AuthorizedAction
				var required bool
				var err error
				if purpose == protocol.PredicateTypeDeploymentV1 {
					action, required, err = agent.handleDeploymentLocked(retry, root)
				} else {
					action, required, err = agent.handleManagedResourceLocked(retry, root)
				}
				assertAuthorizedAction(t, action, AuthorizedAction{})
				if required || retry.actionBasis != nil {
					t.Fatal("completed removal derived new work")
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestActionOnlyApplyFencesGenerationsAndPreservesCompletedAction(t *testing.T) {
	agent := newDispatchTestAgent()
	action := AuthorizedAction{
		RootEvidence: "original-root", Principal: protocol.Principal{Subject: "original"},
		Scope: ordinaryTestScope(2, protocol.ActionPut), PredicateType: protocol.PredicateTypeDeploymentV1,
		Manifests: []protocol.TypedManifest{{MediaType: "application/octet-stream", Bytes: []byte{0, 255}}},
		Basis:     []protocol.Digest{"original-root"},
	}
	// This private seam receives already derived common values; apply has no
	// assertion or evidence input to decode or compare.
	if err := agent.applyLocked(action); err != nil {
		t.Fatal(err)
	}
	retry := AuthorizedAction{RootEvidence: "different-root", Scope: action.Scope, PredicateType: protocol.PredicateTypeManagedResourceV1}
	if err := agent.applyLocked(retry); err != nil {
		t.Fatal(err)
	}
	view, _ := agent.Applied(action.Scope.FullResourceName)
	assertAuthorizedAction(t, view.AuthorizedAction, action)
	retry.Scope.Generation--
	if err := agent.applyLocked(retry); !errors.Is(err, ErrGeneration) {
		t.Fatalf("stale action: %v", err)
	}
	view, _ = agent.Applied(action.Scope.FullResourceName)
	assertAuthorizedAction(t, view.AuthorizedAction, action)
	action.Scope.Generation++
	action.Scope.Action = protocol.ActionRemove
	if err := agent.applyLocked(action); err != nil {
		t.Fatal(err)
	}
	if err := agent.applyLocked(action); err != nil {
		t.Fatal(err)
	}
	if len(agent.applied) != 0 || agent.generations[action.Scope.FullResourceName] != 3 {
		t.Fatal("removal failed to preserve only the high-water mark")
	}
}

func TestConcurrentDeliveriesRetainNewestGeneration(t *testing.T) {
	user := testProducer(t, "user")
	agent, err := New(Config{Tenant: sessionTenant(), TargetID: "target-test"})
	if err != nil {
		t.Fatal(err)
	}
	trust := sessionTestTrust()
	trust.AuthorityRegistry[0].DeliveryPolicies[0].RequireEvidenceLog = false
	if err := agent.Bootstrap(trust); err != nil {
		t.Fatal(err)
	}
	enrollTestTarget(t, agent.profile, user)
	start := make(chan struct{})
	results := make(chan error, 2)
	var newest protocol.TypedEvidence
	for _, generation := range []uint64{1, 2} {
		root := ordinaryTestEvidence(t, user, protocol.PredicateTypeDeploymentV1, ordinaryTestScope(generation, protocol.ActionPut))
		if generation == 2 {
			newest = root
		}
		go func() {
			<-start
			results <- agent.Deliver(resourcemanager.DeliveryPackage{Root: protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: root}}})
		}()
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil && !errors.Is(err, ErrGeneration) {
			t.Fatal(err)
		}
	}
	view, ok := agent.Applied(ordinaryTestScope(2, protocol.ActionPut).FullResourceName)
	if !ok || view.Scope.Generation != 2 || view.RootEvidence != evidenceIdentity(t, newest) {
		t.Fatal("concurrent deliveries regressed the accepted action")
	}
}

func TestManagedActionUsesExactAuthenticatedSpecAndRelationMedia(t *testing.T) {
	user, addon := testProducer(t, "user"), testProducer(t, "addon")
	target := directkey.NewTarget()
	enrollTestTarget(t, target, user)
	enrollTestTarget(t, target, addon)
	scope := ordinaryTestScope(4, protocol.ActionPut)
	encodedScope, err := json.Marshal(scope)
	if err != nil {
		t.Fatal(err)
	}
	// Native signing accepts these exact noncanonical assertion bytes. The
	// manifest must preserve the authenticated spec's internal whitespace.
	spec := `{ "region" : "east", "size" : 2 }`
	body := string(encodedScope[:len(encodedScope)-1]) + `,"resource_type":"kind.example/Cluster","spec":` + spec + `}`
	root := lookupAssertionItem(t, user, protocol.TypedAssertion{PredicateType: protocol.PredicateTypeManagedResourceV1, Bytes: []byte(body)}).Evidence
	relation := lookupRelationItem(t, addon, "kind.example/Cluster", "application/authenticated")
	tentative := rawRelationAssertion(t, "kind.example/Cluster", "application/tentative")
	wrapped := &hintTarget{TargetAPI: target, kind: target.ProvenanceType(), edit: func(hints *protocol.TentativeHints) {
		if hints.Assertion.PredicateType == protocol.PredicateTypeFulfillmentRelationV1 {
			hints.Assertion = tentative
		}
	}}
	session, _ := relationLookupSession(t, loggedTestPackage(t, root, relation.Evidence), wrapped)
	agent := newDispatchTestAgent()
	if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
		t.Fatal(err)
	}
	want := AuthorizedAction{
		RootEvidence: evidenceIdentity(t, root), Principal: user.Principal(), Scope: scope, PredicateType: protocol.PredicateTypeManagedResourceV1,
		Manifests: []protocol.TypedManifest{{MediaType: "application/authenticated", Bytes: []byte(spec)}},
		Basis:     []protocol.Digest{evidenceIdentity(t, root), evidenceIdentity(t, relation.Evidence)},
	}
	slices.Sort(want.Basis)
	view, ok := agent.Applied(scope.FullResourceName)
	if !ok {
		t.Fatal("managed action was not applied")
	}
	assertAuthorizedAction(t, view.AuthorizedAction, want)
}

func assertAuthorizedAction(t *testing.T, got, want AuthorizedAction) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("action=%+v, want %+v", got, want)
	}
}
