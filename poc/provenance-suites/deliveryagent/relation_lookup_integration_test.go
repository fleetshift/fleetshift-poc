package deliveryagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/temporal"
)

func TestRequestedRelationVerificationHasNoAlternativeFallback(t *testing.T) {
	for _, tc := range []struct {
		name         string
		firstBad     bool
		badInclusion bool
		badSignature bool
	}{
		{"good first, later unenrolled claim", false, false, false},
		{"unenrolled first, later good claim", true, false, false},
		{"good first, later bad inclusion", false, true, false},
		{"bad inclusion first, later good claim", true, true, false},
		{"good first, later bad signature", false, false, true},
		{"bad signature first, later good claim", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, addon, rogue := testProducer(t, "user"), testProducer(t, "addon"), testProducer(t, "rogue")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			enrollTestTarget(t, target, addon)
			good := lookupRelationItem(t, addon, "kind.example/Cluster", "application/good")
			badSigner := rogue
			if tc.badInclusion || tc.badSignature {
				badSigner = addon
			}
			bad := lookupRelationItem(t, badSigner, "kind.example/Cluster", "application/bad")
			if tc.badSignature {
				var body directkey.SignatureBody
				if err := json.Unmarshal(bad.Evidence.Bytes, &body); err != nil {
					t.Fatal(err)
				}
				body.Signature[0] ^= 1
				var err error
				bad.Evidence.Bytes, err = protocol.MarshalCanonical(body)
				if err != nil {
					t.Fatal(err)
				}
			}
			first, second := good, bad
			if tc.firstBad {
				first, second = bad, good
			}
			root := lookupManagedRoot(t, user, "kind.example/Cluster")
			pkg := loggedTestPackage(t, root, first.Evidence, second.Evidence)
			badIndex := 1
			if !tc.firstBad {
				badIndex = 2
			}
			if tc.badInclusion {
				pkg.Supporting[badIndex-1].EvidenceLog.InclusionProof[0] = protocol.DigestBytes([]byte("bad inclusion"))
			}
			counted := &countedTarget{delegate: target}
			session, logs := relationLookupSession(t, pkg, counted)
			catalog := session.catalog
			agent := newDispatchTestAgent()
			err := session.withNode(context.Background(), catalog.rootID, func(root verifiedNode) error {
				return agent.dispatchApplyLocked(session, root)
			})
			if tc.firstBad {
				want := protocol.ErrVerificationFailed
				if tc.badInclusion {
					want = protocol.ErrInvalidLogInclusion
				}
				if !errors.Is(err, want) {
					t.Fatalf("selected failure=%v, want %v", err, want)
				}
				if session.edgeCount != 0 || len(agent.applied) != 0 {
					t.Fatal("failed claim reached dependency accounting or apply")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				view, ok := agent.Applied("//fleetshift.io/clusters/test")
				if !ok || len(view.Manifests) != 1 || view.Manifests[0].MediaType != "application/good" || string(view.Manifests[0].Bytes) != `{"region":"east"}` {
					t.Fatalf("applied view=%+v", view)
				}
				if session.edgeCount != 1 {
					t.Fatal("requested relation did not become the sole dependency")
				}
			}
			if counted.parseCalls != 3 || counted.beginCalls != 2 || logs.calls[catalog.supporting[1]] != 0 {
				t.Fatalf("unexpected extra work: parse=%d begin=%d later inclusion=%d", counted.parseCalls, counted.beginCalls, logs.calls[catalog.supporting[1]])
			}
		})
	}
}

func TestRequestedRelationUsesAuthenticatedContentAfterTentativeLookup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		actualType string
		media      protocol.MediaType
		purpose    protocol.PredicateType
		want       error
	}{
		{"false claimed service", "other.example/Cluster", "application/json", protocol.PredicateTypeFulfillmentRelationV1, protocol.ErrPolicyReevaluation},
		{"invalid authenticated key", "local-kind", "application/json", protocol.PredicateTypeFulfillmentRelationV1, protocol.ErrMalformedEvidence},
		{"missing authenticated media", "kind.example/Cluster", "", protocol.PredicateTypeFulfillmentRelationV1, protocol.ErrMalformedEvidence},
		{"false claimed predicate", "kind.example/Cluster", "application/json", protocol.PredicateTypeDeploymentV1, protocol.ErrPolicyReevaluation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, addon := testProducer(t, "user"), testProducer(t, "addon")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			enrollTestTarget(t, target, addon)
			actual := rawRelationAssertion(t, tc.actualType, tc.media)
			actual.PredicateType = tc.purpose
			item := lookupAssertionItem(t, addon, actual)
			root := lookupManagedRoot(t, user, "kind.example/Cluster")
			pkg := loggedTestPackage(t, root, item.Evidence)
			claim, err := (protocol.FulfillmentRelation{ResourceType: resourceTypeForTest(t, "kind.example/Cluster"), MediaType: "application/tentative"}).Assertion()
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &hintTarget{TargetAPI: target, kind: target.ProvenanceType(), edit: func(hints *protocol.TentativeHints) {
				if hints.Assertion.PredicateType != protocol.PredicateTypeManagedResourceV1 {
					hints.Assertion = claim
				}
			}}
			session, _ := relationLookupSession(t, pkg, wrapped)
			catalog := session.catalog
			agent := newDispatchTestAgent()
			err = session.withNode(context.Background(), catalog.rootID, func(root verifiedNode) error {
				return agent.dispatchApplyLocked(session, root)
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("semantic failure=%v, want %v", err, tc.want)
			}
			if session.edgeCount != 0 || len(agent.applied) != 0 {
				t.Fatal("tentative assertion became a semantic dependency or reached apply")
			}
		})
	}
}

func lookupManagedRoot(t *testing.T, signer *directkey.Producer, kind string) protocol.TypedEvidence {
	t.Helper()
	assertion, err := (protocol.ManagedResourceAuthorization{
		DeliveryScope: protocol.DeliveryScope{Tenant: signer.Principal().Tenant(), TargetID: "target-test", FullResourceName: "//fleetshift.io/clusters/test", Generation: 1, Action: protocol.ActionPut},
		ResourceType:  resourceTypeForTest(t, kind), Spec: []byte(`{"region":"east"}`),
	}).Assertion()
	if err != nil {
		t.Fatal(err)
	}
	return lookupAssertionItem(t, signer, assertion).Evidence
}

// rawManagedRoot lets the native producer sign malformed type syntax to test
// parsing at the target boundary, without constructing an invalid ResourceType.
func rawManagedRoot(t *testing.T, signer *directkey.Producer, kind string) protocol.TypedEvidence {
	t.Helper()
	encoded, err := protocol.MarshalCanonical(struct {
		protocol.DeliveryScope
		ResourceType string          `json:"resource_type"`
		Spec         json.RawMessage `json:"spec"`
	}{
		DeliveryScope: protocol.DeliveryScope{Tenant: signer.Principal().Tenant(), TargetID: "target-test", FullResourceName: "//fleetshift.io/clusters/test", Generation: 1, Action: protocol.ActionPut},
		ResourceType:  kind, Spec: []byte(`{"region":"east"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return lookupAssertionItem(t, signer, protocol.TypedAssertion{PredicateType: protocol.PredicateTypeManagedResourceV1, Bytes: encoded}).Evidence
}

func TestManagedResourceIgnoresUnusableAndUnrelatedSupport(t *testing.T) {
	for _, kind := range []string{"unknown native type", "malformed native envelope", "missing native purpose", "malformed common predicate", "invalid resource key", "opaque suite predicate", "unrelated bad inclusion"} {
		for _, trailing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/trailing=%t", kind, trailing), func(t *testing.T) {
				user, addon := testProducer(t, "user"), testProducer(t, "addon")
				target := directkey.NewTarget()
				enrollTestTarget(t, target, user)
				enrollTestTarget(t, target, addon)
				root := lookupManagedRoot(t, user, "kind.example/Cluster")
				good := lookupRelationItem(t, addon, "kind.example/Cluster", "application/json")
				decoy := lookupRelationItem(t, addon, "other.example/Cluster", "application/json")
				switch kind {
				case "unknown native type":
					decoy.Evidence.ProvenanceType = "uninstalled/v1"
				case "malformed native envelope":
					decoy.Evidence.Bytes = []byte("not native JSON")
				case "missing native purpose":
					var body directkey.SignatureBody
					if err := json.Unmarshal(decoy.Evidence.Bytes, &body); err != nil {
						t.Fatal(err)
					}
					body.Assertion.PredicateType = ""
					var err error
					decoy.Evidence.Bytes, err = protocol.MarshalCanonical(body)
					if err != nil {
						t.Fatal(err)
					}
				case "malformed common predicate":
					decoy = lookupAssertionItem(t, addon, protocol.TypedAssertion{PredicateType: protocol.PredicateTypeFulfillmentRelationV1, Bytes: []byte("not common JSON")})
				case "invalid resource key":
					decoy = lookupAssertionItem(t, addon, rawRelationAssertion(t, "local-kind", "application/json"))
				case "opaque suite predicate":
					decoy = lookupAssertionItem(t, addon, protocol.TypedAssertion{PredicateType: "suite/opaque", Bytes: []byte("opaque event")})
				}
				first, second := decoy, good
				decoyIndex := 0
				if trailing {
					first, second, decoyIndex = good, decoy, 1
				}
				pkg := loggedTestPackage(t, root, first.Evidence, second.Evidence)
				if kind == "unrelated bad inclusion" {
					pkg.Supporting[decoyIndex].EvidenceLog.InclusionProof[0] = protocol.DigestBytes([]byte("bad unused inclusion"))
				}
				counted := &countedTarget{delegate: target}
				session, logs := relationLookupSession(t, pkg, counted)
				agent := newDispatchTestAgent()
				if err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) }); err != nil {
					t.Fatal(err)
				}
				if _, ok := agent.Applied("//fleetshift.io/clusters/test"); !ok {
					t.Fatal("valid requested relation did not reach apply")
				}
				wantParse := 3
				if !trailing && kind != "unknown native type" {
					wantParse++
				}
				if counted.parseCalls != wantParse || counted.beginCalls != 2 || logs.calls[session.catalog.supporting[decoyIndex]] != 0 || session.edgeCount != 1 {
					t.Fatalf("unused support did extra work: parse=%d begin=%d unused inclusion=%d edges=%d", counted.parseCalls, counted.beginCalls, logs.calls[session.catalog.supporting[decoyIndex]], session.edgeCount)
				}
			})
		}
	}
}

func TestManagedRootValidatesResourceTypeDespiteNativeProducerBypass(t *testing.T) {
	user := testProducer(t, "user")
	target := directkey.NewTarget()
	enrollTestTarget(t, target, user)
	for _, kind := range []string{"clusters", "kind.example/v1/Cluster", "kind.example/Clus ter"} {
		t.Run(kind, func(t *testing.T) {
			root := rawManagedRoot(t, user, kind)
			counted := &countedTarget{delegate: target}
			session, _ := relationLookupSession(t, loggedTestPackage(t, root), counted)
			agent := newDispatchTestAgent()
			err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
			if !errors.Is(err, protocol.ErrMalformedEvidence) {
				t.Fatalf("invalid signed root type=%v", err)
			}
			if counted.parseCalls != 1 || counted.beginCalls != 1 || len(agent.applied) != 0 {
				t.Fatal("invalid root reached supporting work or apply")
			}
		})
	}
}

func TestRequestedRelationPreparationLimitRejectsWithoutFallbackOrApply(t *testing.T) {
	user, addon := testProducer(t, "user"), testProducer(t, "addon")
	target := directkey.NewTarget()
	enrollTestTarget(t, target, user)
	enrollTestTarget(t, target, addon)
	root := lookupManagedRoot(t, user, "kind.example/Cluster")
	first := lookupRelationItem(t, addon, "kind.example/Cluster", "application/first")
	later := lookupRelationItem(t, addon, "kind.example/Cluster", "application/later")
	counted := &countedTarget{delegate: &overLimitRelationTarget{TargetAPI: target}}
	session, logs := relationLookupSession(t, loggedTestPackage(t, root, first.Evidence, later.Evidence), counted)
	// A second profile makes an attempted profile fallback observable even
	// though the direct-key implementation would reject these parameters.
	laterProfile := protocol.ProfileConfig{ProvenanceType: target.ProvenanceType(), Parameters: []byte(`{"later":true}`)}
	trust := sessionTestTrust()
	authority := &trust.AuthorityRegistry[0]
	authority.DeliveryPolicies = append(authority.DeliveryPolicies, sessionPolicy(protocol.PredicateTypeManagedResourceV1, authority.ProvenanceProfiles[0]))
	authority.ProvenanceProfiles = append(authority.ProvenanceProfiles, laterProfile)
	authority.DeliveryPolicies[1].Profiles = append(authority.DeliveryPolicies[1].Profiles, profileDigest(laterProfile))
	session = newVerificationSession(session.catalog, trust, session.temporal)
	agent := newDispatchTestAgent()
	err := session.withNode(context.Background(), session.catalog.rootID, func(root verifiedNode) error { return agent.dispatchApplyLocked(session, root) })
	if !errors.Is(err, protocol.ErrInvalidTimestampBinding) || errors.Is(err, protocol.ErrNoSuccessfulProfile) {
		t.Fatalf("preparation limit did not propagate directly: %v", err)
	}
	if counted.parseCalls != 3 || counted.beginCalls != 2 || counted.finishCalls != 1 || session.catalog.cursor != 1 {
		t.Fatalf("fallback work: parse=%d begin=%d finish=%d cursor=%d", counted.parseCalls, counted.beginCalls, counted.finishCalls, session.catalog.cursor)
	}
	if logs.calls[session.catalog.supporting[0]] != 0 || logs.calls[session.catalog.supporting[1]] != 0 || session.edgeCount != 0 || len(agent.applied) != 0 {
		t.Fatal("over-limit preparation reached supporting occurrence, dependency accounting, or apply")
	}
}

// overLimitRelationTarget keeps real native parsing and verification sessions,
// adding excessive temporal preparation only to fulfillment-relation attempts.
type overLimitRelationTarget struct{ protocol.TargetAPI }

func (t *overLimitRelationTarget) BeginVerification(ctx context.Context, request protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	session, err := t.TargetAPI.BeginVerification(ctx, request)
	if err != nil || request.DeliveryContext.PredicateType != protocol.PredicateTypeFulfillmentRelationV1 {
		return session, err
	}
	return &overLimitPreparationSession{ProvenanceVerificationSession: session}, nil
}

type overLimitPreparationSession struct {
	protocol.ProvenanceVerificationSession
}

func (s *overLimitPreparationSession) Prepare(ctx context.Context) (protocol.TemporalPreparation, error) {
	preparation, err := s.ProvenanceVerificationSession.Prepare(ctx)
	if err != nil {
		return preparation, err
	}
	preparation.Timestamps = append(preparation.Timestamps, protocol.UnverifiedTimestampBinding{
		Format: protocol.TimestampFormatRFC3161V1,
		Token:  make([]byte, protocol.MaxTimestampTokenBytes+1),
	})
	return preparation, nil
}

func relationLookupSession(t *testing.T, pkg resourcemanager.DeliveryPackage, target protocol.TargetAPI) (*verificationSession, *countedLogVerifier) {
	t.Helper()
	catalog, err := newEvidenceCatalog(pkg, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.ProvenanceType(): target}), defaultVerificationLimits())
	if err != nil {
		t.Fatal(err)
	}
	root := catalog.item(catalog.rootID)
	prepared, err := temporal.Prepare(temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, catalog.update, root.Evidence, root.EvidenceLog)
	if err != nil {
		t.Fatal(err)
	}
	logs := &countedLogVerifier{inner: prepared.Log, calls: make(map[protocol.Digest]int)}
	trust := sessionTestTrust()
	trust.AuthorityRegistry[0].DeliveryPolicies = append(trust.AuthorityRegistry[0].DeliveryPolicies, sessionPolicy(protocol.PredicateTypeManagedResourceV1, trust.AuthorityRegistry[0].ProvenanceProfiles[0]))
	return newVerificationSession(catalog, trust, protocol.TemporalVerificationServices{Log: logs}), logs
}
