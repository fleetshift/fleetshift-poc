package deliveryagent

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stolostron/ocm-fleet/poc/provenance-suites/directkey"
	"github.com/stolostron/ocm-fleet/poc/provenance-suites/protocol"
	"github.com/stolostron/ocm-fleet/poc/provenance-suites/resourcemanager"
	"github.com/stolostron/ocm-fleet/poc/provenance-suites/temporal"
)

func TestAgentCatalogFailurePrecedesLogTransitionAndApply(t *testing.T) {
	agent, err := New(Config{Tenant: sessionTenant(), ProviderTenant: sessionTenant(), TargetID: "target-test"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agent.Bootstrap(sessionTestTrust()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	root := catalogItem("duplicate", "root")
	duplicate := cloneCatalogItem(root)
	// If temporal.Prepare ran first, the missing update would mask the catalog
	// error. Duplicate identity must be rejected before any log transition.
	before := agent.Checkpoint()
	err = agent.Deliver(resourcemanager.DeliveryPackage{Root: root, Supporting: []protocol.Item{duplicate}})
	if !errors.Is(err, errDuplicateEvidenceIdentity) {
		t.Fatalf("Deliver error = %v, want duplicate identity before log preparation", err)
	}
	if got := agent.Checkpoint(); got != before {
		t.Fatalf("checkpoint changed on catalog failure: got %+v, want %+v", got, before)
	}
	if len(agent.memory.applied) != 0 || agent.suiteApplyCount != 0 {
		t.Fatalf("catalog failure reached apply: applied=%d suiteApply=%d", len(agent.memory.applied), agent.suiteApplyCount)
	}
}

func TestFulfillmentRelationChecksProviderReferenceAfterAuthentication(t *testing.T) {
	otherAuthority := sessionTenant()
	otherAuthority.Authority = "https://other.example.test"
	for _, tc := range []struct {
		name     string
		provider protocol.Tenant
		matches  bool
	}{
		{"platform provider", sessionTenant(), true},
		{"other partition", otherSessionTenant(), false},
		{"other authority", otherAuthority, false},
		{"unprovisioned", protocol.Tenant{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user := testProducer(t, "addon")
			target := directkey.NewTarget()
			enrollTestTarget(t, target, user)
			counted := &countedTarget{delegate: target}
			rootEvidence, err := user.CreateEvidence(context.Background(), protocol.TypedAssertion{PredicateType: protocol.PredicateTypeDeploymentV1, Bytes: []byte(`{"root":true}`)})
			if err != nil {
				t.Fatal(err)
			}
			want := protocol.FulfillmentRelation{ResourceType: resourceTypeForTest(t, "test.example/Cluster"), MediaType: "application/json"}
			assertion, err := want.Assertion()
			if err != nil {
				t.Fatal(err)
			}
			relationEvidence, err := user.CreateEvidence(context.Background(), assertion)
			if err != nil {
				t.Fatal(err)
			}
			pkg := loggedTestPackage(t, rootEvidence, relationEvidence)
			catalog, err := newEvidenceCatalog(pkg, func(kind protocol.ProvenanceType) (protocol.TargetAPI, bool) {
				return counted, kind == counted.ProvenanceType()
			}, defaultVerificationLimits())
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := temporal.Prepare(temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, catalog.update, rootEvidence, pkg.Root.EvidenceLog)
			if err != nil {
				t.Fatal(err)
			}
			session := newVerificationSession(catalog, sessionTestTrust(), protocol.TemporalVerificationServices{Log: prepared.Log})
			// Authentication is independent of each assertion's semantic use.
			for _, id := range []protocol.Digest{catalog.rootID, catalog.supporting[0]} {
				if _, err := session.verifyNode(context.Background(), id); err != nil {
					t.Fatal(err)
				}
			}
			agent := newDispatchTestAgent()
			agent.config.ProviderTenant = tc.provider
			got, err := agent.relationForTest(session, catalog.rootID, protocol.ManagedResourceAuthorization{ResourceType: want.ResourceType})
			if tc.matches {
				if err != nil || got != want {
					t.Fatalf("relation = %+v, error = %v", got, err)
				}
				if session.edgeCount != 1 {
					t.Fatal("matching provider relation did not become a selected dependency")
				}
			} else {
				if !errors.Is(err, protocol.ErrTenantMismatch) {
					t.Fatalf("provider mismatch error = %v", err)
				}
				if session.edgeCount != 0 {
					t.Fatal("wrong provider relation became a selected dependency")
				}
			}
			if counted.beginCalls != 2 || counted.finishCalls != 2 {
				t.Fatalf("profile calls = Begin %d, Finish %d; want two cached authentications", counted.beginCalls, counted.finishCalls)
			}
		})
	}
}

func TestIntentTenantBindingPrecedesApply(t *testing.T) {
	for _, tc := range []struct {
		name          string
		scopeMismatch bool
	}{
		{"signed scope", true}, {"authenticated principal", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, rootID, _ := newBareSession(t, defaultVerificationLimits(), 1)
			root := deploymentRootForDispatchTest(t, rootID)
			if tc.scopeMismatch {
				authorization, err := protocol.DecodeDeploymentAuthorization(root.result.Assertion)
				if err != nil {
					t.Fatal(err)
				}
				authorization.Tenant.Partition = "other"
				root.result.Assertion, err = authorization.Assertion()
				if err != nil {
					t.Fatal(err)
				}
			} else {
				root.result.Authenticated.Principal.TenantPartition = "other"
			}
			agent := newDispatchTestAgent()
			err := agent.dispatchForTest(session, root)
			if tc.scopeMismatch {
				if !errors.Is(err, protocol.ErrPolicyReevaluation) {
					t.Fatalf("scope mismatch error = %v", err)
				}
			} else if !errors.Is(err, protocol.ErrTenantMismatch) {
				t.Fatalf("principal mismatch error = %v", err)
			}
			if len(agent.memory.applied) != 0 || len(agent.memory.generations) != 0 {
				t.Fatal("tenant mismatch reached apply")
			}
		})
	}
}

func TestAgentAuthenticatesFulfillmentRelationRootButRejectsApply(t *testing.T) {
	agent, err := New(Config{Tenant: sessionTenant(), ProviderTenant: sessionTenant(), TargetID: "target-test"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := agent.Bootstrap(sessionTestTrust()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	user := testProducer(t, "addon")
	enrollTestTarget(t, agent.profile, user)
	assertion, err := (protocol.FulfillmentRelation{
		ResourceType: resourceTypeForTest(t, "test.example/Cluster"),
		MediaType:    "application/json",
	}).Assertion()
	if err != nil {
		t.Fatalf("create fulfillment relation assertion: %v", err)
	}
	evidence, err := user.CreateEvidence(context.Background(), assertion)
	if err != nil {
		t.Fatalf("create fulfillment relation: %v", err)
	}
	if err := agent.Deliver(loggedTestPackage(t, evidence)); !errors.Is(err, protocol.ErrUnknownPredicateType) {
		t.Fatalf("Deliver error = %v, want predicate dispatch to reject the authenticated relation", err)
	}
	if len(agent.memory.applied) != 0 || len(agent.memory.generations) != 0 || agent.suiteApplyCount != 0 {
		t.Fatalf("relation root reached apply: applied=%d generations=%d suiteApply=%d", len(agent.memory.applied), len(agent.memory.generations), agent.suiteApplyCount)
	}
}

func TestSuiteApplyFailureRejectsDeliveryAfterVerifiedCheckpoint(t *testing.T) {
	agent, err := New(Config{Tenant: sessionTenant(), TargetID: "target-test"})
	if err != nil {
		t.Fatal(err)
	}
	trust := sessionTestTrust()
	authority := &trust.AuthorityRegistry[0]
	authority.DeliveryPolicies = append(authority.DeliveryPolicies, sessionPolicy(directkey.PredicateTypeEnrollmentV1, authority.ProvenanceProfiles[0]))
	if err := agent.Bootstrap(trust); err != nil {
		t.Fatal(err)
	}
	existing, replacement := testProducer(t, "user"), testProducer(t, "user")
	enrollTestTarget(t, agent.profile, existing)
	before, _ := agent.PublicKey(existing.Principal())
	root, err := replacement.CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	counted := &countedTarget{delegate: agent.profile}
	agent.lookup = catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{counted.ProvenanceType(): counted})
	pkg := loggedTestPackage(t, root)
	agent.LoseNextAcknowledgement()
	// Native enrollment authentication succeeds, but the real Apply refuses
	// to replace an established key. Failure must precede acknowledgement.
	if err := agent.Deliver(pkg); !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("suite apply failure: %v", err)
	}
	after, _ := agent.PublicKey(existing.Principal())
	if !bytes.Equal(before, after) || counted.beginCalls != 1 || counted.finishCalls != 1 || counted.applyCalls != 1 || agent.SuiteApplyCount() != 1 || !agent.loseNextAcknowledgement {
		t.Fatal("suite failure changed the key, skipped verification, or reached acknowledgement")
	}
	if agent.Checkpoint() != pkg.EvidenceLog.Checkpoint || len(agent.memory.applied) != 0 || len(agent.memory.generations) != 0 {
		t.Fatal("suite failure lost the verified checkpoint or reached ordinary effects")
	}
	request := counted.applyRequests[0]
	if request.Temporal.LogPosition == nil || request.Temporal.LogPosition.Index != pkg.Root.EvidenceLog.Index || request.Authenticated.Principal != replacement.Principal() {
		t.Fatal("suite apply did not receive finalized root verification facts")
	}
}

// These helpers compose the real verifier and application at the unit-test
// boundary. They let graph fixtures inspect derivation independently of effects.
func newDispatchTestAgent() *Agent {
	memory := NewMemoryApplication()
	return &Agent{
		Verifier:    &Verifier{config: Config{Tenant: sessionTenant(), ProviderTenant: sessionTenant(), TargetID: "target-test"}},
		application: memory,
		memory:      memory,
	}
}

func (a *Agent) dispatchForTest(session *verificationSession, root verifiedNode) error {
	attempt := a.application.NewAttempt()
	defer attempt.Close()
	result, err := a.evaluateRoot(context.Background(), session, root, attempt.(GenerationPrechecker).Precheck)
	if err != nil {
		return err
	}
	return a.acceptVerifiedDelivery(context.Background(), result, attempt)
}

func (a *Agent) deploymentForTest(session *verificationSession, root verifiedNode) (AuthorizedAction, bool, error) {
	attempt := a.application.NewAttempt()
	defer attempt.Close()
	return a.handleDeployment(context.Background(), session, root, attempt.(GenerationPrechecker).Precheck)
}

func (a *Agent) managedResourceForTest(session *verificationSession, root verifiedNode) (AuthorizedAction, bool, error) {
	attempt := a.application.NewAttempt()
	defer attempt.Close()
	return a.handleManagedResource(context.Background(), session, root, attempt.(GenerationPrechecker).Precheck)
}

func (a *Agent) workRequiredForTest(root verifiedNode, scope protocol.DeliveryScope) (bool, error) {
	attempt := a.application.NewAttempt()
	defer attempt.Close()
	return a.ordinaryWorkRequired(context.Background(), root, scope, attempt.(GenerationPrechecker).Precheck)
}

func (a *Agent) relationForTest(session *verificationSession, parent protocol.Digest, authorization protocol.ManagedResourceAuthorization) (protocol.FulfillmentRelation, error) {
	return a.verifyFulfillmentRelation(context.Background(), session, parent, authorization)
}

func (a *Agent) acceptForTest(action AuthorizedAction) error {
	attempt := a.application.NewAttempt()
	defer attempt.Close()
	return attempt.Accept(context.Background(), action)
}

func deploymentRootForDispatchTest(t *testing.T, identity protocol.Digest) verifiedNode {
	t.Helper()
	authorization := protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			Tenant:           sessionTenant(),
			TargetID:         "target-test",
			FullResourceName: "resources.example/test-resource",
			Generation:       1,
			Action:           protocol.ActionPut,
		},
	}
	encoded, err := protocol.MarshalCanonical(authorization)
	if err != nil {
		t.Fatalf("MarshalCanonical deployment authorization: %v", err)
	}
	return verifiedNode{
		identity: identity,
		result: protocol.VerificationResult{
			ProvenanceAuthenticationResult: protocol.ProvenanceAuthenticationResult{
				Authenticated: protocol.AuthenticatedEvidence{
					PredicateType: protocol.PredicateTypeDeploymentV1,
					Principal:     protocol.Principal{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example.test", Subject: "alice"},
				},
				Assertion: protocol.TypedAssertion{
					PredicateType: protocol.PredicateTypeDeploymentV1,
					Bytes:         encoded,
				},
			},
		},
	}
}

type recordingApplyTarget struct {
	applyCalls int
}

func (*recordingApplyTarget) ProvenanceType() protocol.ProvenanceType {
	return "test-apply/v1"
}

func (*recordingApplyTarget) ParseHints(protocol.TypedEvidence) (protocol.TentativeHints, error) {
	return protocol.TentativeHints{}, errors.New("not used")
}

func (*recordingApplyTarget) RequiresEvidenceLog() bool { return false }

func (*recordingApplyTarget) BeginVerification(context.Context, protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	return nil, errors.New("not used")
}

func (*recordingApplyTarget) Owns(predicate protocol.PredicateType) bool {
	return predicate == "test-owned/v1"
}

func (t *recordingApplyTarget) Apply(context.Context, protocol.ApplyRequest) error {
	t.applyCalls++
	return nil
}
