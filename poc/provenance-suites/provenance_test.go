package provenancesuites

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/deliveryagent"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/producer"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

const (
	testTenant               = protocol.TenantID("tenant-acme")
	testTarget               = "target-east"
	testIssuer               = protocol.Authority("https://issuer.example.test")
	testReplicasMediaType    = protocol.MediaType("application/vnd.example.replicas+json")
	testClusterSpecMediaType = protocol.MediaType("application/vnd.example.cluster-spec+json")
	testResourceType         = "clusters"
)

func deploymentName(id string) protocol.FullResourceName {
	return protocol.FullResourceName("//fleetshift.io/deployments/" + id)
}

func clusterName(id string) protocol.FullResourceName {
	return protocol.FullResourceName("//kind.fleetshift.io/clusters/" + id)
}

func TestEnrollmentAndSignedDelivery(t *testing.T) {
	s := newEnrolledScenario(t)

	evidence := mustSignDeployment(t, s.user, deploymentName("fulfillment-1"), 1, []byte(`{"replicas":3}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); err != nil {
		t.Fatalf("submit delivery: %v", err)
	}
	applied, ok := s.agent.Applied(deploymentName("fulfillment-1"))
	if !ok {
		t.Fatal("agent did not apply the delivery")
	}
	if applied.PredicateType != protocol.PredicateTypeDeploymentV1 {
		t.Fatalf("applied predicate = %s, want %s", applied.PredicateType, protocol.PredicateTypeDeploymentV1)
	}
	if len(applied.Manifests) != 1 {
		t.Fatalf("applied %d manifests, want 1", len(applied.Manifests))
	}
	if applied.Manifests[0].MediaType != testReplicasMediaType {
		t.Fatalf("applied media type = %s, want %s", applied.Manifests[0].MediaType, testReplicasMediaType)
	}
	if got, want := string(applied.Manifests[0].Bytes), `{"replicas":3}`; got != want {
		t.Fatalf("applied payload = %s, want %s", got, want)
	}
}

func TestLoggedEnrollmentIsOrdinaryDeliveryAndLaterContentSkipsTheLeaf(t *testing.T) {
	const westTarget = "target-west"
	s := newTwoTargetScenario(t, westTarget)
	if got := s.manager.EvidenceLogSize(); got != 1 {
		t.Fatalf("log size after enrollment = %d, want 1", got)
	}
	if s.agent.Checkpoint().Size != 1 || s.west.Checkpoint().Size != 1 {
		t.Fatalf("agent checkpoints after enrollment: east=%d west=%d, want 1", s.agent.Checkpoint().Size, s.west.Checkpoint().Size)
	}
	if s.agent.SuiteApplyCount() != 1 {
		t.Fatalf("east suite Apply count = %d, want 1", s.agent.SuiteApplyCount())
	}
	if s.west.SuiteApplyCount() != 1 {
		t.Fatalf("west suite Apply count = %d, want 1", s.west.SuiteApplyCount())
	}
	if _, ok := s.west.PublicKey(s.user.Principal()); !ok {
		t.Fatal("west agent did not apply enrollment through Deliver")
	}

	evidence := mustSignDeploymentFor(t, s.user, testTarget, deploymentName("after-enroll"), 1, []byte(`{"replicas":3}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); err != nil {
		t.Fatalf("submit delivery after enrollment: %v", err)
	}
	if got := evidenceIndex(t, s.manager, evidence); got != 1 {
		t.Fatalf("content log index = %d, want 1 (enrollment occupied 0)", got)
	}
	if got := s.manager.EvidenceLogSize(); got != 2 {
		t.Fatalf("content checkpoint size = %d, want 2", got)
	}
	if _, ok := s.agent.Applied(deploymentName("after-enroll")); !ok {
		t.Fatal("east agent did not apply the content delivery")
	}
	if s.agent.SuiteApplyCount() != 1 {
		t.Fatal("later content delivery called suite Apply")
	}
}

func TestIntentAndTrustConfigUpdateDoNotCallSuiteApply(t *testing.T) {
	s := newEnrolledScenario(t)
	afterEnroll := s.agent.SuiteApplyCount()
	if afterEnroll != 1 {
		t.Fatalf("enrollment suite Apply count = %d, want 1", afterEnroll)
	}

	evidence := mustSignDeployment(t, s.user, deploymentName("intent-dispatch"), 1, []byte(`{"replicas":1}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); err != nil {
		t.Fatalf("submit deployment: %v", err)
	}
	if s.agent.SuiteApplyCount() != afterEnroll {
		t.Fatal("deployment/v1 called suite Apply")
	}

	encoded, err := protocol.MarshalCanonical(protocol.DeliveryScope{
		TenantID:         testTenant,
		TargetID:         testTarget,
		FullResourceName: deploymentName("trust-config"),
		Generation:       1,
		Action:           protocol.ActionPut,
	})
	if err != nil {
		t.Fatalf("marshal trust-config assertion: %v", err)
	}
	updateEvidence, err := s.user.DirectKey().CreateEvidence(context.Background(), protocol.TypedAssertion{
		PredicateType: protocol.PredicateTypeTrustConfigUpdateV1,
		Bytes:         encoded,
	})
	if err != nil {
		t.Fatalf("create trust-config evidence: %v", err)
	}
	_, err = s.manager.Compromised().PushDelivery(context.Background(), updateEvidence)
	if !errors.Is(err, protocol.ErrUnknownPredicateType) {
		t.Fatalf("trust-config-update error = %v, want ErrUnknownPredicateType", err)
	}
	if s.agent.SuiteApplyCount() != afterEnroll {
		t.Fatal("trust-config-update/v1 called suite Apply")
	}
}

func TestUnownedPredicateWithPolicyDoesNotCallSuiteApply(t *testing.T) {
	const unowned protocol.PredicateType = "not-owned/v1"
	trust := testTrust()
	profile := trust.AuthorityRegistry[0].ProvenanceProfiles[0]
	trust.AuthorityRegistry[0].DeliveryPolicies = append(trust.AuthorityRegistry[0].DeliveryPolicies, protocol.DeliveryPolicy{
		Match: protocol.PolicyMatch{
			PredicateType:     unowned,
			RootAuthorization: true,
		},
		LiveCredential: protocol.RequirementNone,
		Provenance:     protocol.RequirementRequired,
		Profiles:       []protocol.ProfileConfig{profile},
	})
	s := newScenarioWithTrust(t, trust)
	enrollProducer(t, s, s.user)
	afterEnroll := s.agent.SuiteApplyCount()

	encoded, err := protocol.MarshalCanonical(protocol.DeliveryScope{
		TenantID:         testTenant,
		TargetID:         testTarget,
		FullResourceName: deploymentName("unowned"),
		Generation:       1,
		Action:           protocol.ActionPut,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	evidence, err := s.user.DirectKey().CreateEvidence(context.Background(), protocol.TypedAssertion{
		PredicateType: unowned,
		Bytes:         encoded,
	})
	if err != nil {
		t.Fatalf("create evidence: %v", err)
	}
	_, err = s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrUnknownPredicateType) {
		t.Fatalf("error = %v, want ErrUnknownPredicateType", err)
	}
	if s.agent.SuiteApplyCount() != afterEnroll {
		t.Fatal("unowned predicate called suite Apply")
	}
	if _, ok := s.agent.Applied(deploymentName("unowned")); ok {
		t.Fatal("agent applied an unowned root predicate")
	}
}

func TestResourceManagerCannotForgeDeliverySignature(t *testing.T) {
	s := newEnrolledScenario(t)
	attacker := mustProducer(t, "mallory")
	evidence := mustSignDeployment(t, attacker, deploymentName("forged"), 1, []byte(`{"replicas":9}`))

	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("compromised push error = %v, want ErrVerificationFailed", err)
	}
	if _, ok := s.agent.Applied(deploymentName("forged")); ok {
		t.Fatal("agent applied a delivery signed by an unenrolled key")
	}
}

func TestResourceManagerCannotAlterSignedContent(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, deploymentName("tamper"), 1, []byte(`{"replicas":3}`))
	evidence = tamperEmbeddedAssertion(t, evidence, func(assertion *protocol.TypedAssertion) {
		var authorization protocol.DeploymentAuthorization
		if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
			t.Fatalf("decode: %v", err)
		}
		authorization.Manifests[0].Bytes = []byte(`{"replicas":9}`)
		tampered, err := protocol.MarshalCanonical(authorization)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		assertion.Bytes = tampered
	})
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("tampered content error = %v, want ErrVerificationFailed", err)
	}
}

func TestFirstEnrollmentIsTOFUWonByWhoeverArrivesFirst(t *testing.T) {
	s := newScenario(t)
	attacker := mustProducer(t, "alice")
	enrollment, err := attacker.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("attacker enrollment: %v", err)
	}
	if err := s.manager.Compromised().CommitEnrollment(context.Background(), enrollment); err != nil {
		t.Fatalf("compromised first enrollment: %v", err)
	}
	got, ok := s.agent.PublicKey(attacker.Principal())
	if !ok || string(got) != string(attacker.PublicKey()) {
		t.Fatal("TOFU limitation was not visible: attacker did not win the first bind")
	}

	genuine := mustProducer(t, "alice")
	genuineEnrollment, err := genuine.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("genuine enrollment: %v", err)
	}
	err = s.manager.Compromised().CommitEnrollment(context.Background(), genuineEnrollment)
	if err == nil {
		t.Fatal("established mapping accepted a substituted enrollment")
	}
}

func TestAuthorizerBypassStillRequiresAuthenticProvenance(t *testing.T) {
	s := newScenario(t)
	s.manager = resourcemanager.New(testTenant, func(resourcemanager.AuthorizationRequest) error {
		return errors.New("rbac denied")
	})
	if err := s.manager.RegisterAgent(testTarget, s.agent); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	enrollment, err := s.user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("enrollment: %v", err)
	}
	if _, err := s.manager.SubmitDirectKeyEnrollment(context.Background(), s.user.Principal(), enrollment); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("authorized enrollment error = %v, want ErrUnauthorized", err)
	}
	if err := s.manager.Compromised().CommitEnrollment(context.Background(), enrollment); err != nil {
		t.Fatalf("compromised enrollment: %v", err)
	}
	evidence := mustSignDeployment(t, s.user, deploymentName("rbac"), 1, []byte(`{"ok":true}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("authorized delivery error = %v, want ErrUnauthorized", err)
	}
	if _, err := s.manager.Compromised().PushDelivery(context.Background(), evidence); err != nil {
		t.Fatalf("compromised genuine delivery: %v", err)
	}
	if _, ok := s.agent.Applied(deploymentName("rbac")); !ok {
		t.Fatal("agent rejected authentic provenance after RM authorization bypass")
	}
}

func TestUnknownProvenanceTypeFailsClosed(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, deploymentName("unknown-type"), 1, []byte(`{"ok":true}`))
	evidence.ProvenanceType = "unknown/v1"
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrUnknownProvenanceType) {
		t.Fatalf("error = %v, want ErrUnknownProvenanceType", err)
	}
}

func TestInitializedVerifierRejectsSecondBootstrap(t *testing.T) {
	s := newScenario(t)
	if err := s.agent.Bootstrap(testTrust()); !errors.Is(err, protocol.ErrAlreadyInitialized) {
		t.Fatalf("second bootstrap error = %v, want ErrAlreadyInitialized", err)
	}
}

func TestUninitializedVerifierRejectsDelivery(t *testing.T) {
	agent, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: testTarget})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	user := mustProducer(t, "alice")
	evidence := mustSignDeployment(t, user, deploymentName("too-early"), 1, []byte(`{}`))
	err = agent.Deliver(resourcemanager.DeliveryPackage{Root: protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: evidence}}})
	if !errors.Is(err, protocol.ErrUninitializedVerifier) {
		t.Fatalf("error = %v, want ErrUninitializedVerifier", err)
	}
}

func TestAlteredAssertionBytesFailContentBinding(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, deploymentName("other-tenant"), 1, []byte(`{"ok":true}`))
	evidence = tamperEmbeddedAssertion(t, evidence, func(assertion *protocol.TypedAssertion) {
		var authorization protocol.DeploymentAuthorization
		if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
			t.Fatalf("decode: %v", err)
		}
		authorization.TenantID = "tenant-other"
		tampered, err := protocol.MarshalCanonical(authorization)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		assertion.Bytes = tampered
	})
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("error = %v, want ErrVerificationFailed", err)
	}
}

func TestRetryAfterLostAcknowledgementIsIdempotent(t *testing.T) {
	s := newEnrolledScenario(t)
	s.agent.LoseNextAcknowledgement()
	evidence := mustSignDeployment(t, s.user, deploymentName("lost-ack"), 1, []byte(`{"ok":true}`))
	receipt, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence)
	if !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("error = %v, want ErrAcknowledgementLost", err)
	}
	if _, ok := s.agent.Applied(deploymentName("lost-ack")); !ok {
		t.Fatal("agent did not apply before losing the acknowledgement")
	}
	cached, ok := s.manager.AgentCheckpoint(testTarget)
	if !ok || cached.Size != 1 {
		t.Fatalf("manager cache after lost ack = %+v, want enrollment checkpoint size 1", cached)
	}
	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	cached, ok = s.manager.AgentCheckpoint(testTarget)
	if !ok || cached != s.agent.Checkpoint() {
		t.Fatalf("manager cache after retry = %+v, agent checkpoint = %+v", cached, s.agent.Checkpoint())
	}
	if got, want := s.agent.StaleCheckpointResponses(), uint64(1); got != want {
		t.Fatalf("agent stale-checkpoint responses = %d, want %d", got, want)
	}
}

func TestRejectedDeliveryAdvancesCheckpointAndRetryRecoversWithoutApplying(t *testing.T) {
	s := newEnrolledScenario(t)
	managerBefore, ok := s.manager.AgentCheckpoint(testTarget)
	if !ok {
		t.Fatal("resource manager did not retain the registered-agent checkpoint")
	}

	attacker := mustProducer(t, "mallory")
	forged := mustSignDeployment(t, attacker, deploymentName("rejected-after-log"), 1, []byte(`{"owner":"attacker"}`))
	receipt, err := s.manager.Compromised().PushDelivery(context.Background(), forged)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("push forged delivery error = %v, want ErrVerificationFailed", err)
	}
	if _, applied := s.agent.Applied(deploymentName("rejected-after-log")); applied {
		t.Fatal("agent applied a delivery it rejected after advancing the log")
	}
	wantSize := evidenceIndex(t, s.manager, forged) + 1
	if got := s.agent.Checkpoint(); got.Size != wantSize {
		t.Fatalf("agent checkpoint size = %d, want %d after rejecting the included delivery", got.Size, wantSize)
	}
	if got, _ := s.manager.AgentCheckpoint(testTarget); got != managerBefore {
		t.Fatalf("manager advanced checkpoint on a rejected delivery: got %+v, want %+v", got, managerBefore)
	}
	if got := s.agent.StaleCheckpointResponses(); got != 0 {
		t.Fatalf("agent stale-checkpoint responses after first rejection = %d, want 0", got)
	}

	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("retry forged delivery error = %v, want ErrVerificationFailed", err)
	}
	if _, applied := s.agent.Applied(deploymentName("rejected-after-log")); applied {
		t.Fatal("retry applied a delivery that remains semantically invalid")
	}
	if got, want := s.manager.AgentCheckpoint(testTarget); !want || got != s.agent.Checkpoint() {
		t.Fatalf("manager checkpoint after retry = %+v, %t; want agent checkpoint %+v", got, want, s.agent.Checkpoint())
	}
	if got, want := s.agent.StaleCheckpointResponses(), uint64(1); got != want {
		t.Fatalf("agent stale-checkpoint responses = %d, want %d", got, want)
	}

	signed := mustSignDeployment(t, s.user, deploymentName("after-rejected-log"), 1, []byte(`{"owner":"alice"}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), signed); err != nil {
		t.Fatalf("submit valid delivery after checkpoint recovery: %v", err)
	}
	if _, ok := s.agent.Applied(deploymentName("after-rejected-log")); !ok {
		t.Fatal("agent did not apply the valid delivery after recovering the rejected-log checkpoint")
	}
}

func TestRetryManagedResourceAfterLostAcknowledgementReusesRelation(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	s.agent.LoseNextAcknowledgement()
	spec := json.RawMessage(`{"region":"us-east-1"}`)
	evidence := mustSignManagedResource(t, s.user, clusterName("cluster-retry"), 1, spec)
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	receipt, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("error = %v, want ErrAcknowledgementLost", err)
	}
	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	applied, ok := s.agent.Applied(clusterName("cluster-retry"))
	if !ok {
		t.Fatal("agent did not retain the managed resource after retry")
	}
	if applied.Manifests[0].MediaType != testClusterSpecMediaType {
		t.Fatalf("retry dropped the fulfillment relation: media type = %s", applied.Manifests[0].MediaType)
	}
}

func TestManagedResourceAppliesDerivedManifestFromFulfillmentRelation(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	spec := json.RawMessage(`{"region":"us-east-1"}`)
	evidence := mustSignManagedResource(t, s.user, clusterName("cluster-1"), 1, spec)
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)

	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if err != nil {
		t.Fatalf("submit managed resource: %v", err)
	}
	pkg := s.recorder.last
	if pkg.EvidenceLog == nil {
		t.Fatal("managed-resource package missing evidence-log update")
	}
	assertItemInclusion(t, pkg.EvidenceLog.Checkpoint, pkg.Root, evidenceIndex(t, s.manager, evidence))
	if len(pkg.Supporting) != 1 {
		t.Fatalf("supporting items = %d, want 1", len(pkg.Supporting))
	}
	assertItemInclusion(t, pkg.EvidenceLog.Checkpoint, pkg.Supporting[0], evidenceIndex(t, s.manager, relEvidence))
	applied, ok := s.agent.Applied(clusterName("cluster-1"))
	if !ok {
		t.Fatal("agent did not apply the managed resource")
	}
	if applied.PredicateType != protocol.PredicateTypeManagedResourceV1 {
		t.Fatalf("applied predicate = %s, want %s", applied.PredicateType, protocol.PredicateTypeManagedResourceV1)
	}
	if len(applied.Manifests) != 1 {
		t.Fatalf("applied %d manifests, want 1", len(applied.Manifests))
	}
	if applied.Manifests[0].MediaType != testClusterSpecMediaType {
		t.Fatalf("derived media type = %s, want %s", applied.Manifests[0].MediaType, testClusterSpecMediaType)
	}
	if got, want := string(applied.Manifests[0].Bytes), string(spec); got != want {
		t.Fatalf("derived spec = %s, want %s", got, want)
	}
}

func TestManagedResourceWithoutFulfillmentRelationIsRejected(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	evidence := mustSignManagedResource(t, s.user, clusterName("cluster-missing"), 1, json.RawMessage(`{"region":"us-east-1"}`))
	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence)
	if !errors.Is(err, deliveryagent.ErrFulfillmentRelationRequired) {
		t.Fatalf("error = %v, want ErrFulfillmentRelationRequired", err)
	}
	if _, ok := s.agent.Applied(clusterName("cluster-missing")); ok {
		t.Fatal("agent applied a managed resource with no fulfillment relation")
	}
}

func TestManagedResourceRejectsFulfillmentRelationWithWrongResourceType(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	evidence := mustSignManagedResource(t, s.user, clusterName("cluster-wrong-type"), 1, json.RawMessage(`{"region":"us-east-1"}`))
	relEvidence := mustSignRelation(t, s.addon, "monitoring-stacks", testClusterSpecMediaType)
	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if err == nil {
		t.Fatal("accepted a fulfillment relation for a different resource type")
	}
	if _, ok := s.agent.Applied(clusterName("cluster-wrong-type")); ok {
		t.Fatal("agent applied a managed resource with a mismatched relation")
	}
}

func TestManagedResourceRejectsRelationSignedByUnenrolledKey(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	rogue := mustProducer(t, "rogue-addon")
	evidence := mustSignManagedResource(t, s.user, clusterName("cluster-rogue"), 1, json.RawMessage(`{"region":"us-east-1"}`))
	relEvidence := mustSignRelation(t, rogue, testResourceType, testClusterSpecMediaType)
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence, relEvidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) && !errors.Is(err, protocol.ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want verification failure", err)
	}
	if _, ok := s.agent.Applied(clusterName("cluster-rogue")); ok {
		t.Fatal("agent applied a managed resource with an unenrolled relation signer")
	}
}

func TestDeploymentIgnoresCourieredFulfillmentRelation(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	evidence := mustSignDeployment(t, s.user, deploymentName("deploy-with-relation"), 1, []byte(`{"replicas":3}`))
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence); err != nil {
		t.Fatalf("submit deployment: %v", err)
	}
	applied, ok := s.agent.Applied(deploymentName("deploy-with-relation"))
	if !ok {
		t.Fatal("agent did not apply the deployment")
	}
	if applied.PredicateType != protocol.PredicateTypeDeploymentV1 {
		t.Fatalf("applied predicate = %s, want %s", applied.PredicateType, protocol.PredicateTypeDeploymentV1)
	}
	if applied.Manifests[0].MediaType != testReplicasMediaType {
		t.Fatalf("unused relation became apply authority: media type = %s", applied.Manifests[0].MediaType)
	}
	if got, want := string(applied.Manifests[0].Bytes), `{"replicas":3}`; got != want {
		t.Fatalf("applied payload = %s, want %s", got, want)
	}
}

func TestDeploymentIgnoresTamperedUnusedSupportingInclusion(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	s.agent.FailNextDeliveriesBeforeAccepting(1)
	evidence := mustSignDeployment(t, s.user, deploymentName("deploy-with-tampered-relation"), 1, []byte(`{"replicas":3}`))
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if !errors.Is(err, deliveryagent.ErrDeliveryUnavailable) {
		t.Fatalf("error = %v, want ErrDeliveryUnavailable", err)
	}

	pkg := s.recorder.last
	if len(pkg.Supporting) != 1 || pkg.Supporting[0].EvidenceLog == nil {
		t.Fatalf("expected one supporting item with an inclusion, got %+v", pkg.Supporting)
	}
	tampered := *pkg.Supporting[0].EvidenceLog
	tampered.Index++
	pkg.Supporting[0].EvidenceLog = &tampered

	if err := s.agent.Deliver(pkg); err != nil {
		t.Fatalf("tampered unused supporting inclusion rejected: %v", err)
	}
	applied, ok := s.agent.Applied(deploymentName("deploy-with-tampered-relation"))
	if !ok {
		t.Fatal("agent did not apply the deployment")
	}
	if applied.PredicateType != protocol.PredicateTypeDeploymentV1 {
		t.Fatalf("applied predicate = %s, want %s", applied.PredicateType, protocol.PredicateTypeDeploymentV1)
	}
	if applied.Manifests[0].MediaType != testReplicasMediaType {
		t.Fatalf("unused relation became apply authority: media type = %s", applied.Manifests[0].MediaType)
	}
}

func TestUnknownRootPredicateFailsClosed(t *testing.T) {
	s := newEnrolledScenario(t)
	scope := protocol.DeliveryScope{
		TenantID:         testTenant,
		TargetID:         testTarget,
		FullResourceName: deploymentName("unknown-pred"),
		Generation:       1,
		Action:           protocol.ActionPut,
	}
	encoded, err := protocol.MarshalCanonical(scope)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertion := protocol.TypedAssertion{
		PredicateType: "not-a-real-predicate/v1",
		Bytes:         encoded,
	}
	evidence, err := s.user.DirectKey().CreateEvidence(context.Background(), assertion)
	if err != nil {
		t.Fatalf("create evidence: %v", err)
	}
	_, err = s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrNoMatchingPolicy) && !errors.Is(err, protocol.ErrUnknownPredicateType) {
		t.Fatalf("error = %v, want fail-closed unknown predicate", err)
	}
	if _, ok := s.agent.Applied(deploymentName("unknown-pred")); ok {
		t.Fatal("agent applied an unknown root predicate")
	}
}

func TestDeploymentRejectsMissingManifestMediaType(t *testing.T) {
	s := newEnrolledScenario(t)
	authorization := protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TenantID:         testTenant,
			TargetID:         testTarget,
			FullResourceName: deploymentName("missing-media"),
			Generation:       1,
			Action:           protocol.ActionPut,
		},
		Manifests: []protocol.TypedManifest{{
			Bytes: []byte(`{"replicas":3}`),
		}},
	}
	encoded, err := protocol.MarshalCanonical(authorization)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertion := protocol.TypedAssertion{
		PredicateType: protocol.PredicateTypeDeploymentV1,
		Bytes:         encoded,
	}
	evidence, err := s.user.DirectKey().CreateEvidence(context.Background(), assertion)
	if err != nil {
		t.Fatalf("create evidence: %v", err)
	}
	_, err = s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrMalformedEvidence) {
		t.Fatalf("error = %v, want ErrMalformedEvidence", err)
	}
	if _, ok := s.agent.Applied(deploymentName("missing-media")); ok {
		t.Fatal("agent applied a deployment with an untyped manifest")
	}
}

func TestPredicateTypeTamperFailsContentBinding(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, deploymentName("pred-tamper"), 1, []byte(`{"replicas":3}`))
	evidence = tamperEmbeddedAssertion(t, evidence, func(assertion *protocol.TypedAssertion) {
		assertion.PredicateType = protocol.PredicateTypeManagedResourceV1
	})
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) && !errors.Is(err, protocol.ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want content-digest or policy re-evaluation failure", err)
	}
}

func TestSignDeploymentRequiresCompleteDeliveryScope(t *testing.T) {
	c := mustProducer(t, "alice")
	_, err := c.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
		Manifests: []protocol.TypedManifest{{
			MediaType: testReplicasMediaType,
			Bytes:     []byte(`{}`),
		}},
	})
	if err == nil {
		t.Fatal("signed a deployment with an empty delivery scope")
	}
}

func TestSignDeploymentRequiresResourceName(t *testing.T) {
	c := mustProducer(t, "alice")
	_, err := c.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TargetID: testTarget,
			Action:   protocol.ActionPut,
		},
		Manifests: []protocol.TypedManifest{{
			MediaType: testReplicasMediaType,
			Bytes:     []byte(`{}`),
		}},
	})
	if err == nil {
		t.Fatal("signed a deployment without an AIP-122 resource name")
	}
}

func TestSignManagedResourceRequiresCompleteDeliveryScope(t *testing.T) {
	c := mustProducer(t, "alice")
	_, err := c.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
		ResourceType: testResourceType,
		Spec:         json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("signed a managed resource with an empty delivery scope")
	}
}

func TestLostAckRetryRebuildsProofsFromAgentCheckpoint(t *testing.T) {
	const westTarget = "target-west"
	s := newTwoTargetScenario(t, westTarget)
	s.agent.LoseNextAcknowledgement()

	evidenceA := mustSignDeploymentFor(t, s.user, testTarget, deploymentName("east-lost"), 1, []byte(`{"ok":true}`))
	receipt, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidenceA)
	if !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("error = %v, want ErrAcknowledgementLost", err)
	}
	cached, ok := s.manager.AgentCheckpoint(testTarget)
	if !ok || cached.Size != 1 {
		t.Fatalf("manager cache after lost ack = %+v, want enrollment checkpoint size 1", cached)
	}

	evidenceB := mustSignDeploymentFor(t, s.user, westTarget, deploymentName("west-advance"), 1, []byte(`{"ok":true}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidenceB); err != nil {
		t.Fatalf("submit west delivery: %v", err)
	}

	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err != nil {
		t.Fatalf("retry east delivery: %v", err)
	}
	if s.agent.StaleCheckpointResponses() == 0 {
		t.Fatal("retry did not take the stale-checkpoint loop")
	}
	cached, ok = s.manager.AgentCheckpoint(testTarget)
	if !ok || cached != s.agent.Checkpoint() {
		t.Fatalf("manager cache after stale retry = %+v, agent checkpoint = %+v", cached, s.agent.Checkpoint())
	}
	if s.agent.Checkpoint().Size != 3 {
		t.Fatalf("east agent checkpoint size = %d, want 3", s.agent.Checkpoint().Size)
	}
}

func TestTwoTargetDeliverySkipsUnrelatedLeaf(t *testing.T) {
	const westTarget = "target-west"
	s := newTwoTargetScenario(t, westTarget)

	evidenceB := mustSignDeploymentFor(t, s.user, westTarget, deploymentName("west-only"), 1, []byte(`{"replicas":1}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidenceB); err != nil {
		t.Fatalf("submit west delivery: %v", err)
	}

	evidenceA := mustSignDeploymentFor(t, s.user, testTarget, deploymentName("east-after-west"), 1, []byte(`{"replicas":3}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidenceA); err != nil {
		t.Fatalf("submit east delivery: %v", err)
	}

	pkg := s.recorder.last
	if pkg.EvidenceLog == nil {
		t.Fatal("east package missing evidence-log update")
	}
	if pkg.EvidenceLog.Checkpoint.Size != 3 {
		t.Fatalf("east checkpoint size = %d, want 3", pkg.EvidenceLog.Checkpoint.Size)
	}
	assertItemInclusion(t, pkg.EvidenceLog.Checkpoint, pkg.Root, evidenceIndex(t, s.manager, evidenceA))
	rootID, err := pkg.Root.Evidence.Identity()
	if err != nil {
		t.Fatalf("east root identity: %v", err)
	}
	wantID, err := evidenceA.Identity()
	if err != nil {
		t.Fatalf("east identity: %v", err)
	}
	if rootID != wantID {
		t.Fatalf("east root identity = %q, want %q", rootID, wantID)
	}
	otherID, err := evidenceB.Identity()
	if err != nil {
		t.Fatalf("west identity: %v", err)
	}
	if rootID == otherID {
		t.Fatal("east package disclosed the unrelated west evidence identity")
	}
	if _, ok := s.agent.Applied(deploymentName("west-only")); ok {
		t.Fatal("east agent applied the west resource")
	}
	if _, ok := s.west.Applied(deploymentName("west-only")); !ok {
		t.Fatal("west agent did not apply its delivery")
	}
}

func TestRootAndSupportingEvidenceGetDistinctLogPositionsAtAcceptance(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	before := s.manager.EvidenceLogSize()
	root := mustSignManagedResource(t, s.user, clusterName("cluster-three-leaves"), 1, json.RawMessage(`{"region":"us-east-1"}`))
	relA := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	relB := mustSignRelation(t, s.addon, "monitoring-stacks", testClusterSpecMediaType)
	receipt, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), root, relA, relB)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if got, want := s.manager.EvidenceLogSize(), before+3; got != want {
		t.Fatalf("log size = %d, want %d", got, want)
	}
	if evidenceIndex(t, s.manager, relA) == evidenceIndex(t, s.manager, root) {
		t.Fatal("supporting evidence reused the root log position")
	}
	if evidenceIndex(t, s.manager, relA) == evidenceIndex(t, s.manager, relB) {
		t.Fatal("distinct supporting evidence shared a log position")
	}
	stored, ok := s.manager.LookupDelivery(receipt.DeliveryID)
	if !ok {
		t.Fatal("missing stored delivery")
	}
	rootID, err := root.Identity()
	if err != nil {
		t.Fatalf("root identity: %v", err)
	}
	relAID, err := relA.Identity()
	if err != nil {
		t.Fatalf("relation identity: %v", err)
	}
	if stored.Root != rootID || len(stored.Supporting) != 2 || stored.Supporting[0] != relAID {
		t.Fatalf("stored delivery = %+v, want identities not evidence bytes", stored)
	}
}

func TestDuplicateSupportingEvidenceInOneAcceptanceAppendsOnce(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	before := s.manager.EvidenceLogSize()
	root := mustSignManagedResource(t, s.user, clusterName("cluster-dup-support"), 1, json.RawMessage(`{"region":"us-east-1"}`))
	rel := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	receipt, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), root, rel, rel)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if got, want := s.manager.EvidenceLogSize(), before+2; got != want {
		t.Fatalf("log size = %d, want %d", got, want)
	}
	stored, ok := s.manager.LookupDelivery(receipt.DeliveryID)
	if !ok || len(stored.Supporting) != 1 {
		t.Fatalf("stored supporting = %v, want one identity", stored.Supporting)
	}
}

func TestReusedEvidenceKeepsItsFirstLogIndex(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	rel := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	first := mustSignManagedResource(t, s.user, clusterName("cluster-reuse-1"), 1, json.RawMessage(`{"n":1}`))
	if _, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), first, rel); err != nil {
		t.Fatalf("accept first: %v", err)
	}
	relIndex := evidenceIndex(t, s.manager, rel)
	firstIndex := evidenceIndex(t, s.manager, first)
	afterFirst := s.manager.EvidenceLogSize()

	second := mustSignManagedResource(t, s.user, clusterName("cluster-reuse-2"), 1, json.RawMessage(`{"n":2}`))
	if _, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), second, rel); err != nil {
		t.Fatalf("accept second: %v", err)
	}
	if got := evidenceIndex(t, s.manager, rel); got != relIndex {
		t.Fatalf("reused relation index = %d, want canonical %d", got, relIndex)
	}
	if got, want := s.manager.EvidenceLogSize(), afterFirst+1; got != want {
		t.Fatalf("log size after reuse = %d, want %d (only the new root)", got, want)
	}

	intervening := mustSignDeployment(t, s.user, deploymentName("intervening"), 1, []byte(`{"ok":true}`))
	if _, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), intervening); err != nil {
		t.Fatalf("accept intervening: %v", err)
	}
	afterIntervening := s.manager.EvidenceLogSize()
	if _, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), first, rel); err != nil {
		t.Fatalf("resubmit first: %v", err)
	}
	if got := evidenceIndex(t, s.manager, first); got != firstIndex {
		t.Fatalf("resubmission moved the original root from %d to %d", firstIndex, got)
	}
	if got := evidenceIndex(t, s.manager, rel); got != relIndex {
		t.Fatalf("resubmission moved the relation index to %d", got)
	}
	if s.manager.EvidenceLogSize() != afterIntervening {
		t.Fatal("resubmission appended reused evidence")
	}
}

func TestEnrollmentBroadcastIsOneLeafAndTwoDispatches(t *testing.T) {
	const westTarget = "target-west"
	user := mustProducer(t, "alice")
	east, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: testTarget})
	if err != nil {
		t.Fatalf("new east agent: %v", err)
	}
	west, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: westTarget})
	if err != nil {
		t.Fatalf("new west agent: %v", err)
	}
	if err := east.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap east: %v", err)
	}
	if err := west.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap west: %v", err)
	}
	manager := resourcemanager.New(testTenant, nil)
	if err := manager.RegisterAgent(testTarget, east); err != nil {
		t.Fatalf("register east: %v", err)
	}
	if err := manager.RegisterAgent(westTarget, west); err != nil {
		t.Fatalf("register west: %v", err)
	}
	enrollment, err := user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("create enrollment: %v", err)
	}
	receipt, err := manager.SubmitDirectKeyEnrollment(context.Background(), user.Principal(), enrollment)
	if err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if got := manager.EvidenceLogSize(); got != 1 {
		t.Fatalf("enrollment log size = %d, want 1", got)
	}
	if len(receipt.DispatchIDs) != 2 {
		t.Fatalf("enrollment dispatches = %d, want 2", len(receipt.DispatchIDs))
	}
	if _, ok := east.PublicKey(user.Principal()); !ok {
		t.Fatal("east agent did not apply enrollment")
	}
	if _, ok := west.PublicKey(user.Principal()); !ok {
		t.Fatal("west agent did not apply enrollment")
	}
}

func TestAcceptanceWithoutRouteLeavesDispatchableOutbox(t *testing.T) {
	s := newEnrolledScenario(t)
	const missing = "target-unregistered"
	before := s.manager.EvidenceLogSize()
	evidence := mustSignDeploymentFor(t, s.user, missing, deploymentName("waiting-route"), 1, []byte(`{"ok":true}`))
	receipt, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), evidence)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if got, want := s.manager.EvidenceLogSize(), before+1; got != want {
		t.Fatalf("log size = %d, want %d", got, want)
	}
	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); !errors.Is(err, resourcemanager.ErrAgentUnavailable) {
		t.Fatalf("dispatch without route error = %v, want ErrAgentUnavailable", err)
	}
	if got := s.manager.EvidenceLogSize(); got != before+1 {
		t.Fatalf("failed dispatch appended evidence: size = %d", got)
	}

	agent, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: missing})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	if err := agent.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := s.manager.RegisterAgent(missing, agent); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err == nil {
		t.Fatal("dispatch to an unenrolled agent unexpectedly succeeded")
	}
	if got := s.manager.EvidenceLogSize(); got != before+1 {
		t.Fatalf("retry dispatch appended evidence: size = %d", got)
	}
}

func TestDispatchDoesNotAppendOrReauthorize(t *testing.T) {
	var delivers int
	manager := resourcemanager.New(testTenant, func(req resourcemanager.AuthorizationRequest) error {
		if req.Action == resourcemanager.ActionDeliver {
			delivers++
		}
		return nil
	})
	agent, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: testTarget})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	if err := agent.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := manager.RegisterAgent(testTarget, agent); err != nil {
		t.Fatalf("register: %v", err)
	}
	s := &scenario{user: mustProducer(t, "alice"), manager: manager, agent: agent}
	enrollProducer(t, s, s.user)

	evidence := mustSignDeployment(t, s.user, deploymentName("dispatch-once"), 1, []byte(`{"ok":true}`))
	receipt, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), evidence)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if delivers != 1 {
		t.Fatalf("authorizer delivers = %d, want 1 at accept", delivers)
	}
	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	size := s.manager.EvidenceLogSize()
	if err := s.manager.Dispatch(context.Background(), onlyDispatch(t, receipt)); err != nil {
		t.Fatalf("acknowledged dispatch: %v", err)
	}
	if delivers != 1 {
		t.Fatalf("dispatch re-ran authorization: delivers = %d", delivers)
	}
	if s.manager.EvidenceLogSize() != size {
		t.Fatal("acknowledged dispatch appended evidence")
	}
}

func TestRootRepeatedInSupportIsOmittedFromStoredSupport(t *testing.T) {
	s := newEnrolledScenario(t)
	before := s.manager.EvidenceLogSize()
	evidence := mustSignDeployment(t, s.user, deploymentName("root-in-support"), 1, []byte(`{"ok":true}`))
	receipt, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), evidence, evidence)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if got, want := s.manager.EvidenceLogSize(), before+1; got != want {
		t.Fatalf("log size = %d, want %d", got, want)
	}
	stored, ok := s.manager.LookupDelivery(receipt.DeliveryID)
	if !ok || len(stored.Supporting) != 0 {
		t.Fatalf("stored supporting = %v, want omitted root duplicate", stored.Supporting)
	}
}

func TestConflictingEnrollmentDoesNotGrowTheEvidenceLog(t *testing.T) {
	s := newScenario(t)
	first := mustProducer(t, "alice")
	enrollment, err := first.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("first enrollment: %v", err)
	}
	if _, err := s.manager.AcceptDirectKeyEnrollment(context.Background(), first.Principal(), enrollment); err != nil {
		t.Fatalf("accept first enrollment: %v", err)
	}
	before := s.manager.EvidenceLogSize()

	second := mustProducer(t, "alice")
	conflict, err := second.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("second enrollment: %v", err)
	}
	if _, err := s.manager.AcceptDirectKeyEnrollment(context.Background(), second.Principal(), conflict); err == nil {
		t.Fatal("conflicting enrollment was accepted")
	}
	if s.manager.EvidenceLogSize() != before {
		t.Fatal("conflicting enrollment appended an evidence-log leaf")
	}
}

func TestFailedAcceptDoesNotRegisterEvidence(t *testing.T) {
	s := newEnrolledScenario(t)
	before := s.manager.EvidenceLogSize()
	evidence := mustSignDeployment(t, s.user, deploymentName("denied"), 1, []byte(`{"ok":true}`))
	denied := resourcemanager.New(testTenant, func(resourcemanager.AuthorizationRequest) error {
		return errors.New("denied")
	})
	if _, err := denied.AcceptDelivery(context.Background(), s.user.Principal(), evidence); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("error = %v, want ErrUnauthorized", err)
	}
	if denied.EvidenceLogSize() != 0 {
		t.Fatal("unauthorized accept registered evidence")
	}

	bad := mustSignDeployment(t, s.user, deploymentName("bad-support"), 1, []byte(`{"ok":true}`))
	unknown := bad
	unknown.ProvenanceType = "unknown/v1"
	if _, err := s.manager.AcceptDelivery(context.Background(), s.user.Principal(), evidence, unknown); !errors.Is(err, protocol.ErrUnknownProvenanceType) {
		t.Fatalf("error = %v, want ErrUnknownProvenanceType", err)
	}
	if s.manager.EvidenceLogSize() != before {
		t.Fatal("invalid supporting evidence partially appended")
	}
}

func TestAgentRejectsLogInclusionThatDoesNotMatchRootEvidence(t *testing.T) {
	s := newEnrolledScenario(t)
	retained := s.agent.Checkpoint()
	evidence := mustSignDeployment(t, s.user, deploymentName("mismatch"), 1, []byte(`{"ok":true}`))
	err := s.agent.Deliver(resourcemanager.DeliveryPackage{
		EvidenceLog: &protocol.EvidenceLogUpdate{
			From:       retained,
			Checkpoint: retained,
		},
		Root: protocol.Item{
			SignedStatement: protocol.SignedStatement{Evidence: evidence},
			EvidenceLog:     &protocol.EvidenceLogInclusion{Index: 0},
		},
	})
	if !errors.Is(err, deliveryagent.ErrLogFork) {
		t.Fatalf("error = %v, want ErrLogFork", err)
	}
	if !errors.Is(err, protocol.ErrInvalidLogInclusion) {
		t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
	}
	assertNotStale(t, err)
	if _, ok := s.agent.Applied(deploymentName("mismatch")); ok {
		t.Fatal("agent applied a delivery whose root inclusion did not prove the root evidence")
	}
}

func TestAgentRejectsForkedAndSkipAheadLogProofs(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, deploymentName("pin-log"), 1, []byte(`{"ok":true}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); err != nil {
		t.Fatalf("submit pinning delivery: %v", err)
	}
	retained := s.agent.Checkpoint()

	next := mustSignDeployment(t, s.user, deploymentName("forked"), 1, []byte(`{"ok":false}`))
	forkRoot, err := protocol.EncodeDigest(bytes.Repeat([]byte{0xff}, 32))
	if err != nil {
		t.Fatalf("encode fork root: %v", err)
	}

	err = s.agent.Deliver(resourcemanager.DeliveryPackage{
		EvidenceLog: &protocol.EvidenceLogUpdate{
			From:       retained,
			Checkpoint: protocol.Checkpoint{Size: retained.Size + 1, Root: forkRoot},
		},
		Root: protocol.Item{
			SignedStatement: protocol.SignedStatement{Evidence: next},
			EvidenceLog:     &protocol.EvidenceLogInclusion{},
		},
	})
	if !errors.Is(err, deliveryagent.ErrLogFork) {
		t.Fatalf("forked root error = %v, want ErrLogFork", err)
	}
	assertNotStale(t, err)

	err = s.agent.Deliver(resourcemanager.DeliveryPackage{
		EvidenceLog: &protocol.EvidenceLogUpdate{
			From:       retained,
			Checkpoint: protocol.Checkpoint{Size: 99, Root: forkRoot},
		},
		Root: protocol.Item{
			SignedStatement: protocol.SignedStatement{Evidence: next},
			EvidenceLog:     &protocol.EvidenceLogInclusion{},
		},
	})
	if !errors.Is(err, deliveryagent.ErrLogFork) {
		t.Fatalf("skip-ahead error = %v, want ErrLogFork", err)
	}
	assertNotStale(t, err)
	if _, ok := s.agent.Applied(deploymentName("forked")); ok {
		t.Fatal("agent applied a forked or skip-ahead delivery")
	}
}

type recordingAgent struct {
	inner *deliveryagent.Agent
	last  resourcemanager.DeliveryPackage
}

func (r *recordingAgent) Deliver(pkg resourcemanager.DeliveryPackage) error {
	r.last = pkg
	return r.inner.Deliver(pkg)
}

type scenario struct {
	user     *producer.Producer
	addon    *producer.Producer
	manager  *resourcemanager.Manager
	agent    *deliveryagent.Agent
	west     *deliveryagent.Agent
	recorder *recordingAgent
}

func newScenario(t *testing.T) *scenario {
	t.Helper()
	return newScenarioWithTrust(t, testTrust())
}

func newScenarioWithTrust(t *testing.T, trust protocol.TrustConfiguration) *scenario {
	t.Helper()
	user := mustProducer(t, "alice")
	agent, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: testTarget})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	if err := agent.Bootstrap(trust); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	recorder := &recordingAgent{inner: agent}
	manager := resourcemanager.New(testTenant, nil)
	if err := manager.RegisterAgent(testTarget, recorder); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	return &scenario{user: user, manager: manager, agent: agent, recorder: recorder}
}

func newEnrolledScenario(t *testing.T) *scenario {
	t.Helper()
	s := newScenario(t)
	enrollProducer(t, s, s.user)
	return s
}

func newEnrolledManagedResourceScenario(t *testing.T) *scenario {
	t.Helper()
	s := newEnrolledScenario(t)
	s.addon = mustProducer(t, "addon-clusters")
	enrollProducer(t, s, s.addon)
	return s
}

func newTwoTargetScenario(t *testing.T, westTarget string) *scenario {
	t.Helper()
	user := mustProducer(t, "alice")
	east, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: testTarget})
	if err != nil {
		t.Fatalf("new east agent: %v", err)
	}
	west, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: westTarget})
	if err != nil {
		t.Fatalf("new west agent: %v", err)
	}
	if err := east.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap east: %v", err)
	}
	if err := west.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap west: %v", err)
	}
	recorder := &recordingAgent{inner: east}
	manager := resourcemanager.New(testTenant, nil)
	if err := manager.RegisterAgent(testTarget, recorder); err != nil {
		t.Fatalf("register east agent: %v", err)
	}
	if err := manager.RegisterAgent(westTarget, west); err != nil {
		t.Fatalf("register west agent: %v", err)
	}
	s := &scenario{user: user, manager: manager, agent: east, west: west, recorder: recorder}
	enrollProducer(t, s, s.user)
	if _, ok := west.PublicKey(s.user.Principal()); !ok {
		t.Fatal("west agent did not retain the enrollment mapping")
	}
	return s
}

func enrollProducer(t *testing.T, s *scenario, c *producer.Producer) {
	t.Helper()
	enrollment, err := c.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("create enrollment: %v", err)
	}
	if _, err := s.manager.SubmitDirectKeyEnrollment(context.Background(), c.Principal(), enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if _, ok := s.agent.PublicKey(c.Principal()); !ok {
		t.Fatal("agent did not retain the enrollment mapping")
	}
}

func mustProducer(t *testing.T, subject protocol.Subject) *producer.Producer {
	t.Helper()
	c, err := producer.New(producer.Config{
		TenantID: testTenant,
		Principal: protocol.Principal{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: testIssuer,
			Subject:   subject,
		},
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	return c
}

func mustSignDeployment(t *testing.T, c *producer.Producer, name protocol.FullResourceName, generation uint64, payload []byte) protocol.TypedEvidence {
	t.Helper()
	return mustSignDeploymentFor(t, c, testTarget, name, generation, payload)
}

func mustSignDeploymentFor(t *testing.T, c *producer.Producer, target string, name protocol.FullResourceName, generation uint64, payload []byte) protocol.TypedEvidence {
	t.Helper()
	evidence, err := c.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TargetID:         target,
			FullResourceName: name,
			Generation:       generation,
			Action:           protocol.ActionPut,
		},
		Manifests: []protocol.TypedManifest{{
			MediaType: testReplicasMediaType,
			Bytes:     payload,
		}},
	})
	if err != nil {
		t.Fatalf("sign deployment: %v", err)
	}
	return evidence
}

func mustSignManagedResource(t *testing.T, c *producer.Producer, name protocol.FullResourceName, generation uint64, spec json.RawMessage) protocol.TypedEvidence {
	t.Helper()
	evidence, err := c.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TargetID:         testTarget,
			FullResourceName: name,
			Generation:       generation,
			Action:           protocol.ActionPut,
		},
		ResourceType: testResourceType,
		Spec:         spec,
	})
	if err != nil {
		t.Fatalf("sign managed resource: %v", err)
	}
	return evidence
}

func mustSignRelation(t *testing.T, c *producer.Producer, resourceType string, mediaType protocol.MediaType) protocol.TypedEvidence {
	t.Helper()
	evidence, err := c.SignFulfillmentRelation(context.Background(), protocol.FulfillmentRelation{
		ResourceType: resourceType,
		MediaType:    mediaType,
	})
	if err != nil {
		t.Fatalf("sign fulfillment relation: %v", err)
	}
	return evidence
}

func assertNotStale(t *testing.T, err error) {
	t.Helper()
	var stale *deliveryagent.CheckpointStaleError
	if errors.As(err, &stale) {
		t.Fatalf("error reported as stale checkpoint: %v", err)
	}
}

func onlyDispatch(t *testing.T, receipt resourcemanager.DeliveryReceipt) resourcemanager.DispatchID {
	t.Helper()
	if len(receipt.DispatchIDs) != 1 {
		t.Fatalf("dispatch IDs = %v, want exactly 1", receipt.DispatchIDs)
	}
	return receipt.DispatchIDs[0]
}

func assertItemInclusion(t *testing.T, checkpoint protocol.Checkpoint, item protocol.Item, wantIndex uint64) {
	t.Helper()
	if item.EvidenceLog == nil {
		t.Fatal("item missing evidence-log inclusion")
	}
	if item.EvidenceLog.Index != wantIndex {
		t.Fatalf("item index = %d, want %d", item.EvidenceLog.Index, wantIndex)
	}
	if err := protocol.VerifyEvidenceLogInclusion(checkpoint, item.Evidence, *item.EvidenceLog); err != nil {
		t.Fatalf("item inclusion: %v", err)
	}
}

func evidenceIndex(t *testing.T, manager *resourcemanager.Manager, evidence protocol.TypedEvidence) uint64 {
	t.Helper()
	identity, err := evidence.Identity()
	if err != nil {
		t.Fatalf("evidence identity: %v", err)
	}
	index, ok := manager.EvidenceLogIndex(identity)
	if !ok {
		t.Fatalf("no evidence-log index for %s", identity)
	}
	return index
}

func tamperEmbeddedAssertion(t *testing.T, evidence protocol.TypedEvidence, mutate func(*protocol.TypedAssertion)) protocol.TypedEvidence {
	t.Helper()
	var body directkey.SignatureBody
	if err := json.Unmarshal(evidence.Bytes, &body); err != nil {
		t.Fatalf("decode signature body: %v", err)
	}
	mutate(&body.Assertion)
	raw, err := protocol.MarshalCanonical(body)
	if err != nil {
		t.Fatalf("marshal tampered evidence: %v", err)
	}
	evidence.Bytes = raw
	return evidence
}

func testTrust() protocol.TrustConfiguration {
	profile := protocol.ProfileConfig{ProvenanceType: protocol.ProvenanceTypeDirectKeyV1}
	return protocol.TrustConfiguration{
		AuthorityRegistry: []protocol.AuthorityConfig{{
			PrincipalAuthority: protocol.PrincipalAuthority{
				Scheme:    protocol.IdentitySchemeOIDCSubV1,
				Authority: testIssuer,
			},
			TenantMapping:      protocol.TenantMapping{StaticTenant: testTenant},
			ProvenanceProfiles: []protocol.ProfileConfig{profile},
			DeliveryPolicies: []protocol.DeliveryPolicy{
				{
					Match: protocol.PolicyMatch{
						PredicateType:     protocol.PredicateTypeDeploymentV1,
						RootAuthorization: true,
					},
					LiveCredential: protocol.RequirementNone,
					Provenance:     protocol.RequirementRequired,
					Profiles:       []protocol.ProfileConfig{profile},
				},
				{
					Match: protocol.PolicyMatch{
						PredicateType:     protocol.PredicateTypeManagedResourceV1,
						RootAuthorization: true,
					},
					LiveCredential: protocol.RequirementNone,
					Provenance:     protocol.RequirementRequired,
					Profiles:       []protocol.ProfileConfig{profile},
				},
				{
					Match: protocol.PolicyMatch{
						PredicateType:     protocol.PredicateTypeFulfillmentRelationV1,
						RootAuthorization: false,
					},
					LiveCredential: protocol.RequirementNone,
					Provenance:     protocol.RequirementRequired,
					Profiles:       []protocol.ProfileConfig{profile},
				},
				{
					Match: protocol.PolicyMatch{
						PredicateType:     directkey.PredicateTypeEnrollmentV1,
						RootAuthorization: true,
					},
					LiveCredential: protocol.RequirementNone,
					Provenance:     protocol.RequirementRequired,
					Profiles:       []protocol.ProfileConfig{profile},
				},
				{
					Match: protocol.PolicyMatch{
						PredicateType:     protocol.PredicateTypeTrustConfigUpdateV1,
						RootAuthorization: true,
					},
					LiveCredential: protocol.RequirementNone,
					Provenance:     protocol.RequirementRequired,
					Profiles:       []protocol.ProfileConfig{profile},
				},
			},
		}},
	}
}
