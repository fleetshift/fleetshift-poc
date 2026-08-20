package provenancesuites

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/client"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/deliveryagent"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
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
	testResourceName         = "prod"
)

func TestEnrollmentAndSignedDelivery(t *testing.T) {
	s := newEnrolledScenario(t)

	evidence := mustSignDeployment(t, s.user, "fulfillment-1", 1, []byte(`{"replicas":3}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); err != nil {
		t.Fatalf("submit delivery: %v", err)
	}
	applied, ok := s.agent.Applied("fulfillment-1")
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

func TestResourceManagerCannotForgeDeliverySignature(t *testing.T) {
	s := newEnrolledScenario(t)
	attacker := mustClient(t, "mallory")
	evidence := mustSignDeployment(t, attacker, "forged", 1, []byte(`{"replicas":9}`))

	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("compromised push error = %v, want ErrVerificationFailed", err)
	}
	if _, ok := s.agent.Applied("forged"); ok {
		t.Fatal("agent applied a delivery signed by an unenrolled key")
	}
}

func TestResourceManagerCannotAlterSignedContent(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, "tamper", 1, []byte(`{"replicas":3}`))
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
	attacker := mustClient(t, "alice")
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

	genuine := mustClient(t, "alice")
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
	if err := s.manager.RegisterDirectKeyEnroller(s.agent); err != nil {
		t.Fatalf("register direct-key enroller: %v", err)
	}
	enrollment, err := s.user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("enrollment: %v", err)
	}
	if err := s.manager.SubmitDirectKeyEnrollment(context.Background(), s.user.Principal(), enrollment); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("authorized enrollment error = %v, want ErrUnauthorized", err)
	}
	if err := s.manager.Compromised().CommitEnrollment(context.Background(), enrollment); err != nil {
		t.Fatalf("compromised enrollment: %v", err)
	}
	evidence := mustSignDeployment(t, s.user, "rbac", 1, []byte(`{"ok":true}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("authorized delivery error = %v, want ErrUnauthorized", err)
	}
	if _, err := s.manager.Compromised().PushDelivery(context.Background(), evidence); err != nil {
		t.Fatalf("compromised genuine delivery: %v", err)
	}
	if _, ok := s.agent.Applied("rbac"); !ok {
		t.Fatal("agent rejected authentic provenance after RM authorization bypass")
	}
}

func TestUnknownProvenanceTypeFailsClosed(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, "unknown-type", 1, []byte(`{"ok":true}`))
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
	user := mustClient(t, "alice")
	evidence := mustSignDeployment(t, user, "too-early", 1, []byte(`{}`))
	err = agent.Deliver(resourcemanager.DeliveryPackage{Root: protocol.SignedStatement{Evidence: evidence}})
	if !errors.Is(err, protocol.ErrUninitializedVerifier) {
		t.Fatalf("error = %v, want ErrUninitializedVerifier", err)
	}
}

func TestAlteredAssertionBytesFailContentBinding(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, "other-tenant", 1, []byte(`{"ok":true}`))
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
	evidence := mustSignDeployment(t, s.user, "lost-ack", 1, []byte(`{"ok":true}`))
	commitment, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence)
	if !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("error = %v, want ErrAcknowledgementLost", err)
	}
	if _, ok := s.agent.Applied("lost-ack"); !ok {
		t.Fatal("agent did not apply before losing the acknowledgement")
	}
	if err := s.manager.RetryDelivery(context.Background(), commitment.Index); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestRetryManagedResourceAfterLostAcknowledgementReusesRelation(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	s.agent.LoseNextAcknowledgement()
	spec := json.RawMessage(`{"region":"us-east-1"}`)
	evidence := mustSignManagedResource(t, s.user, "cluster-retry", 1, spec)
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	commitment, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("error = %v, want ErrAcknowledgementLost", err)
	}
	if err := s.manager.RetryDelivery(context.Background(), commitment.Index); err != nil {
		t.Fatalf("retry: %v", err)
	}
	applied, ok := s.agent.Applied("cluster-retry")
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
	evidence := mustSignManagedResource(t, s.user, "cluster-1", 1, spec)
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)

	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if err != nil {
		t.Fatalf("submit managed resource: %v", err)
	}
	applied, ok := s.agent.Applied("cluster-1")
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
	evidence := mustSignManagedResource(t, s.user, "cluster-missing", 1, json.RawMessage(`{"region":"us-east-1"}`))
	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence)
	if !errors.Is(err, deliveryagent.ErrFulfillmentRelationRequired) {
		t.Fatalf("error = %v, want ErrFulfillmentRelationRequired", err)
	}
	if _, ok := s.agent.Applied("cluster-missing"); ok {
		t.Fatal("agent applied a managed resource with no fulfillment relation")
	}
}

func TestManagedResourceRejectsFulfillmentRelationWithWrongResourceType(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	evidence := mustSignManagedResource(t, s.user, "cluster-wrong-type", 1, json.RawMessage(`{"region":"us-east-1"}`))
	relEvidence := mustSignRelation(t, s.addon, "monitoring-stacks", testClusterSpecMediaType)
	_, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence)
	if err == nil {
		t.Fatal("accepted a fulfillment relation for a different resource type")
	}
	if _, ok := s.agent.Applied("cluster-wrong-type"); ok {
		t.Fatal("agent applied a managed resource with a mismatched relation")
	}
}

func TestManagedResourceRejectsRelationSignedByUnenrolledKey(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	rogue := mustClient(t, "rogue-addon")
	evidence := mustSignManagedResource(t, s.user, "cluster-rogue", 1, json.RawMessage(`{"region":"us-east-1"}`))
	relEvidence := mustSignRelation(t, rogue, testResourceType, testClusterSpecMediaType)
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence, relEvidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) && !errors.Is(err, protocol.ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want verification failure", err)
	}
	if _, ok := s.agent.Applied("cluster-rogue"); ok {
		t.Fatal("agent applied a managed resource with an unenrolled relation signer")
	}
}

func TestDeploymentIgnoresCourieredFulfillmentRelation(t *testing.T) {
	s := newEnrolledManagedResourceScenario(t)
	evidence := mustSignDeployment(t, s.user, "deploy-with-relation", 1, []byte(`{"replicas":3}`))
	relEvidence := mustSignRelation(t, s.addon, testResourceType, testClusterSpecMediaType)
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, relEvidence); err != nil {
		t.Fatalf("submit deployment: %v", err)
	}
	applied, ok := s.agent.Applied("deploy-with-relation")
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

func TestUnknownRootPredicateFailsClosed(t *testing.T) {
	s := newEnrolledScenario(t)
	scope := protocol.DeliveryScope{
		TenantID:      testTenant,
		TargetID:      testTarget,
		FulfillmentID: "unknown-pred",
		Generation:    1,
		Action:        protocol.ActionPut,
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
	if _, ok := s.agent.Applied("unknown-pred"); ok {
		t.Fatal("agent applied an unknown root predicate")
	}
}

func TestDeploymentRejectsMissingManifestMediaType(t *testing.T) {
	s := newEnrolledScenario(t)
	authorization := protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TenantID:      testTenant,
			TargetID:      testTarget,
			FulfillmentID: "missing-media",
			Generation:    1,
			Action:        protocol.ActionPut,
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
	if _, ok := s.agent.Applied("missing-media"); ok {
		t.Fatal("agent applied a deployment with an untyped manifest")
	}
}

func TestPredicateTypeTamperFailsContentBinding(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence := mustSignDeployment(t, s.user, "pred-tamper", 1, []byte(`{"replicas":3}`))
	evidence = tamperEmbeddedAssertion(t, evidence, func(assertion *protocol.TypedAssertion) {
		assertion.PredicateType = protocol.PredicateTypeManagedResourceV1
	})
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, protocol.ErrVerificationFailed) && !errors.Is(err, protocol.ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want content-digest or policy re-evaluation failure", err)
	}
}

func TestSignDeploymentRequiresCompleteDeliveryScope(t *testing.T) {
	c := mustClient(t, "alice")
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

func TestSignManagedResourceRequiresCompleteDeliveryScope(t *testing.T) {
	c := mustClient(t, "alice")
	_, err := c.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
		ResourceType: testResourceType,
		ResourceName: testResourceName,
		Spec:         json.RawMessage(`{}`),
	})
	if err == nil {
		t.Fatal("signed a managed resource with an empty delivery scope")
	}
}

type scenario struct {
	user    *client.Client
	addon   *client.Client
	manager *resourcemanager.Manager
	agent   *deliveryagent.Agent
}

func newScenario(t *testing.T) *scenario {
	t.Helper()
	user := mustClient(t, "alice")
	agent, err := deliveryagent.New(deliveryagent.Config{TenantID: testTenant, TargetID: testTarget})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	if err := agent.Bootstrap(testTrust()); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	manager := resourcemanager.New(testTenant, nil)
	if err := manager.RegisterAgent(testTarget, agent); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := manager.RegisterDirectKeyEnroller(agent); err != nil {
		t.Fatalf("register direct-key enroller: %v", err)
	}
	return &scenario{user: user, manager: manager, agent: agent}
}

func newEnrolledScenario(t *testing.T) *scenario {
	t.Helper()
	s := newScenario(t)
	enrollClient(t, s, s.user)
	return s
}

func newEnrolledManagedResourceScenario(t *testing.T) *scenario {
	t.Helper()
	s := newEnrolledScenario(t)
	s.addon = mustClient(t, "addon-clusters")
	enrollClient(t, s, s.addon)
	return s
}

func enrollClient(t *testing.T, s *scenario, c *client.Client) {
	t.Helper()
	enrollment, err := c.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("create enrollment: %v", err)
	}
	if err := s.manager.SubmitDirectKeyEnrollment(context.Background(), c.Principal(), enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if _, ok := s.agent.PublicKey(c.Principal()); !ok {
		t.Fatal("agent did not retain the enrollment mapping")
	}
}

func mustClient(t *testing.T, subject protocol.Subject) *client.Client {
	t.Helper()
	c, err := client.New(client.Config{
		TenantID: testTenant,
		Principal: protocol.Principal{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: testIssuer,
			Subject:   subject,
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

func mustSignDeployment(t *testing.T, c *client.Client, fulfillment string, generation uint64, payload []byte) protocol.TypedEvidence {
	t.Helper()
	evidence, err := c.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TargetID:      testTarget,
			FulfillmentID: fulfillment,
			Generation:    generation,
			Action:        protocol.ActionPut,
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

func mustSignManagedResource(t *testing.T, c *client.Client, fulfillment string, generation uint64, spec json.RawMessage) protocol.TypedEvidence {
	t.Helper()
	evidence, err := c.SignManagedResource(context.Background(), protocol.ManagedResourceAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TargetID:      testTarget,
			FulfillmentID: fulfillment,
			Generation:    generation,
			Action:        protocol.ActionPut,
		},
		ResourceType: testResourceType,
		ResourceName: testResourceName,
		Spec:         spec,
	})
	if err != nil {
		t.Fatalf("sign managed resource: %v", err)
	}
	return evidence
}

func mustSignRelation(t *testing.T, c *client.Client, resourceType string, mediaType protocol.MediaType) protocol.TypedEvidence {
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
			},
		}},
	}
}
