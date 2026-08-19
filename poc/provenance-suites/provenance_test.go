package provenancesuites

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/client"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/deliveryagent"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

const (
	testTenant = protocol.TenantID("tenant-acme")
	testTarget = "target-east"
	testIssuer = protocol.Authority("https://issuer.example.test")
)

func TestEnrollmentAndSignedDelivery(t *testing.T) {
	s := newEnrolledScenario(t)

	evidence, assertion := mustSign(t, s.user, "fulfillment-1", 1, []byte(`{"replicas":3}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, assertion); err != nil {
		t.Fatalf("submit delivery: %v", err)
	}
	applied, ok := s.agent.Applied("fulfillment-1")
	if !ok {
		t.Fatal("agent did not apply the delivery")
	}
	if got, want := string(applied.Payload), `{"replicas":3}`; got != want {
		t.Fatalf("applied payload = %s, want %s", got, want)
	}
}

func TestResourceManagerCannotForgeDeliverySignature(t *testing.T) {
	s := newEnrolledScenario(t)
	attacker := mustClient(t, "mallory")
	evidence, assertion := mustSign(t, attacker, "forged", 1, []byte(`{"replicas":9}`))

	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence, assertion)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("compromised push error = %v, want ErrVerificationFailed", err)
	}
	if _, ok := s.agent.Applied("forged"); ok {
		t.Fatal("agent applied a delivery signed by an unenrolled key")
	}
}

func TestResourceManagerCannotAlterSignedContent(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence, assertion := mustSign(t, s.user, "tamper", 1, []byte(`{"replicas":3}`))
	var authorization protocol.DeliveryAuthorization
	if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
		t.Fatalf("decode: %v", err)
	}
	authorization.Payload = []byte(`{"replicas":9}`)
	tampered, err := protocol.MarshalCanonical(authorization)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertion.Bytes = tampered
	_, err = s.manager.Compromised().PushDelivery(context.Background(), evidence, assertion)
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
	evidence, assertion := mustSign(t, s.user, "rbac", 1, []byte(`{"ok":true}`))
	if _, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, assertion); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("authorized delivery error = %v, want ErrUnauthorized", err)
	}
	if _, err := s.manager.Compromised().PushDelivery(context.Background(), evidence, assertion); err != nil {
		t.Fatalf("compromised genuine delivery: %v", err)
	}
	if _, ok := s.agent.Applied("rbac"); !ok {
		t.Fatal("agent rejected authentic provenance after RM authorization bypass")
	}
}

func TestUnknownProvenanceTypeFailsClosed(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence, assertion := mustSign(t, s.user, "unknown-type", 1, []byte(`{"ok":true}`))
	evidence.ProvenanceType = "unknown/v1"
	_, err := s.manager.Compromised().PushDelivery(context.Background(), evidence, assertion)
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
	evidence, assertion := mustSign(t, user, "too-early", 1, []byte(`{}`))
	err = agent.Deliver(resourcemanager.DeliveryPackage{Evidence: evidence, Assertion: assertion})
	if !errors.Is(err, protocol.ErrUninitializedVerifier) {
		t.Fatalf("error = %v, want ErrUninitializedVerifier", err)
	}
}

func TestAlteredAssertionBytesFailContentBinding(t *testing.T) {
	s := newEnrolledScenario(t)
	evidence, assertion := mustSign(t, s.user, "other-tenant", 1, []byte(`{"ok":true}`))
	var authorization protocol.DeliveryAuthorization
	if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
		t.Fatalf("decode: %v", err)
	}
	authorization.TenantID = "tenant-other"
	tampered, err := protocol.MarshalCanonical(authorization)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	assertion.Bytes = tampered
	_, err = s.manager.Compromised().PushDelivery(context.Background(), evidence, assertion)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("error = %v, want ErrVerificationFailed", err)
	}
}

func TestRetryAfterLostAcknowledgementIsIdempotent(t *testing.T) {
	s := newEnrolledScenario(t)
	s.agent.LoseNextAcknowledgement()
	evidence, assertion := mustSign(t, s.user, "lost-ack", 1, []byte(`{"ok":true}`))
	commitment, err := s.manager.SubmitDelivery(context.Background(), s.user.Principal(), evidence, assertion)
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

type scenario struct {
	user    *client.Client
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
	enrollment, err := s.user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("create enrollment: %v", err)
	}
	if err := s.manager.SubmitDirectKeyEnrollment(context.Background(), s.user.Principal(), enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if _, ok := s.agent.PublicKey(s.user.Principal()); !ok {
		t.Fatal("agent did not retain the enrollment mapping")
	}
	return s
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

func mustSign(t *testing.T, c *client.Client, fulfillment string, generation uint64, payload []byte) (protocol.TypedEvidence, protocol.TypedAssertion) {
	t.Helper()
	evidence, assertion, err := c.SignDelivery(context.Background(), protocol.DeliveryAuthorization{
		TargetID:      testTarget,
		FulfillmentID: fulfillment,
		Generation:    generation,
		Action:        protocol.ActionPut,
		Payload:       payload,
	})
	if err != nil {
		t.Fatalf("sign delivery: %v", err)
	}
	return evidence, assertion
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
			DeliveryPolicies: []protocol.DeliveryPolicy{{
				Match: protocol.PolicyMatch{
					ContentType:       protocol.ContentTypeDeliveryAuthorizationV1,
					RootAuthorization: true,
				},
				LiveCredential: protocol.RequirementNone,
				Provenance:     protocol.RequirementRequired,
				Profiles:       []protocol.ProfileConfig{profile},
			}},
		}},
	}
}
