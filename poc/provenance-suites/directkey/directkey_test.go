package directkey

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestVerifyRejectsNonEmptyProfileParameters(t *testing.T) {
	client := newTestClient(t)
	target := NewTarget()
	mustEnroll(t, target, client)
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	_, _, err = target.Verify(context.Background(), protocol.VerifyRequest{
		Statement:       protocol.SignedStatement{Evidence: evidence},
		ProfileConfig:   protocol.ProfileConfig{ProvenanceType: protocol.ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"fulcio":"no"}`)},
		AuthorityConfig: testAuthority(),
		DeliveryContext: testDeliveryContext(),
	})
	if !errors.Is(err, protocol.ErrUnknownProvenanceType) {
		t.Fatalf("error = %v, want ErrUnknownProvenanceType", err)
	}
}

func TestVerifyRejectsAuthorityConfigForADifferentPrincipalAuthority(t *testing.T) {
	client := newTestClient(t)
	target := NewTarget()
	mustEnroll(t, target, client)
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	authority := testAuthority()
	authority.PrincipalAuthority.Authority = "https://other.example.test"
	_, _, err = target.Verify(context.Background(), protocol.VerifyRequest{
		Statement:       protocol.SignedStatement{Evidence: evidence},
		ProfileConfig:   testProfile(),
		AuthorityConfig: authority,
		DeliveryContext: testDeliveryContext(),
	})
	if !errors.Is(err, protocol.ErrUnknownAuthority) {
		t.Fatalf("error = %v, want ErrUnknownAuthority", err)
	}
}

func TestCreateEvidenceCarriesUserReferenceNotPublicKey(t *testing.T) {
	client := newTestClient(t)
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	if evidence.ProvenanceType != protocol.ProvenanceTypeDirectKeyV1 {
		t.Fatalf("provenance type = %s", evidence.ProvenanceType)
	}
	if evidence.MediaType != MediaTypeSignature {
		t.Fatalf("media type = %s, want %s", evidence.MediaType, MediaTypeSignature)
	}

	var asMap map[string]any
	if err := json.Unmarshal(evidence.Bytes, &asMap); err != nil {
		t.Fatalf("unmarshal evidence: %v", err)
	}
	if _, exists := asMap["public_key"]; exists {
		t.Fatal("delivery evidence carried a public_key field")
	}

	var body SignatureBody
	if err := json.Unmarshal(evidence.Bytes, &body); err != nil {
		t.Fatalf("unmarshal signature body: %v", err)
	}
	if !body.Principal.Equal(client.Principal()) {
		t.Fatalf("user reference = %#v, want %#v", body.Principal, client.Principal())
	}
	if len(body.Signature) == 0 {
		t.Fatal("delivery evidence has no signature")
	}
	if body.Assertion.PredicateType != protocol.PredicateTypeDeploymentV1 || len(body.Assertion.Bytes) == 0 {
		t.Fatal("delivery evidence did not embed the inner assertion")
	}
}

func TestParseHintsEnrollmentReturnsEnrollmentPredicate(t *testing.T) {
	client := newTestClient(t)
	enrollment, err := client.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	hints, err := NewTarget().ParseHints(enrollment)
	if err != nil {
		t.Fatalf("ParseHints: %v", err)
	}
	if hints.PredicateType != PredicateTypeEnrollmentV1 {
		t.Fatalf("predicate hint = %s, want %s", hints.PredicateType, PredicateTypeEnrollmentV1)
	}
	if hints.Subject != "alice" {
		t.Fatalf("subject hint = %q, want alice", hints.Subject)
	}
}

func TestVerifyAndApplyEnrollmentRetainsMapping(t *testing.T) {
	client := newTestClient(t)
	enrollment, err := client.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	if enrollment.MediaType != MediaTypeEnrollment {
		t.Fatalf("media type = %s, want %s", enrollment.MediaType, MediaTypeEnrollment)
	}

	target := NewTarget()
	authenticated, assertion, err := verifyEnrollment(t, target, enrollment)
	if err != nil {
		t.Fatalf("Verify enrollment: %v", err)
	}
	if authenticated.PredicateType != PredicateTypeEnrollmentV1 {
		t.Fatalf("authenticated predicate = %s, want %s", authenticated.PredicateType, PredicateTypeEnrollmentV1)
	}
	if assertion.PredicateType != PredicateTypeEnrollmentV1 {
		t.Fatalf("assertion predicate = %s, want %s", assertion.PredicateType, PredicateTypeEnrollmentV1)
	}
	if _, ok := target.PublicKey(client.Principal()); ok {
		t.Fatal("Verify retained the mapping; Apply should be the mapping transition")
	}

	if err := applyEnrollment(t, target, enrollment, authenticated, assertion); err != nil {
		t.Fatalf("Apply enrollment: %v", err)
	}
	got, ok := target.PublicKey(client.Principal())
	if !ok {
		t.Fatal("verifier did not retain the public key mapping")
	}
	if string(got) != string(client.PublicKey()) {
		t.Fatal("retained public key does not match enrolled key")
	}
}

func TestVerifyEnrollmentDoesNotRequireRetainedKey(t *testing.T) {
	client := newTestClient(t)
	enrollment, err := client.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	_, _, err = verifyEnrollment(t, NewTarget(), enrollment)
	if err != nil {
		t.Fatalf("TOFU Verify of enrollment: %v", err)
	}
}

func TestVerifyUsesRetainedMappingNotSupportMaterial(t *testing.T) {
	client := newTestClient(t)
	target := NewTarget()
	mustEnroll(t, target, client)
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}

	attacker := newTestClient(t)
	support := protocol.SupportMaterial{
		MediaType: MediaTypeEnrollment,
		Bytes:     attacker.PublicKey(),
	}
	authenticated, assertion, err := target.Verify(context.Background(), protocol.VerifyRequest{
		Statement: protocol.SignedStatement{
			Evidence: evidence,
			Support:  support,
		},
		ProfileConfig:   testProfile(),
		AuthorityConfig: testAuthority(),
		DeliveryContext: testDeliveryContext(),
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if authenticated.Principal.Subject != "alice" {
		t.Fatalf("subject = %q, want alice", authenticated.Principal.Subject)
	}
	if authenticated.MappedFleetShiftTenant != "tenant-acme" {
		t.Fatalf("tenant = %q, want tenant-acme", authenticated.MappedFleetShiftTenant)
	}
	if assertion.PredicateType != protocol.PredicateTypeDeploymentV1 {
		t.Fatalf("emitted predicate = %s, want %s", assertion.PredicateType, protocol.PredicateTypeDeploymentV1)
	}
}

func TestVerifyFailsWithoutEnrollment(t *testing.T) {
	client := newTestClient(t)
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	_, _, err = NewTarget().Verify(context.Background(), protocol.VerifyRequest{
		Statement:       protocol.SignedStatement{Evidence: evidence},
		ProfileConfig:   testProfile(),
		AuthorityConfig: testAuthority(),
		DeliveryContext: testDeliveryContext(),
	})
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("error = %v, want ErrVerificationFailed", err)
	}
}

func TestVerifyFailsWhenSignatureDoesNotMatchRetainedKey(t *testing.T) {
	alice := newTestClient(t)
	mallory := newTestClient(t)
	target := NewTarget()
	mustEnroll(t, target, alice)

	evidence, err := mallory.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	_, _, err = target.Verify(context.Background(), protocol.VerifyRequest{
		Statement:       protocol.SignedStatement{Evidence: evidence},
		ProfileConfig:   testProfile(),
		AuthorityConfig: testAuthority(),
		DeliveryContext: testDeliveryContext(),
	})
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("error = %v, want ErrVerificationFailed", err)
	}
}

func TestFirstEnrollmentIsUnauthenticatedTOFU(t *testing.T) {
	target := NewTarget()
	attacker, err := NewClient(protocol.Principal{
		Scheme:    protocol.IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   "alice",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	enrollment, err := attacker.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	authenticated, assertion, err := verifyEnrollment(t, target, enrollment)
	if err != nil {
		t.Fatalf("first enrollment Verify of claimed alice: %v", err)
	}
	if err := applyEnrollment(t, target, enrollment, authenticated, assertion); err != nil {
		t.Fatalf("first enrollment Apply of claimed alice: %v", err)
	}
	got, ok := target.PublicKey(attacker.Principal())
	if !ok || string(got) != string(attacker.PublicKey()) {
		t.Fatal("TOFU enrollment did not retain the attacker's key for the claimed subject")
	}
}

func TestVerifyEnrollmentRejectsTamperedPrincipal(t *testing.T) {
	alice := newTestClient(t)
	attacker, err := NewClient(protocol.Principal{
		Scheme:    protocol.IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   "mallory",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	enrollment, err := attacker.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	var body EnrollmentBody
	if err := json.Unmarshal(enrollment.Bytes, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	body.Principal = alice.Principal()
	raw, err := encodeJSON(body)
	if err != nil {
		t.Fatalf("encode substituted enrollment: %v", err)
	}
	enrollment.Bytes = raw

	_, _, err = verifyEnrollment(t, NewTarget(), enrollment)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("error = %v, want ErrVerificationFailed", err)
	}
}

func TestApplyRejectsKeySubstitutionForEstablishedPrincipal(t *testing.T) {
	alice := newTestClient(t)
	target := NewTarget()
	mustEnroll(t, target, alice)

	attacker, err := NewClient(alice.Principal())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	enrollment, err := attacker.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	authenticated, assertion, err := verifyEnrollment(t, target, enrollment)
	if err != nil {
		t.Fatalf("substitution Verify: %v", err)
	}
	err = applyEnrollment(t, target, enrollment, authenticated, assertion)
	if !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("error = %v, want ErrVerificationFailed", err)
	}
	got, _ := target.PublicKey(alice.Principal())
	if string(got) != string(alice.PublicKey()) {
		t.Fatal("retained mapping was replaced")
	}
}

func TestOwnsEnrollmentNotIntent(t *testing.T) {
	target := NewTarget()
	if !target.Owns(PredicateTypeEnrollmentV1) {
		t.Fatal("direct-key/v1 must own enrollment")
	}
	if target.Owns(protocol.PredicateTypeDeploymentV1) {
		t.Fatal("direct-key/v1 must not own deployment/v1")
	}
	if target.Owns(protocol.PredicateTypeTrustConfigUpdateV1) {
		t.Fatal("direct-key/v1 must not own trust-config-update/v1")
	}
	if target.Owns("not-owned/v1") {
		t.Fatal("direct-key/v1 must not own an unknown predicate")
	}
}

func TestApplyOfDeploymentPredicateFailsClosed(t *testing.T) {
	client := newTestClient(t)
	target := NewTarget()
	mustEnroll(t, target, client)
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	authenticated, assertion, err := target.Verify(context.Background(), protocol.VerifyRequest{
		Statement:       protocol.SignedStatement{Evidence: evidence},
		ProfileConfig:   testProfile(),
		AuthorityConfig: testAuthority(),
		DeliveryContext: testDeliveryContext(),
	})
	if err != nil {
		t.Fatalf("Verify deployment: %v", err)
	}
	err = target.Apply(context.Background(), protocol.ApplyRequest{
		Authenticated: authenticated,
		Assertion:     assertion,
		Statement:     protocol.SignedStatement{Evidence: evidence},
		Index:         1,
	})
	if !errors.Is(err, protocol.ErrUnknownPredicateType) {
		t.Fatalf("error = %v, want ErrUnknownPredicateType", err)
	}
}

func TestParseHintsFailClosedOnUnknownMediaType(t *testing.T) {
	_, err := NewTarget().ParseHints(protocol.TypedEvidence{
		ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
		Encoded: protocol.Encoded{
			MediaType: "application/unknown",
			Bytes:     []byte(`{}`),
		},
	})
	if !errors.Is(err, protocol.ErrUnknownMediaType) {
		t.Fatalf("error = %v, want ErrUnknownMediaType", err)
	}
}

func TestManagerStoresImmutableEvidenceAndEmptySupport(t *testing.T) {
	client := newTestClient(t)
	manager := NewManager()
	enrollment, err := client.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	if err := manager.CommitEnrollment(context.Background(), enrollment); err != nil {
		t.Fatalf("CommitEnrollment: %v", err)
	}
	key, ok := manager.PublicKey(client.Principal())
	if !ok || string(key) != string(client.PublicKey()) {
		t.Fatal("resource manager did not courier the enrollment public key")
	}

	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	identity, err := manager.StoreEvidence(context.Background(), evidence)
	if err != nil {
		t.Fatalf("StoreEvidence: %v", err)
	}
	want, err := evidence.Identity()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if identity != want {
		t.Fatalf("stored identity = %q, want %q", identity, want)
	}
	support, err := manager.AssembleSupportMaterial(context.Background(), evidence)
	if err != nil {
		t.Fatalf("AssembleSupportMaterial: %v", err)
	}
	if len(support.Bytes) != 0 {
		t.Fatalf("support material carried %d bytes; direct-key delivery must not courier the public key", len(support.Bytes))
	}
}

func TestDecodeAssertionThenDecodeDeliveryScope(t *testing.T) {
	client := newTestClient(t)
	want := testAssertion(t)
	evidence, err := client.CreateEvidence(context.Background(), want)
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	got, err := NewManager().DecodeAssertion(evidence)
	if err != nil {
		t.Fatalf("DecodeAssertion: %v", err)
	}
	if got.PredicateType != want.PredicateType {
		t.Fatalf("predicate = %s, want %s", got.PredicateType, want.PredicateType)
	}
	if string(got.Bytes) != string(want.Bytes) {
		t.Fatalf("assertion bytes = %s, want %s", got.Bytes, want.Bytes)
	}
	scope, err := protocol.DecodeDeliveryScope(got)
	if err != nil {
		t.Fatalf("DecodeDeliveryScope: %v", err)
	}
	if scope.TenantID != "tenant-acme" || scope.TargetID != "target-east" {
		t.Fatalf("scope = %+v", scope)
	}
}

func TestDecodeAssertionDoesNotAuthenticate(t *testing.T) {
	client := newTestClient(t)
	manager := NewManager()
	enrollment, err := client.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	if err := manager.CommitEnrollment(context.Background(), enrollment); err != nil {
		t.Fatalf("CommitEnrollment: %v", err)
	}
	evidence, err := client.CreateEvidence(context.Background(), testAssertion(t))
	if err != nil {
		t.Fatalf("CreateEvidence: %v", err)
	}
	var body SignatureBody
	if err := json.Unmarshal(evidence.Bytes, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	body.Assertion.Bytes = []byte(`{"tenant_id":"tenant-other"}`)
	raw, err := encodeJSON(body)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	evidence.Bytes = raw

	got, err := manager.DecodeAssertion(evidence)
	if err != nil {
		t.Fatalf("DecodeAssertion: %v", err)
	}
	if string(got.Bytes) != string(body.Assertion.Bytes) {
		t.Fatal("DecodeAssertion did not return the untrusted inner statement")
	}
	if _, err := manager.CheckDelivery(evidence); !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("CheckDelivery error = %v, want ErrVerificationFailed", err)
	}
}

func newTestClient(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient(protocol.Principal{
		Scheme:    protocol.IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   "alice",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func mustEnroll(t *testing.T, target *Target, client *Client) {
	t.Helper()
	enrollment, err := client.CreateEnrollment()
	if err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	authenticated, assertion, err := verifyEnrollment(t, target, enrollment)
	if err != nil {
		t.Fatalf("Verify enrollment: %v", err)
	}
	if err := applyEnrollment(t, target, enrollment, authenticated, assertion); err != nil {
		t.Fatalf("Apply enrollment: %v", err)
	}
}

func verifyEnrollment(t *testing.T, target *Target, enrollment protocol.TypedEvidence) (protocol.AuthenticatedEvidence, protocol.TypedAssertion, error) {
	t.Helper()
	return target.Verify(context.Background(), protocol.VerifyRequest{
		Statement:       protocol.SignedStatement{Evidence: enrollment},
		ProfileConfig:   testProfile(),
		AuthorityConfig: testAuthority(),
		DeliveryContext: protocol.DeliveryContext{
			ClaimedTenant:     "tenant-acme",
			RootAuthorization: true,
		},
	})
}

func applyEnrollment(t *testing.T, target *Target, enrollment protocol.TypedEvidence, authenticated protocol.AuthenticatedEvidence, assertion protocol.TypedAssertion) error {
	t.Helper()
	return target.Apply(context.Background(), protocol.ApplyRequest{
		Authenticated: authenticated,
		Assertion:     assertion,
		Statement:     protocol.SignedStatement{Evidence: enrollment},
		Index:         0,
	})
}

func testAssertion(t *testing.T) protocol.TypedAssertion {
	t.Helper()
	assertion, err := protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			TenantID:      "tenant-acme",
			TargetID:      "target-east",
			FulfillmentID: "fulfillment-1",
			Generation:    1,
			Action:        protocol.ActionPut,
		},
		Manifests: []protocol.TypedManifest{{
			MediaType: "application/vnd.example.replicas+json",
			Bytes:     []byte(`{"replicas":3}`),
		}},
	}.Assertion()
	if err != nil {
		t.Fatalf("assertion: %v", err)
	}
	return assertion
}

func testProfile() protocol.ProfileConfig {
	return protocol.ProfileConfig{ProvenanceType: protocol.ProvenanceTypeDirectKeyV1}
}

func testAuthority() protocol.AuthorityConfig {
	profile := testProfile()
	return protocol.AuthorityConfig{
		PrincipalAuthority: protocol.PrincipalAuthority{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
		},
		TenantMapping:      protocol.TenantMapping{StaticTenant: "tenant-acme"},
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
					PredicateType:     PredicateTypeEnrollmentV1,
					RootAuthorization: true,
				},
				LiveCredential: protocol.RequirementNone,
				Provenance:     protocol.RequirementRequired,
				Profiles:       []protocol.ProfileConfig{profile},
			},
		},
	}
}

func testDeliveryContext() protocol.DeliveryContext {
	return protocol.DeliveryContext{
		ClaimedTenant:     "tenant-acme",
		PredicateType:     protocol.PredicateTypeDeploymentV1,
		RootAuthorization: true,
	}
}
