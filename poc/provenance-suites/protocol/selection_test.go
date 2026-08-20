package protocol

import (
	"context"
	"errors"
	"testing"
)

func TestStaticTenantMappingFailsClosedWhenUnconfigured(t *testing.T) {
	_, err := TenantMapping{}.Map("")
	if !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("error = %v, want ErrTenantMismatch", err)
	}
}

func TestSelectAndVerifyAcceptsFirstMatchingProfile(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	var tried []ProvenanceType
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				tried = append(tried, pt)
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}

	got, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if got.Principal.Subject != "alice" {
		t.Fatalf("subject = %q, want alice", got.Principal.Subject)
	}
	if len(tried) != 1 || tried[0] != ProvenanceTypeDirectKeyV1 {
		t.Fatalf("tried profiles = %v, want [%s]", tried, ProvenanceTypeDirectKeyV1)
	}
}

func TestSelectAndVerifyPassesTheWholeStatementToVerify(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	support := SupportMaterial{
		MediaType: "application/test+json",
		Bytes:     []byte("helpers"),
	}
	var got SignedStatement
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(req VerifyRequest) (AuthenticatedEvidence, error) {
				got = req.Statement
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}

	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence, Support: support}, delivery, trust, lookup)
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if string(got.Evidence.Bytes) != string(evidence.Bytes) {
		t.Fatal("Verify did not receive the statement evidence")
	}
	if string(got.Support.Bytes) != "helpers" {
		t.Fatalf("Verify support = %q, want helpers", got.Support.Bytes)
	}
}

func TestSelectAndVerifyRejectsPolicyProfileThatIsNotInstalled(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []ProfileConfig{{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Parameters:     []byte(`{"not":"installed"}`),
	}}
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{pt: pt}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrUnknownProvenanceType) && !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want uninstalled profile to fail closed", err)
	}
}

func TestSelectAndVerifyRejectsUnknownProvenanceType(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	evidence.ProvenanceType = "unknown/v1"
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, func(ProvenanceType) (TargetAPI, bool) {
		return nil, false
	})
	if !errors.Is(err, ErrUnknownProvenanceType) {
		t.Fatalf("error = %v, want ErrUnknownProvenanceType", err)
	}
}

func TestSelectAndVerifyRejectsUnknownAuthority(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := func(ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: evidence.ProvenanceType,
			hints: TentativeHints{
				Scheme:        IdentitySchemeOIDCSubV1,
				Authority:     "https://unknown.example.test",
				Subject:       "alice",
				PredicateType: PredicateTypeDeploymentV1,
			},
		}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrUnknownAuthority) {
		t.Fatalf("error = %v, want ErrUnknownAuthority", err)
	}
}

func TestSelectAndVerifyRejectsPredicateTypeOutsideMatchedPolicy(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			hints: TentativeHints{
				Scheme:        IdentitySchemeOIDCSubV1,
				Authority:     "https://issuer.example.test",
				Subject:       "alice",
				PredicateType: PredicateTypeManagedResourceV1,
			},
		}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrNoMatchingPolicy) {
		t.Fatalf("error = %v, want ErrNoMatchingPolicy", err)
	}
}

func TestSelectAndVerifyRejectsAmbiguousPolicies(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	trust.AuthorityRegistry[0].DeliveryPolicies = append(trust.AuthorityRegistry[0].DeliveryPolicies, trust.AuthorityRegistry[0].DeliveryPolicies[0])
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{pt: pt}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrAmbiguousPolicy) {
		t.Fatalf("error = %v, want ErrAmbiguousPolicy", err)
	}
}

func TestSelectAndVerifyDoesNotFallBackAcrossProvenanceTypes(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	other := ProfileConfig{ProvenanceType: "other/v1"}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []ProfileConfig{
		other,
		trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles[0],
	}
	triedOther := false
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				if pt == "other/v1" {
					triedOther = true
				}
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}
	got, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if triedOther {
		t.Fatal("selection used a profile whose provenance type did not match the evidence")
	}
	if got.ProvenanceType != ProvenanceTypeDirectKeyV1 {
		t.Fatalf("provenance type = %s, want %s", got.ProvenanceType, ProvenanceTypeDirectKeyV1)
	}
}

func TestSelectAndVerifyBindsProfileDigestToTheProfileThatVerified(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []ProfileConfig{first, second}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(req VerifyRequest) (AuthenticatedEvidence, error) {
				auth := successfulEvidence(t, trust, evidence, delivery)
				digest, err := second.Digest()
				if err != nil {
					t.Fatalf("digest: %v", err)
				}
				auth.ProfileConfigDigest = digest
				return auth, nil
			},
		}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want ErrPolicyReevaluation", err)
	}
}

func TestSelectAndVerifyTriesNextProfileOfSameTypeAfterFailure(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []ProfileConfig{first, second}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}

	var tried []string
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(req VerifyRequest) (AuthenticatedEvidence, error) {
				tried = append(tried, string(req.ProfileConfig.Parameters))
				if string(req.ProfileConfig.Parameters) == `{"n":1}` {
					return AuthenticatedEvidence{}, ErrVerificationFailed
				}
				auth := successfulEvidence(t, trust, evidence, delivery)
				digest, err := req.ProfileConfig.Digest()
				if err != nil {
					t.Fatalf("profile digest: %v", err)
				}
				auth.ProfileConfigDigest = digest
				return auth, nil
			},
		}, true
	}
	got, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if len(tried) != 2 {
		t.Fatalf("tried %d profiles, want 2", len(tried))
	}
	want, err := second.Digest()
	if err != nil {
		t.Fatalf("second profile digest: %v", err)
	}
	if got.ProfileConfigDigest != want {
		t.Fatalf("selected profile digest = %q, want %q", got.ProfileConfigDigest, want)
	}
}

func TestSelectAndVerifyRejectsClaimedTenantMismatch(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	delivery.ClaimedTenant = "tenant-other"
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("error = %v, want ErrTenantMismatch", err)
	}
}

func TestSelectAndVerifyRejectsHintSubjectMismatch(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			hints: TentativeHints{
				Scheme:        IdentitySchemeOIDCSubV1,
				Authority:     "https://issuer.example.test",
				Subject:       "mallory",
				PredicateType: PredicateTypeDeploymentV1,
			},
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}
	_, _, err := SelectAndVerify(context.Background(), SignedStatement{Evidence: evidence}, delivery, trust, lookup)
	if !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want ErrPolicyReevaluation", err)
	}
}

type stubTarget struct {
	pt     ProvenanceType
	hints  TentativeHints
	verify func(VerifyRequest) (AuthenticatedEvidence, error)
}

func (s *stubTarget) ProvenanceType() ProvenanceType { return s.pt }

func (s *stubTarget) ParseHints(TypedEvidence) (TentativeHints, error) {
	if s.hints.Scheme != "" {
		return s.hints, nil
	}
	return TentativeHints{
		Scheme:        IdentitySchemeOIDCSubV1,
		Authority:     "https://issuer.example.test",
		Subject:       "alice",
		PredicateType: PredicateTypeDeploymentV1,
	}, nil
}

func (s *stubTarget) Verify(_ context.Context, req VerifyRequest) (AuthenticatedEvidence, TypedAssertion, error) {
	if s.verify != nil {
		auth, err := s.verify(req)
		if err != nil {
			return AuthenticatedEvidence{}, TypedAssertion{}, err
		}
		return auth, TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)}, nil
	}
	return AuthenticatedEvidence{}, TypedAssertion{}, ErrVerificationFailed
}

func selectionFixture(t *testing.T) (TrustConfiguration, TypedEvidence, DeliveryContext) {
	t.Helper()
	profile := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1}
	authority := AuthorityConfig{
		PrincipalAuthority: PrincipalAuthority{
			Scheme:    IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
		},
		TenantMapping:      TenantMapping{StaticTenant: "tenant-acme"},
		ProvenanceProfiles: []ProfileConfig{profile},
		DeliveryPolicies: []DeliveryPolicy{{
			Match: PolicyMatch{
				PredicateType:     PredicateTypeDeploymentV1,
				RootAuthorization: true,
			},
			LiveCredential: RequirementNone,
			Provenance:     RequirementRequired,
			Profiles:       []ProfileConfig{profile},
		}},
	}
	trust := TrustConfiguration{AuthorityRegistry: []AuthorityConfig{authority}}
	evidence := TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Encoded: Encoded{
			MediaType: "application/test+json",
			Bytes:     []byte(`{"hint":"alice"}`),
		},
	}
	delivery := DeliveryContext{
		ClaimedTenant:     "tenant-acme",
		PredicateType:     PredicateTypeDeploymentV1,
		RootAuthorization: true,
	}
	return trust, evidence, delivery
}

func successfulEvidence(t *testing.T, trust TrustConfiguration, evidence TypedEvidence, delivery DeliveryContext) AuthenticatedEvidence {
	t.Helper()
	authority := trust.AuthorityRegistry[0]
	authorityDigest, err := authority.Digest()
	if err != nil {
		t.Fatalf("authority digest: %v", err)
	}
	var profileDigest Digest
	foundProfile := false
	for _, profile := range authority.DeliveryPolicies[0].Profiles {
		if profile.ProvenanceType != evidence.ProvenanceType {
			continue
		}
		digest, err := profile.Digest()
		if err != nil {
			t.Fatalf("profile digest: %v", err)
		}
		profileDigest = digest
		foundProfile = true
		break
	}
	if !foundProfile {
		t.Fatalf("fixture policy has no profile of type %s", evidence.ProvenanceType)
	}
	assertion := TypedAssertion{PredicateType: delivery.PredicateType, Bytes: []byte(`{"ok":true}`)}
	contentDigest, err := assertion.Digest()
	if err != nil {
		t.Fatalf("content digest: %v", err)
	}
	return AuthenticatedEvidence{
		Principal: Principal{
			Scheme:    IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
			Subject:   "alice",
		},
		MappedFleetShiftTenant: "tenant-acme",
		PredicateType:          delivery.PredicateType,
		ContentDigest:          contentDigest,
		ProvenanceType:         evidence.ProvenanceType,
		AuthorityConfigDigest:  authorityDigest,
		ProfileConfigDigest:    profileDigest,
	}
}
