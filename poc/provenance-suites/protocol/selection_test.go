package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
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

	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if got.Authenticated.Principal.Subject != "alice" {
		t.Fatalf("subject = %q, want alice", got.Authenticated.Principal.Subject)
	}
	if len(tried) != 1 || tried[0] != ProvenanceTypeDirectKeyV1 {
		t.Fatalf("tried profiles = %v, want [%s]", tried, ProvenanceTypeDirectKeyV1)
	}
}

func TestSelectAndVerifyCopiesOnlySignedStatementIntoVerifyRequest(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	support := SupportMaterial{
		MediaType: "application/test+json",
		Bytes:     []byte("helpers"),
	}
	trap := Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	item := Item{
		SignedStatement: SignedStatement{Evidence: evidence, Support: support},
		EvidenceLog: &EvidenceLogInclusion{
			Index:          7,
			InclusionProof: []Digest{trap},
		},
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

	_, err := SelectAndVerify(context.Background(), item, delivery, trust, lookup, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if string(got.Evidence.Bytes) != string(evidence.Bytes) {
		t.Fatal("BeginVerification did not receive the statement evidence")
	}
	if string(got.Support.Bytes) != "helpers" {
		t.Fatalf("statement support = %q, want helpers", got.Support.Bytes)
	}
	if bytes.Contains(got.Evidence.Bytes, []byte(trap)) || bytes.Contains(got.Support.Bytes, []byte(trap)) {
		t.Fatal("VerifyRequest.Statement contained the item inclusion trap")
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if !errors.Is(err, ErrUnknownProvenanceType) && !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want uninstalled profile to fail closed", err)
	}
}

func TestSelectAndVerifyRejectsUnknownProvenanceType(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	evidence.ProvenanceType = "unknown/v1"
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, func(ProvenanceType) (TargetAPI, bool) {
		return nil, false
	}, defaultServices())
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
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
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if triedOther {
		t.Fatal("selection used a profile whose provenance type did not match the evidence")
	}
	if got.Authenticated.ProvenanceType != ProvenanceTypeDirectKeyV1 {
		t.Fatalf("provenance type = %s, want %s", got.Authenticated.ProvenanceType, ProvenanceTypeDirectKeyV1)
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
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
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
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
	if got.Authenticated.ProfileConfigDigest != want {
		t.Fatalf("selected profile digest = %q, want %q", got.Authenticated.ProfileConfigDigest, want)
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("error = %v, want ErrTenantMismatch", err)
	}
}

func TestTargetAPIApplyUnknownPredicateFailsClosed(t *testing.T) {
	target := &stubTarget{pt: ProvenanceTypeDirectKeyV1}
	if target.Owns("not-a-suite-predicate/v1") {
		t.Fatal("stub owned an undeclared predicate")
	}
	err := target.Apply(context.Background(), ApplyRequest{
		Authenticated: AuthenticatedEvidence{PredicateType: "not-a-suite-predicate/v1"},
		Assertion: TypedAssertion{
			PredicateType: "not-a-suite-predicate/v1",
			Bytes:         []byte(`{}`),
		},
	})
	if !errors.Is(err, ErrUnknownPredicateType) {
		t.Fatalf("error = %v, want ErrUnknownPredicateType", err)
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
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want ErrPolicyReevaluation", err)
	}
}

func TestSelectAndVerifyRejectsOccurrenceIdentityMismatch(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	log := &fakeLogVerifier{
		evidence: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{Log: log})
	if !errors.Is(err, ErrInvalidLogInclusion) || !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want identity mismatch to fail the candidate", err)
	}
}

func TestSelectAndVerifyProjectsOnlyLogPosition(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	item := itemFor(evidence)
	got, err := SelectAndVerify(context.Background(), item, delivery, trust, lookup, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if got.Temporal.LogPosition == nil {
		t.Fatal("LogPosition is nil after verified inclusion")
	}
	if got.Temporal.LogPosition.Domain != LogDomainTenantEvidenceV1 || got.Temporal.LogPosition.Index != item.EvidenceLog.Index {
		t.Fatalf("LogPosition = %+v, want tenant domain index %d", got.Temporal.LogPosition, item.EvidenceLog.Index)
	}
	if len(got.Temporal.Times) != 0 {
		t.Fatalf("Times = %v, want empty without timestamp bindings", got.Temporal.Times)
	}
}

func TestSelectAndVerifyRejectsOccurrenceOutsideRequiredDomain(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	log := &fakeLogVerifier{domain: "other-log/v1"}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{Log: log})
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("error = %v, want ErrTemporalValidity for a non-tenant occurrence domain", err)
	}
}

func TestSelectAndVerifyRejectsMissingInclusion(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	item := Item{SignedStatement: SignedStatement{Evidence: evidence}}
	_, err := SelectAndVerify(context.Background(), item, delivery, trust, lookup, defaultServices())
	if !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
	}
}

func TestSelectAndVerifyOccurrenceFailureTriesNextProfile(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []ProfileConfig{first, second}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}

	log := &fakeLogVerifier{failLeft: 1}
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(req VerifyRequest) (AuthenticatedEvidence, error) {
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
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{Log: log})
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	want, err := second.Digest()
	if err != nil {
		t.Fatalf("second profile digest: %v", err)
	}
	if got.Authenticated.ProfileConfigDigest != want {
		t.Fatalf("selected profile digest = %q, want %q", got.Authenticated.ProfileConfigDigest, want)
	}
	if log.calls < 2 {
		t.Fatalf("VerifyOccurrence calls = %d, want at least 2", log.calls)
	}
}

func TestSelectAndVerifyResultCarriesSelectedContext(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	authority := trust.AuthorityRegistry[0]
	if !got.Authority.Equal(authority) {
		t.Fatal("result Authority is not the selected authority")
	}
	if got.Profile.ProvenanceType != ProvenanceTypeDirectKeyV1 {
		t.Fatalf("result Profile type = %s", got.Profile.ProvenanceType)
	}
	if got.Policy.Provenance != RequirementRequired || got.Policy.Match.PredicateType != PredicateTypeDeploymentV1 {
		t.Fatalf("result Policy = %+v", got.Policy)
	}
}

func TestSelectAndVerifyEmptyConstraintsYieldUnboundedWindow(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	item := itemFor(evidence)
	got, err := SelectAndVerify(context.Background(), item, delivery, trust, lookup, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if got.Validity.Window.EstablishedBy != nil || got.Validity.Window.RetiredBy != nil {
		t.Fatalf("window = %+v, want unbounded", got.Validity.Window)
	}
	if got.Temporal.LogPosition == nil || got.Temporal.LogPosition.Index != item.EvidenceLog.Index {
		t.Fatalf("LogPosition = %+v, want verified inclusion index", got.Temporal.LogPosition)
	}
}

func TestSelectAndVerifyNilTimeFailsClosedWhenPrepareReturnsBindings(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			prepare: func() (TemporalPreparation, error) {
				return TemporalPreparation{Timestamps: []UnverifiedTimestampBinding{{
					Format:  TimestampFormatRFC3161V1,
					Token:   []byte("token"),
					Message: []byte("message"),
				}}}, nil
			},
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, defaultServices())
	if err == nil {
		t.Fatal("SelectAndVerify succeeded with timestamp bindings and a nil Time verifier")
	}
	if !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want ErrNoSuccessfulProfile", err)
	}
}

func TestSelectAndVerifyFakeTimestampPathClonesAndProjects(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	token := []byte("token-bytes")
	message := []byte("message-bytes")
	binding := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   token,
		Message: message,
	}
	wantID, err := binding.Clone().Identity()
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	var finishSaw []VerifiedTimeObservation
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			prepare: func() (TemporalPreparation, error) {
				return TemporalPreparation{Timestamps: []UnverifiedTimestampBinding{{
					Format:  TimestampFormatRFC3161V1,
					Token:   token,
					Message: message,
				}}}, nil
			},
			finish: func(inputs VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error) {
				finishSaw = append([]VerifiedTimeObservation(nil), inputs.Timestamps...)
				auth := successfulEvidence(t, trust, evidence, delivery)
				return ProvenanceAuthenticationResult{
					Authenticated: auth,
					Assertion:     TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)},
				}, nil
			},
		}, true
	}
	fakeTime := &fakeTimeVerifier{mutate: true, earliest: t0, latest: t0.Add(time.Second)}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{
		Log:  &fakeLogVerifier{},
		Time: fakeTime,
	})
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if fakeTime.calls != 1 {
		t.Fatalf("VerifyTimestamp calls = %d, want 1", fakeTime.calls)
	}
	if len(finishSaw) != 1 || finishSaw[0].Binding != wantID {
		t.Fatalf("Finish observations = %+v, want Binding %q", finishSaw, wantID)
	}
	if len(got.Temporal.Times) != 1 {
		t.Fatalf("Times = %+v, want one projected observation", got.Temporal.Times)
	}
	if got.Temporal.Times[0].Authority != "tsa-test" || !got.Temporal.Times[0].Earliest.Equal(t0) || !got.Temporal.Times[0].Latest.Equal(t0.Add(time.Second)) {
		t.Fatalf("projected time = %+v", got.Temporal.Times[0])
	}
}

func TestSelectAndVerifyProjectsCoordinatorObservationsNotFinishMutations(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			prepare: func() (TemporalPreparation, error) {
				return TemporalPreparation{Timestamps: []UnverifiedTimestampBinding{{
					Format:  TimestampFormatRFC3161V1,
					Token:   []byte("token"),
					Message: []byte("message"),
				}}}, nil
			},
			finish: func(inputs VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error) {
				if len(inputs.Timestamps) != 1 {
					t.Fatalf("Finish timestamps = %d, want 1", len(inputs.Timestamps))
				}
				inputs.Timestamps[0].Authority = "invented-tsa"
				inputs.Timestamps[0].Earliest = t0.Add(-time.Hour)
				inputs.Timestamps[0].Latest = t0.Add(-time.Hour)
				auth := successfulEvidence(t, trust, evidence, delivery)
				return ProvenanceAuthenticationResult{
					Authenticated: auth,
					Assertion:     TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)},
				}, nil
			},
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{
		Log:  &fakeLogVerifier{},
		Time: &fakeTimeVerifier{earliest: t0, latest: t0.Add(time.Second)},
	})
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if len(got.Temporal.Times) != 1 {
		t.Fatalf("Times = %+v, want one observation", got.Temporal.Times)
	}
	if got.Temporal.Times[0].Authority != "tsa-test" || !got.Temporal.Times[0].Earliest.Equal(t0) || !got.Temporal.Times[0].Latest.Equal(t0.Add(time.Second)) {
		t.Fatalf("projected time = %+v, Finish must not invent coordinator facts", got.Temporal.Times[0])
	}
}

func TestSelectAndVerifyIdentityErrorDoesNotCallTimeAdapter(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			prepare: func() (TemporalPreparation, error) {
				return TemporalPreparation{Timestamps: []UnverifiedTimestampBinding{{
					Format:  "unknown/v1",
					Token:   []byte("token"),
					Message: []byte("message"),
				}}}, nil
			},
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}
	fakeTime := &fakeTimeVerifier{}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{
		Log:  &fakeLogVerifier{},
		Time: fakeTime,
	})
	if !errors.Is(err, ErrInvalidTimestampBinding) {
		t.Fatalf("error = %v, want ErrInvalidTimestampBinding", err)
	}
	if fakeTime.calls != 0 {
		t.Fatalf("VerifyTimestamp calls = %d, want 0", fakeTime.calls)
	}
}

func TestSelectAndVerifyFailedFirstProfileContributesNoTemporalFacts(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []ProfileConfig{first, second}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &failThenSucceedTarget{
			t:        t,
			pt:       pt,
			trust:    trust,
			evidence: evidence,
			delivery: delivery,
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{
		Log:  &fakeLogVerifier{},
		Time: &fakeTimeVerifier{earliest: t0, latest: t0},
	})
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if len(got.Temporal.Times) != 0 {
		t.Fatalf("Times = %+v, want no facts from the failed first profile", got.Temporal.Times)
	}
	want, err := second.Digest()
	if err != nil {
		t.Fatalf("second digest: %v", err)
	}
	if got.Authenticated.ProfileConfigDigest != want {
		t.Fatalf("selected profile digest = %q, want second profile", got.Authenticated.ProfileConfigDigest)
	}
}

func TestSelectAndVerifyNilLogFailsWhenInclusionRequired(t *testing.T) {
	trust, evidence, delivery := selectionFixture(t)
	lookup := successLookup(t, trust, evidence, delivery)
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), delivery, trust, lookup, TemporalVerificationServices{})
	if !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
	}
}

type stubTarget struct {
	pt      ProvenanceType
	hints   TentativeHints
	verify  func(VerifyRequest) (AuthenticatedEvidence, error)
	prepare func() (TemporalPreparation, error)
	finish  func(VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error)
	owns    map[PredicateType]bool
}

func (s *stubTarget) ProvenanceType() ProvenanceType { return s.pt }

func (s *stubTarget) RequiresEvidenceLog() bool { return false }

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

func (s *stubTarget) BeginVerification(_ context.Context, req VerifyRequest) (ProvenanceVerificationSession, error) {
	return &stubSession{target: s, req: req}, nil
}

func (s *stubTarget) Owns(predicate PredicateType) bool {
	return s.owns != nil && s.owns[predicate]
}

func (s *stubTarget) Apply(_ context.Context, req ApplyRequest) error {
	return fmt.Errorf("%w: %s", ErrUnknownPredicateType, req.Authenticated.PredicateType)
}

type stubSession struct {
	target   *stubTarget
	req      VerifyRequest
	prepared bool
	finished bool
	failed   bool
}

func (s *stubSession) Prepare(context.Context) (TemporalPreparation, error) {
	if s.failed || s.prepared {
		s.failed = true
		return TemporalPreparation{}, fmt.Errorf("%w: session Prepare reused", ErrVerificationFailed)
	}
	s.prepared = true
	if s.target.prepare != nil {
		prep, err := s.target.prepare()
		if err != nil {
			s.failed = true
			return TemporalPreparation{}, err
		}
		return prep, nil
	}
	return TemporalPreparation{}, nil
}

func (s *stubSession) Finish(_ context.Context, inputs VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error) {
	if s.failed || !s.prepared || s.finished {
		s.failed = true
		return ProvenanceAuthenticationResult{}, fmt.Errorf("%w: session Finish reused or unprepared", ErrVerificationFailed)
	}
	s.finished = true
	if s.target.finish != nil {
		result, err := s.target.finish(inputs)
		if err != nil {
			s.failed = true
			return ProvenanceAuthenticationResult{}, err
		}
		return result, nil
	}
	if s.target.verify != nil {
		auth, err := s.target.verify(s.req)
		if err != nil {
			s.failed = true
			return ProvenanceAuthenticationResult{}, err
		}
		return ProvenanceAuthenticationResult{
			Authenticated: auth,
			Assertion:     TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)},
		}, nil
	}
	s.failed = true
	return ProvenanceAuthenticationResult{}, ErrVerificationFailed
}

type failThenSucceedTarget struct {
	t        *testing.T
	pt       ProvenanceType
	trust    TrustConfiguration
	evidence TypedEvidence
	delivery DeliveryContext
	attempts int
}

func (s *failThenSucceedTarget) ProvenanceType() ProvenanceType { return s.pt }
func (s *failThenSucceedTarget) RequiresEvidenceLog() bool      { return false }
func (s *failThenSucceedTarget) ParseHints(TypedEvidence) (TentativeHints, error) {
	return TentativeHints{
		Scheme:        IdentitySchemeOIDCSubV1,
		Authority:     "https://issuer.example.test",
		Subject:       "alice",
		PredicateType: PredicateTypeDeploymentV1,
	}, nil
}
func (s *failThenSucceedTarget) Owns(PredicateType) bool { return false }
func (s *failThenSucceedTarget) Apply(context.Context, ApplyRequest) error {
	return ErrUnknownPredicateType
}
func (s *failThenSucceedTarget) BeginVerification(_ context.Context, req VerifyRequest) (ProvenanceVerificationSession, error) {
	s.attempts++
	return &failThenSucceedSession{parent: s, req: req, attempt: s.attempts}, nil
}

type failThenSucceedSession struct {
	parent  *failThenSucceedTarget
	req     VerifyRequest
	attempt int
}

func (s *failThenSucceedSession) Prepare(context.Context) (TemporalPreparation, error) {
	if s.attempt == 1 {
		return TemporalPreparation{Timestamps: []UnverifiedTimestampBinding{{
			Format:  TimestampFormatRFC3161V1,
			Token:   []byte("failed-attempt"),
			Message: []byte("failed-attempt"),
		}}}, nil
	}
	return TemporalPreparation{}, nil
}

func (s *failThenSucceedSession) Finish(context.Context, VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error) {
	if s.attempt == 1 {
		return ProvenanceAuthenticationResult{}, ErrVerificationFailed
	}
	auth := successfulEvidence(s.parent.t, s.parent.trust, s.parent.evidence, s.parent.delivery)
	digest, err := s.req.ProfileConfig.Digest()
	if err != nil {
		return ProvenanceAuthenticationResult{}, err
	}
	auth.ProfileConfigDigest = digest
	return ProvenanceAuthenticationResult{
		Authenticated: auth,
		Assertion:     TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)},
	}, nil
}

type fakeLogVerifier struct {
	domain   LogDomainID
	evidence Digest
	failLeft int
	fail     error
	calls    int
}

func (f *fakeLogVerifier) VerifyOccurrence(_ context.Context, evidence TypedEvidence, inclusion EvidenceLogInclusion) (VerifiedEvidenceLogBinding, error) {
	f.calls++
	if f.failLeft > 0 {
		f.failLeft--
		if f.fail != nil {
			return VerifiedEvidenceLogBinding{}, f.fail
		}
		return VerifiedEvidenceLogBinding{}, ErrInvalidLogInclusion
	}
	id, err := evidence.Identity()
	if err != nil {
		return VerifiedEvidenceLogBinding{}, err
	}
	if f.evidence != "" {
		id = f.evidence
	}
	domain := f.domain
	if domain == "" {
		domain = LogDomainTenantEvidenceV1
	}
	return VerifiedEvidenceLogBinding{
		Position: LogPosition{Domain: domain, Index: inclusion.Index},
		Evidence: id,
	}, nil
}

type fakeTimeVerifier struct {
	mutate   bool
	calls    int
	earliest time.Time
	latest   time.Time
}

func (f *fakeTimeVerifier) VerifyTimestamp(_ context.Context, binding UnverifiedTimestampBinding) (VerifiedTimestampResult, error) {
	f.calls++
	if f.mutate {
		if len(binding.Token) > 0 {
			binding.Token[0] ^= 0xff
		}
		if len(binding.Message) > 0 {
			binding.Message[0] ^= 0xff
		}
	}
	authority := TimestampAuthorityID("tsa-test")
	earliest, latest := f.earliest, f.latest
	if earliest.IsZero() {
		earliest = time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
		latest = earliest
	}
	return VerifiedTimestampResult{Authority: authority, Earliest: earliest, Latest: latest}, nil
}

func successLookup(t *testing.T, trust TrustConfiguration, evidence TypedEvidence, delivery DeliveryContext) TargetLookup {
	t.Helper()
	return func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence, delivery), nil
			},
		}, true
	}
}

func itemFor(evidence TypedEvidence) Item {
	return Item{
		SignedStatement: SignedStatement{Evidence: evidence},
		EvidenceLog:     &EvidenceLogInclusion{Index: 7},
	}
}

func defaultServices() TemporalVerificationServices {
	return TemporalVerificationServices{Log: &fakeLogVerifier{}}
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
