package protocol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
)

func TestSelectAndVerifyAcceptsFirstMatchingProfile(t *testing.T) {
	trust, evidence := selectionFixture(t)
	var tried []ProvenanceType
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				tried = append(tried, pt)
				return successfulEvidence(t, trust, evidence), nil
			},
		}, true
	}

	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
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
	trust, evidence := selectionFixture(t)
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
				return successfulEvidence(t, trust, evidence), nil
			},
		}, true
	}

	_, err := SelectAndVerify(context.Background(), item, trust, lookup, defaultServices())
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

func TestSelectAndVerifyDetachesOnlyProfileBoundaryInputs(t *testing.T) {
	trust, evidence := selectionFixture(t)
	trust.AuthorityRegistry[0].CredentialMethods = []string{"credential"}
	trust.AuthorityRegistry[0].ProvenanceProfiles[0].Parameters = []byte("anchor")
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles[0] = profileDigest(trust.AuthorityRegistry[0].ProvenanceProfiles[0])
	evidence.Bytes = []byte("original evidence")
	statement := SignedStatement{
		Evidence: evidence,
		Support:  SupportMaterial(Encoded{MediaType: "application/test", Bytes: []byte("original support")}),
	}

	parseInput := []byte(nil)
	beginInput := []byte(nil)
	target := &stubTarget{
		pt: ProvenanceTypeDirectKeyV1,
		parse: func(evidence TypedEvidence) (TentativeHints, error) {
			parseInput = append([]byte(nil), evidence.Bytes...)
			evidence.Bytes[0] = 'P'
			return TentativeHints{
				Scheme:    IdentitySchemeOIDCSubV1,
				Authority: "https://issuer.example.test",
				Subject:   "alice",
				Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
			}, nil
		},
		begin: func(req VerifyRequest) {
			beginInput = append([]byte(nil), req.Statement.Evidence.Bytes...)
			req.Statement.Evidence.Bytes[0] = 'B'
			req.Statement.Support.Bytes[0] = 'B'
			req.ProfileConfig.Parameters[0] = 'B'
			req.AuthorityConfig.CredentialMethods[0] = "mutated"
			req.AuthorityConfig.ProvenanceProfiles[0].Parameters[0] = 'B'
			req.AuthorityConfig.DeliveryPolicies[0].Profiles[0] = "mutated-reference"
		},
		verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
			return successfulEvidence(t, trust, evidence), nil
		},
	}
	result, err := SelectAndVerify(context.Background(), Item{SignedStatement: statement, EvidenceLog: &EvidenceLogInclusion{Index: 7}}, trust, func(ProvenanceType) (TargetAPI, bool) {
		return target, true
	}, defaultServices())
	if err != nil {
		t.Fatalf("SelectAndVerify: %v", err)
	}
	if !bytes.Equal(parseInput, []byte("original evidence")) || !bytes.Equal(beginInput, []byte("original evidence")) {
		t.Fatalf("profile inputs = parse %q, begin %q; want detached original evidence", parseInput, beginInput)
	}
	if !bytes.Equal(evidence.Bytes, []byte("original evidence")) || !bytes.Equal(statement.Support.Bytes, []byte("original support")) {
		t.Fatal("profile mutation escaped into caller-owned statement bytes")
	}
	if got := trust.AuthorityRegistry[0].CredentialMethods[0]; got != "credential" {
		t.Fatalf("credential method = %q, want original", got)
	}
	if got := string(trust.AuthorityRegistry[0].ProvenanceProfiles[0].Parameters); got != "anchor" {
		t.Fatalf("installed profile parameters = %q, want original", got)
	}
	if got := trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles[0]; got != profileDigest(trust.AuthorityRegistry[0].ProvenanceProfiles[0]) {
		t.Fatalf("policy profile reference = %q, want original", got)
	}
	if &result.Authority.ProvenanceProfiles[0].Parameters[0] != &trust.AuthorityRegistry[0].ProvenanceProfiles[0].Parameters[0] ||
		&result.Profile.Parameters[0] != &trust.AuthorityRegistry[0].ProvenanceProfiles[0].Parameters[0] ||
		&result.Policy.Profiles[0] != &trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles[0] {
		t.Fatal("selection copied immutable source configuration")
	}
}

func TestSelectAndVerifyRejectsPolicyProfileThatIsNotInstalled(t *testing.T) {
	trust, evidence := selectionFixture(t)
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []Digest{profileDigest(ProfileConfig{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Parameters:     []byte(`{"not":"installed"}`),
	})}
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{pt: pt}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
	if !errors.Is(err, ErrInvalidTrustConfiguration) {
		t.Fatalf("error = %v, want uninstalled profile to fail closed", err)
	}
}

func TestSelectAndVerifyRejectsUnknownProvenanceType(t *testing.T) {
	trust, evidence := selectionFixture(t)
	evidence.ProvenanceType = "unknown/v1"
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) {
		return nil, false
	}, defaultServices())
	if !errors.Is(err, ErrUnknownProvenanceType) {
		t.Fatalf("error = %v, want ErrUnknownProvenanceType", err)
	}
}

func TestSelectAndVerifyRejectsUnknownAuthority(t *testing.T) {
	trust, evidence := selectionFixture(t)
	lookup := func(ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: evidence.ProvenanceType,
			hints: TentativeHints{
				Scheme:    IdentitySchemeOIDCSubV1,
				Authority: "https://unknown.example.test",
				Subject:   "alice",
				Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
			},
		}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
	if !errors.Is(err, ErrUnknownAuthority) {
		t.Fatalf("error = %v, want ErrUnknownAuthority", err)
	}
}

func TestSelectAndVerifyRejectsPredicateTypeOutsideMatchedPolicy(t *testing.T) {
	trust, evidence := selectionFixture(t)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			hints: TentativeHints{
				Scheme:    IdentitySchemeOIDCSubV1,
				Authority: "https://issuer.example.test",
				Subject:   "alice",
				Assertion: TypedAssertion{PredicateType: PredicateTypeManagedResourceV1},
			},
		}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
	if !errors.Is(err, ErrNoMatchingPolicy) {
		t.Fatalf("error = %v, want ErrNoMatchingPolicy", err)
	}
}

func TestSelectAndVerifyUsesFirstMatchingPolicy(t *testing.T) {
	trust, evidence := selectionFixture(t)
	later := trust.AuthorityRegistry[0].DeliveryPolicies[0]
	later.Provenance = RequirementNone
	trust.AuthorityRegistry[0].DeliveryPolicies = append(trust.AuthorityRegistry[0].DeliveryPolicies, later)
	if err := trust.Validate(); err != nil {
		t.Fatal(err)
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, successLookup(t, trust, evidence), defaultServices())
	if err != nil {
		t.Fatal(err)
	}
	if got.Policy.Provenance != RequirementRequired {
		t.Fatalf("selected provenance requirement = %q, want first policy", got.Policy.Provenance)
	}
}

func TestSelectAndVerifyDoesNotFallBackAcrossProvenanceTypes(t *testing.T) {
	trust, evidence := selectionFixture(t)
	other := ProfileConfig{ProvenanceType: "other/v1"}
	trust.AuthorityRegistry[0].ProvenanceProfiles = append(trust.AuthorityRegistry[0].ProvenanceProfiles, other)
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []Digest{
		profileDigest(other),
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
				return successfulEvidence(t, trust, evidence), nil
			},
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
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
	trust, evidence := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []Digest{profileDigest(first), profileDigest(second)}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(req VerifyRequest) (AuthenticatedEvidence, error) {
				auth := successfulEvidence(t, trust, evidence)
				digest, err := second.Digest()
				if err != nil {
					t.Fatalf("digest: %v", err)
				}
				auth.ProfileConfigDigest = digest
				return auth, nil
			},
		}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
	if !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want ErrPolicyReevaluation", err)
	}
}

func TestSelectAndVerifyTriesNextProfileOfSameTypeAfterFailure(t *testing.T) {
	trust, evidence := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []Digest{profileDigest(first), profileDigest(second)}
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
				auth := successfulEvidence(t, trust, evidence)
				digest, err := req.ProfileConfig.Digest()
				if err != nil {
					t.Fatalf("profile digest: %v", err)
				}
				auth.ProfileConfigDigest = digest
				return auth, nil
			},
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
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

func TestSelectAndVerifyRechecksTenantHints(t *testing.T) {
	trust, evidence := selectionFixture(t)
	target := &stubTarget{pt: evidence.ProvenanceType, hints: TentativeHints{
		Scheme: trust.AuthorityRegistry[0].PrincipalAuthority.Scheme, Authority: trust.AuthorityRegistry[0].PrincipalAuthority.Authority,
		TenantPartition: "hint-other", Subject: "alice", Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
	}, verify: func(VerifyRequest) (AuthenticatedEvidence, error) { return successfulEvidence(t, trust, evidence), nil }}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) { return target, true }, defaultServices())
	if !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("authenticated tenant differs from hint: %v", err)
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
	trust, evidence := selectionFixture(t)
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			hints: TentativeHints{
				Scheme:    IdentitySchemeOIDCSubV1,
				Authority: "https://issuer.example.test",
				Subject:   "mallory",
				Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
			},
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence), nil
			},
		}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
	if !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("error = %v, want ErrPolicyReevaluation", err)
	}
}

func TestSelectAndVerifyRejectsOccurrenceIdentityMismatch(t *testing.T) {
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	log := &fakeLogVerifier{
		evidence: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{Log: log})
	if !errors.Is(err, ErrInvalidLogInclusion) || !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want identity mismatch to fail the candidate", err)
	}
}

func TestSelectAndVerifyProjectsOnlyLogPosition(t *testing.T) {
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	item := itemFor(evidence)
	got, err := SelectAndVerify(context.Background(), item, trust, lookup, defaultServices())
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
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	log := &fakeLogVerifier{domain: "other-log/v1"}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{Log: log})
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("error = %v, want ErrTemporalValidity for a non-tenant occurrence domain", err)
	}
}

func TestSelectAndVerifyRejectsMissingInclusion(t *testing.T) {
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	item := Item{SignedStatement: SignedStatement{Evidence: evidence}}
	_, err := SelectAndVerify(context.Background(), item, trust, lookup, defaultServices())
	if !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
	}
}

func TestSelectAndVerifyOccurrenceFailureTriesNextProfile(t *testing.T) {
	trust, evidence := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []Digest{profileDigest(first), profileDigest(second)}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}

	log := &fakeLogVerifier{failLeft: 1}
	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(req VerifyRequest) (AuthenticatedEvidence, error) {
				auth := successfulEvidence(t, trust, evidence)
				digest, err := req.ProfileConfig.Digest()
				if err != nil {
					t.Fatalf("profile digest: %v", err)
				}
				auth.ProfileConfigDigest = digest
				return auth, nil
			},
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{Log: log})
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
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
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
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	item := itemFor(evidence)
	got, err := SelectAndVerify(context.Background(), item, trust, lookup, defaultServices())
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
	trust, evidence := selectionFixture(t)
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
				return successfulEvidence(t, trust, evidence), nil
			},
		}, true
	}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices())
	if err == nil {
		t.Fatal("SelectAndVerify succeeded with timestamp bindings and a nil Time verifier")
	}
	if !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("error = %v, want ErrNoSuccessfulProfile", err)
	}
}

func TestSelectAndVerifyFakeTimestampPathClonesAndProjects(t *testing.T) {
	trust, evidence := selectionFixture(t)
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
				auth := successfulEvidence(t, trust, evidence)
				return ProvenanceAuthenticationResult{
					Authenticated: auth,
					Assertion:     TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)},
				}, nil
			},
		}, true
	}
	fakeTime := &fakeTimeVerifier{mutate: true, earliest: t0, latest: t0.Add(time.Second)}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{
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

func TestSelectAndVerifyRejectsDuplicateTimestampBindingsBeforeAdapter(t *testing.T) {
	trust, evidence := selectionFixture(t)
	binding := UnverifiedTimestampBinding{
		Format:  TimestampFormatRFC3161V1,
		Token:   []byte("token"),
		Message: []byte("message"),
	}
	target := &stubTarget{
		pt: ProvenanceTypeDirectKeyV1,
		prepare: func() (TemporalPreparation, error) {
			return TemporalPreparation{Timestamps: []UnverifiedTimestampBinding{binding, binding}}, nil
		},
		verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
			return successfulEvidence(t, trust, evidence), nil
		},
	}
	timeAdapter := &fakeTimeVerifier{}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) {
		return target, true
	}, TemporalVerificationServices{Log: &fakeLogVerifier{}, Time: timeAdapter})
	if err == nil {
		t.Fatal("SelectAndVerify succeeded with duplicate timestamp bindings")
	}
	if timeAdapter.calls != 0 {
		t.Fatalf("VerifyTimestamp calls = %d, want 0 for duplicate bindings", timeAdapter.calls)
	}
}

func TestSelectAndVerifyProjectsCoordinatorObservationsNotFinishMutations(t *testing.T) {
	trust, evidence := selectionFixture(t)
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
				auth := successfulEvidence(t, trust, evidence)
				return ProvenanceAuthenticationResult{
					Authenticated: auth,
					Assertion:     TypedAssertion{PredicateType: auth.PredicateType, Bytes: []byte(`{"ok":true}`)},
				}, nil
			},
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{
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
	trust, evidence := selectionFixture(t)
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
				return successfulEvidence(t, trust, evidence), nil
			},
		}, true
	}
	fakeTime := &fakeTimeVerifier{}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{
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
	trust, evidence := selectionFixture(t)
	first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":1}`)}
	second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte(`{"n":2}`)}
	trust.AuthorityRegistry[0].DeliveryPolicies[0].Profiles = []Digest{profileDigest(first), profileDigest(second)}
	trust.AuthorityRegistry[0].ProvenanceProfiles = []ProfileConfig{first, second}
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	lookup := func(pt ProvenanceType) (TargetAPI, bool) {
		return &failThenSucceedTarget{
			t:        t,
			pt:       pt,
			trust:    trust,
			evidence: evidence,
		}, true
	}
	got, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{
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
	trust, evidence := selectionFixture(t)
	lookup := successLookup(t, trust, evidence)
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{})
	if !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
	}
}

func TestSelectAndVerifyPreparedTimestampLimitsAreFatalBeforeOccurrenceOrAdapter(t *testing.T) {
	tests := []struct {
		name     string
		bindings []UnverifiedTimestampBinding
	}{
		{
			name: "too many bindings",
			bindings: func() []UnverifiedTimestampBinding {
				bindings := make([]UnverifiedTimestampBinding, maxPreparedTimestampBindings+1)
				return bindings
			}(),
		},
		{
			name: "aggregate bytes",
			bindings: func() []UnverifiedTimestampBinding {
				bindings := make([]UnverifiedTimestampBinding, 9)
				for i := range bindings {
					bindings[i] = UnverifiedTimestampBinding{
						Format:  TimestampFormatRFC3161V1,
						Message: bytes.Repeat([]byte("x"), MaxTimestampMessageBytes),
					}
				}
				return bindings
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trust, evidence := selectionFixture(t)
			first := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte("first")}
			second := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1, Parameters: []byte("second")}
			authority := &trust.AuthorityRegistry[0]
			authority.ProvenanceProfiles = []ProfileConfig{first, second}
			authority.DeliveryPolicies[0].Profiles = []Digest{profileDigest(first), profileDigest(second)}

			target := &stubTarget{
				pt: ProvenanceTypeDirectKeyV1,
				prepare: func() (TemporalPreparation, error) {
					return TemporalPreparation{Timestamps: test.bindings}, nil
				},
			}
			lookup := func(ProvenanceType) (TargetAPI, bool) { return target, true }
			log := &fakeLogVerifier{}
			timeAdapter := &fakeTimeVerifier{}

			_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, TemporalVerificationServices{
				Log:  log,
				Time: timeAdapter,
			})
			if !errors.Is(err, errPreparedTimestampLimit) {
				t.Fatalf("error = %v, want fatal prepared-timestamp limit", err)
			}
			if target.beginCalls != 1 {
				t.Fatalf("BeginVerification calls = %d, want one attempt with no profile fallback", target.beginCalls)
			}
			if log.calls != 0 {
				t.Fatalf("occurrence verification calls = %d, want 0", log.calls)
			}
			if timeAdapter.calls != 0 {
				t.Fatalf("trusted-time calls = %d, want 0", timeAdapter.calls)
			}
		})
	}
}

func TestSelectAndVerifyAcceptsPreparedTimestampAggregateByteLimitExactly(t *testing.T) {
	trust, evidence := selectionFixture(t)
	bindings := make([]UnverifiedTimestampBinding, 8)
	for i := range bindings {
		message := bytes.Repeat([]byte("x"), MaxTimestampMessageBytes)
		message[0] = byte(i)
		bindings[i] = UnverifiedTimestampBinding{Format: TimestampFormatRFC3161V1, Message: message}
	}
	target := &stubTarget{
		pt: ProvenanceTypeDirectKeyV1,
		prepare: func() (TemporalPreparation, error) {
			return TemporalPreparation{Timestamps: bindings}, nil
		},
		verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
			return successfulEvidence(t, trust, evidence), nil
		},
	}
	timeAdapter := &fakeTimeVerifier{}
	if _, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) {
		return target, true
	}, TemporalVerificationServices{Log: &fakeLogVerifier{}, Time: timeAdapter}); err != nil {
		t.Fatalf("SelectAndVerify at exact aggregate-byte limit: %v", err)
	}
	if timeAdapter.calls != len(bindings) {
		t.Fatalf("trusted-time calls = %d, want %d at exact limit", timeAdapter.calls, len(bindings))
	}
}

type stubTarget struct {
	pt          ProvenanceType
	requiresLog bool
	hints       TentativeHints
	parse       func(TypedEvidence) (TentativeHints, error)
	begin       func(VerifyRequest)
	verify      func(VerifyRequest) (AuthenticatedEvidence, error)
	prepare     func() (TemporalPreparation, error)
	finish      func(VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error)
	owns        map[PredicateType]bool
	beginCalls  int
}

func (s *stubTarget) ProvenanceType() ProvenanceType { return s.pt }

func (s *stubTarget) RequiresEvidenceLog() bool { return s.requiresLog }

func (s *stubTarget) ParseHints(evidence TypedEvidence) (TentativeHints, error) {
	if s.parse != nil {
		return s.parse(evidence)
	}
	if s.hints.Scheme != "" {
		return s.hints, nil
	}
	return TentativeHints{
		Scheme:    IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   "alice",
		Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
	}, nil
}

func (s *stubTarget) BeginVerification(_ context.Context, req VerifyRequest) (ProvenanceVerificationSession, error) {
	s.beginCalls++
	if s.begin != nil {
		s.begin(req)
	}
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
	attempts int
}

func (s *failThenSucceedTarget) ProvenanceType() ProvenanceType { return s.pt }
func (s *failThenSucceedTarget) RequiresEvidenceLog() bool      { return false }
func (s *failThenSucceedTarget) ParseHints(TypedEvidence) (TentativeHints, error) {
	return TentativeHints{
		Scheme:    IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   "alice",
		Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
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
	auth := successfulEvidence(s.parent.t, s.parent.trust, s.parent.evidence)
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

func successLookup(t *testing.T, trust TrustConfiguration, evidence TypedEvidence) TargetLookup {
	t.Helper()
	return func(pt ProvenanceType) (TargetAPI, bool) {
		return &stubTarget{
			pt: pt,
			verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence), nil
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

func selectionFixture(t *testing.T) (TrustConfiguration, TypedEvidence) {
	t.Helper()
	profile := ProfileConfig{ProvenanceType: ProvenanceTypeDirectKeyV1}
	authority := AuthorityConfig{
		PrincipalAuthority: PrincipalAuthority{
			Scheme:    IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
		},

		ProvenanceProfiles: []ProfileConfig{profile},
		DeliveryPolicies: []DeliveryPolicy{{
			Match: PolicyMatch{
				PredicateType: PredicateTypeDeploymentV1,
			},
			LiveCredential:     RequirementNone,
			Provenance:         RequirementRequired,
			RequireEvidenceLog: true,
			Profiles:           []Digest{profileDigest(profile)},
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
	return trust, evidence
}

func successfulEvidence(t *testing.T, trust TrustConfiguration, evidence TypedEvidence) AuthenticatedEvidence {
	t.Helper()
	authority := trust.AuthorityRegistry[0]
	authorityDigest, err := authority.Digest()
	if err != nil {
		t.Fatalf("authority digest: %v", err)
	}
	var profileDigest Digest
	foundProfile := false
	for _, reference := range authority.DeliveryPolicies[0].Profiles {
		profile, err := authority.Profile(reference)
		if err != nil {
			t.Fatal(err)
		}
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
	assertion := TypedAssertion{PredicateType: PredicateTypeDeploymentV1, Bytes: []byte(`{"ok":true}`)}
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

		PredicateType:         PredicateTypeDeploymentV1,
		ContentDigest:         contentDigest,
		ProvenanceType:        evidence.ProvenanceType,
		AuthorityConfigDigest: authorityDigest,
		ProfileConfigDigest:   profileDigest,
	}
}

func TestSelectAndVerifyLeavesMissingProfileTimeRequirementToFinish(t *testing.T) {
	trust, evidence := selectionFixture(t)
	prepareCalls := 0
	finishCalls := 0
	var finishObservations []VerifiedTimeObservation
	target := &stubTarget{
		pt: ProvenanceTypeDirectKeyV1,
		prepare: func() (TemporalPreparation, error) {
			prepareCalls++
			// This profile is time-dependent, but its current authenticated
			// history has no timestamp binding to offer to common code.
			return TemporalPreparation{}, nil
		},
		finish: func(inputs VerifiedProvenanceTemporalInputs) (ProvenanceAuthenticationResult, error) {
			finishCalls++
			finishObservations = append([]VerifiedTimeObservation(nil), inputs.Timestamps...)
			if len(inputs.Timestamps) != 0 {
				t.Errorf("Finish observations = %v, want zero when Prepare returned no bindings", inputs.Timestamps)
			}
			// Finish owns the rule that this statement needs an accepted time
			// boundary. Common selection must not infer or reconstruct one.
			return ProvenanceAuthenticationResult{}, ErrVerificationFailed
		},
	}
	timeVerifier := &fakeTimeVerifier{}
	_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) {
		return target, true
	}, TemporalVerificationServices{Log: &fakeLogVerifier{}, Time: timeVerifier})
	if !errors.Is(err, ErrNoSuccessfulProfile) || !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("SelectAndVerify error = %v, want Finish's missing-history rejection", err)
	}
	if prepareCalls != 1 || finishCalls != 1 {
		t.Fatalf("Prepare calls = %d, Finish calls = %d, want one each", prepareCalls, finishCalls)
	}
	if len(finishObservations) != 0 {
		t.Fatalf("Finish received %d observations, want zero", len(finishObservations))
	}
	if timeVerifier.calls != 0 {
		t.Fatalf("VerifyTimestamp calls = %d, want zero without a prepared binding", timeVerifier.calls)
	}
}

func TestSelectAndVerifyOptionalLogVerifiesSuppliedProofAndProjectsPosition(t *testing.T) {
	trust, evidence := selectionFixture(t)
	checkpoint, inclusion := selectionProofFor(t, evidence)
	item := Item{SignedStatement: SignedStatement{Evidence: evidence}, EvidenceLog: &inclusion}
	logVerifier := &proofCheckingSelectionLog{checkpoint: checkpoint}

	trust.AuthorityRegistry[0].DeliveryPolicies[0].RequireEvidenceLog = false
	got, err := SelectAndVerify(context.Background(), item, trust, successLookup(t, trust, evidence), TemporalVerificationServices{Log: logVerifier})
	if err != nil {
		t.Fatalf("optional-log selection with a valid supplied proof: %v", err)
	}
	wantIdentity, err := evidence.Identity()
	if err != nil {
		t.Fatalf("evidence identity: %v", err)
	}
	if logVerifier.seen != wantIdentity {
		t.Fatalf("log verifier saw evidence identity %q, want exact subject identity %q", logVerifier.seen, wantIdentity)
	}
	if got.Temporal.LogPosition == nil {
		t.Fatal("optional supplied binding did not project a LogPosition")
	}
	if want := (LogPosition{Domain: LogDomainTenantEvidenceV1, Index: inclusion.Index}); *got.Temporal.LogPosition != want {
		t.Fatalf("LogPosition = %+v, want verified supplied position %+v", got.Temporal.LogPosition, want)
	}
}

func TestSelectAndVerifyOptionalLogAllowsNoBindingWithoutAdapter(t *testing.T) {
	trust, evidence := selectionFixture(t)
	item := Item{SignedStatement: SignedStatement{Evidence: evidence}}

	trust.AuthorityRegistry[0].DeliveryPolicies[0].RequireEvidenceLog = false
	got, err := SelectAndVerify(context.Background(), item, trust, successLookup(t, trust, evidence), TemporalVerificationServices{})
	if err != nil {
		t.Fatalf("optional-log selection without an inclusion or log adapter: %v", err)
	}
	if got.Temporal.LogPosition != nil {
		t.Fatalf("LogPosition = %+v, want nil when an optional binding is absent", got.Temporal.LogPosition)
	}
}

func TestSelectAndVerifyOptionalLogStillRejectsMalformedSuppliedProof(t *testing.T) {
	trust, evidence := selectionFixture(t)
	checkpoint, inclusion := selectionProofFor(t, evidence)
	inclusion.InclusionProof = append([]Digest(nil), inclusion.InclusionProof...)
	inclusion.InclusionProof[0] = DigestBytes([]byte("wrong inclusion path"))
	item := Item{SignedStatement: SignedStatement{Evidence: evidence}, EvidenceLog: &inclusion}
	logVerifier := &proofCheckingSelectionLog{checkpoint: checkpoint}

	trust.AuthorityRegistry[0].DeliveryPolicies[0].RequireEvidenceLog = false
	_, err := SelectAndVerify(context.Background(), item, trust, successLookup(t, trust, evidence), TemporalVerificationServices{Log: logVerifier})
	if !errors.Is(err, ErrInvalidLogInclusion) || !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("optional malformed-proof error = %v, want inclusion rejection", err)
	}
	if logVerifier.calls != 1 {
		t.Fatalf("VerifyOccurrence calls = %d, want supplied malformed proof to be checked", logVerifier.calls)
	}
}

type proofCheckingSelectionLog struct {
	checkpoint Checkpoint
	seen       Digest
	calls      int
}

func (v *proofCheckingSelectionLog) VerifyOccurrence(_ context.Context, evidence TypedEvidence, inclusion EvidenceLogInclusion) (VerifiedEvidenceLogBinding, error) {
	v.calls++
	if err := VerifyEvidenceLogInclusion(v.checkpoint, evidence, inclusion); err != nil {
		return VerifiedEvidenceLogBinding{}, err
	}
	identity, err := evidence.Identity()
	if err != nil {
		return VerifiedEvidenceLogBinding{}, err
	}
	v.seen = identity
	return VerifiedEvidenceLogBinding{
		Position: LogPosition{Domain: LogDomainTenantEvidenceV1, Index: inclusion.Index},
		Evidence: identity,
	}, nil
}

func selectionProofFor(t *testing.T, evidence TypedEvidence) (Checkpoint, EvidenceLogInclusion) {
	t.Helper()
	tree := merklelog.New()
	preceding := testLogEvidence("selection-prefix")
	first, _ := mustAppendLog(t, tree, EmptyCheckpoint(), preceding)
	update, _ := mustAppendLog(t, tree, first.Checkpoint, evidence)
	return update.Checkpoint, mustEvidenceLogInclusion(t, tree, 1)
}

func profileDigest(profile ProfileConfig) Digest {
	reference, err := profile.Digest()
	if err != nil {
		panic(err)
	}
	return reference
}
