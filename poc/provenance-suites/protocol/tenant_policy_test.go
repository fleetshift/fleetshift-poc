package protocol

import (
	"context"
	"errors"
	"testing"
)

func TestTenantPoliciesUseConfiguredOrder(t *testing.T) {
	partition := TenantPartition("external-acme")
	empty := TenantPartition("")
	trust, _ := selectionFixture(t)
	defaultPolicy := trust.AuthorityRegistry[0].DeliveryPolicies[0]
	exact := defaultPolicy
	exact.Match.TenantPartition = &partition
	exact.RequireEvidenceLog = false
	unpartitioned := exact
	unpartitioned.Match.TenantPartition = &empty
	otherPurpose := exact
	otherPurpose.Match.PredicateType = PredicateTypeManagedResourceV1
	for _, tc := range []struct {
		name      string
		policies  []DeliveryPolicy
		partition TenantPartition
		wantLog   bool
		wantErr   error
	}{
		{"default before exact", []DeliveryPolicy{defaultPolicy, exact}, partition, true, nil},
		{"exact before default", []DeliveryPolicy{exact, defaultPolicy}, partition, false, nil},
		{"skip another purpose", []DeliveryPolicy{otherPurpose, defaultPolicy}, partition, true, nil},
		{"skip another tenant", []DeliveryPolicy{exact, defaultPolicy}, "other", true, nil},
		{"unpartitioned exact", []DeliveryPolicy{unpartitioned, defaultPolicy}, "", false, nil},
		{"unpartitioned does not match named tenant", []DeliveryPolicy{unpartitioned, defaultPolicy}, partition, true, nil},
		{"duplicate default", []DeliveryPolicy{defaultPolicy, defaultPolicy}, partition, true, nil},
		{"duplicate exact", []DeliveryPolicy{exact, exact}, partition, false, nil},
		{"no matching tenant", []DeliveryPolicy{exact}, "other", false, ErrNoMatchingPolicy},
		{"no matching purpose", []DeliveryPolicy{otherPurpose}, partition, false, ErrNoMatchingPolicy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := trust.Clone()
			config.AuthorityRegistry[0].DeliveryPolicies = tc.policies
			if err := config.Validate(); err != nil {
				t.Fatal(err)
			}
			_, got, err := config.SelectPolicy(TentativeHints{
				Scheme:    config.AuthorityRegistry[0].PrincipalAuthority.Scheme,
				Authority: config.AuthorityRegistry[0].PrincipalAuthority.Authority,
				Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1}, TenantPartition: tc.partition,
			})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("selection error = %v, want %v", err, tc.wantErr)
			}
			if err == nil && got.RequireEvidenceLog != tc.wantLog {
				t.Fatalf("selected logging requirement = %v, want %v", got.RequireEvidenceLog, tc.wantLog)
			}
		})
	}
}

func TestPolicyOrderIsBoundByAuthorityDigest(t *testing.T) {
	trust, _ := selectionFixture(t)
	authority := trust.AuthorityRegistry[0]
	partition := TenantPartition("external-acme")
	exact := authority.DeliveryPolicies[0]
	exact.Match.TenantPartition = &partition
	authority.DeliveryPolicies = append(authority.DeliveryPolicies, exact)
	before, err := authority.Digest()
	if err != nil {
		t.Fatal(err)
	}
	authority.DeliveryPolicies[0], authority.DeliveryPolicies[1] = authority.DeliveryPolicies[1], authority.DeliveryPolicies[0]
	after, err := authority.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("policy reordering did not change the authority configuration digest")
	}
}

func TestFirstMatchingTenantPolicyDoesNotFallBack(t *testing.T) {
	trust, evidence := selectionFixture(t)
	partition := TenantPartition("external-acme")
	authority := &trust.AuthorityRegistry[0]
	exact := authority.DeliveryPolicies[0]
	exact.Match.TenantPartition = &partition
	exact.RequireEvidenceLog = false
	authority.DeliveryPolicies = []DeliveryPolicy{exact, authority.DeliveryPolicies[0]}
	target := &stubTarget{pt: evidence.ProvenanceType, hints: TentativeHints{
		Scheme: authority.PrincipalAuthority.Scheme, Authority: authority.PrincipalAuthority.Authority,
		TenantPartition: partition, Subject: "alice", Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
	}}
	target.verify = func(req VerifyRequest) (AuthenticatedEvidence, error) {
		if req.DeliveryContext.TenantPartition != partition || req.DeliveryContext.PredicateType != PredicateTypeDeploymentV1 {
			t.Fatalf("profile context = %+v, want evidence-derived tenant and purpose", req.DeliveryContext)
		}
		auth := successfulEvidence(t, trust, evidence)
		auth.Principal.TenantPartition = partition
		return auth, nil
	}
	lookup := func(ProvenanceType) (TargetAPI, bool) { return target, true }
	got, err := SelectAndVerify(context.Background(), Item{SignedStatement: SignedStatement{Evidence: evidence}}, trust, lookup, TemporalVerificationServices{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Policy.Match.TenantPartition == nil || *got.Policy.Match.TenantPartition != partition {
		t.Fatal("exact tenant policy was not selected")
	}
	target.verify = func(VerifyRequest) (AuthenticatedEvidence, error) {
		return AuthenticatedEvidence{}, ErrVerificationFailed
	}
	if _, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, lookup, defaultServices()); !errors.Is(err, ErrNoSuccessfulProfile) {
		t.Fatalf("failed exact policy error = %v", err)
	}
	if target.beginCalls != 2 {
		t.Fatalf("profile attempts = %d, want no fallback to match-all policy", target.beginCalls)
	}
}

func TestTenantHintCannotSelectAnotherTenantsPolicy(t *testing.T) {
	trust, evidence := selectionFixture(t)
	partition := TenantPartition("external-acme")
	authority := &trust.AuthorityRegistry[0]
	exact := authority.DeliveryPolicies[0]
	exact.Match.TenantPartition = &partition
	authority.DeliveryPolicies = []DeliveryPolicy{exact, authority.DeliveryPolicies[0]}
	// Omitting the tenant hint must not permit the match-all policy when
	// authenticated content selects an earlier tenant-specific policy.
	target := &stubTarget{pt: evidence.ProvenanceType, verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
		auth := successfulEvidence(t, trust, evidence)
		auth.Principal.TenantPartition = partition
		return auth, nil
	}}
	if _, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) { return target, true }, defaultServices()); !errors.Is(err, ErrPolicyReevaluation) {
		t.Fatalf("omitted partition hint error = %v", err)
	}
	// With the default first, both hinted and authenticated selection use it.
	authority.DeliveryPolicies[0], authority.DeliveryPolicies[1] = authority.DeliveryPolicies[1], authority.DeliveryPolicies[0]
	if _, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) { return target, true }, defaultServices()); err != nil {
		t.Fatalf("same first policy after authentication: %v", err)
	}
}

func TestFirstMatchingPolicyRequirementsDoNotFallBack(t *testing.T) {
	for _, tc := range []struct {
		name         string
		provenance   Requirement
		wantErr      error
		wantAttempts int
	}{
		{"provenance excluded", RequirementNone, ErrNoMatchingPolicy, 0},
		{"required inclusion missing", RequirementRequired, ErrNoSuccessfulProfile, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trust, evidence := selectionFixture(t)
			authority := &trust.AuthorityRegistry[0]
			later := authority.DeliveryPolicies[0]
			later.RequireEvidenceLog = false
			authority.DeliveryPolicies[0].Provenance = tc.provenance
			authority.DeliveryPolicies = append(authority.DeliveryPolicies, later)
			target := &stubTarget{pt: evidence.ProvenanceType, verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence), nil
			}}
			_, err := SelectAndVerify(context.Background(), Item{SignedStatement: SignedStatement{Evidence: evidence}}, trust,
				func(ProvenanceType) (TargetAPI, bool) { return target, true }, TemporalVerificationServices{})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("selection error = %v, want %v", err, tc.wantErr)
			}
			if target.beginCalls != tc.wantAttempts {
				t.Fatalf("profile attempts = %d, want %d", target.beginCalls, tc.wantAttempts)
			}
		})
	}
}

func TestProfileLogRequirementCannotBeDisabledByPolicy(t *testing.T) {
	trust, evidence := selectionFixture(t)
	trust.AuthorityRegistry[0].DeliveryPolicies[0].RequireEvidenceLog = false
	target := &stubTarget{pt: evidence.ProvenanceType, requiresLog: true}
	_, err := SelectAndVerify(context.Background(), Item{SignedStatement: SignedStatement{Evidence: evidence}}, trust, func(ProvenanceType) (TargetAPI, bool) { return target, true }, TemporalVerificationServices{})
	if !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("missing mandatory profile log error = %v", err)
	}
}
