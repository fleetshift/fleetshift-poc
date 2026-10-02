package protocol

import (
	"context"
	"errors"
	"testing"
)

func TestAuthorityProfileReferencesResolveExactConfiguration(t *testing.T) {
	first := ProfileConfig{ProvenanceType: "test/v1", Parameters: []byte("first anchors")}
	second := ProfileConfig{ProvenanceType: "test/v1", Parameters: []byte("second anchors")}
	firstRef, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	secondRef, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	authority := AuthorityConfig{ProvenanceProfiles: []ProfileConfig{first, second}}
	got, err := authority.Profile(secondRef)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Parameters) != "second anchors" {
		t.Fatalf("resolved parameters = %q", got.Parameters)
	}
	if &got.Parameters[0] != &authority.ProvenanceProfiles[1].Parameters[0] {
		t.Fatal("profile lookup copied immutable configuration")
	}
	if firstRef == secondRef {
		t.Fatal("different anchors have the same reference")
	}
	if _, err := authority.Profile(DigestBytes([]byte("absent"))); !errors.Is(err, ErrInvalidTrustConfiguration) {
		t.Fatalf("missing reference error = %v", err)
	}
	authority.ProvenanceProfiles = append(authority.ProvenanceProfiles, second)
	if _, err := authority.Profile(secondRef); !errors.Is(err, ErrInvalidTrustConfiguration) {
		t.Fatalf("duplicate reference error = %v", err)
	}
}

func TestPolicyReferencesResolveBeforeAnyProfileAttempt(t *testing.T) {
	for _, name := range []string{"missing later reference", "duplicate reference", "duplicate definition"} {
		t.Run(name, func(t *testing.T) {
			trust, evidence := selectionFixture(t)
			authority := &trust.AuthorityRegistry[0]
			policy := &authority.DeliveryPolicies[0]
			switch name {
			case "missing later reference":
				policy.Profiles = append(policy.Profiles, DigestBytes([]byte("missing")))
			case "duplicate reference":
				policy.Profiles = append(policy.Profiles, policy.Profiles[0])
			case "duplicate definition":
				authority.ProvenanceProfiles = append(authority.ProvenanceProfiles, authority.ProvenanceProfiles[0])
			}
			if err := trust.Validate(); !errors.Is(err, ErrInvalidTrustConfiguration) {
				t.Fatalf("configuration error = %v", err)
			}
			target := &stubTarget{pt: evidence.ProvenanceType, verify: func(VerifyRequest) (AuthenticatedEvidence, error) {
				return successfulEvidence(t, trust, evidence), nil
			}}
			_, err := SelectAndVerify(context.Background(), itemFor(evidence), trust, func(ProvenanceType) (TargetAPI, bool) { return target, true }, defaultServices())
			if !errors.Is(err, ErrInvalidTrustConfiguration) {
				t.Fatalf("selection error = %v", err)
			}
			if target.beginCalls != 0 {
				t.Fatalf("profile attempts = %d, want 0", target.beginCalls)
			}
		})
	}
}

func TestTrustValidatesShadowedPolicyReferences(t *testing.T) {
	trust, _ := selectionFixture(t)
	authority := &trust.AuthorityRegistry[0]
	later := authority.DeliveryPolicies[0]
	later.Profiles = []Digest{DigestBytes([]byte("missing"))}
	authority.DeliveryPolicies = append(authority.DeliveryPolicies, later)
	if err := trust.Validate(); !errors.Is(err, ErrInvalidTrustConfiguration) {
		t.Fatalf("shadowed policy reference error = %v, want invalid configuration", err)
	}
}

func TestTrustCloneDetachesExternalConfiguration(t *testing.T) {
	trust, _ := selectionFixture(t)
	authority := &trust.AuthorityRegistry[0]
	partition := TenantPartition("original")
	authority.CredentialMethods = []string{"credential"}
	authority.ProvenanceProfiles[0].Parameters = []byte("anchors")
	authority.DeliveryPolicies[0].Match.TenantPartition = &partition
	authority.DeliveryPolicies[0].Profiles[0] = profileDigest(authority.ProvenanceProfiles[0])
	if err := trust.Validate(); err != nil {
		t.Fatal(err)
	}
	originalReference := authority.DeliveryPolicies[0].Profiles[0]
	copy := trust.Clone()
	copiedAuthority := &copy.AuthorityRegistry[0]
	copiedAuthority.PrincipalAuthority.Authority = "changed"
	copiedAuthority.CredentialMethods[0] = "changed"
	copiedAuthority.ProvenanceProfiles[0].Parameters[0] = 'X'
	*copiedAuthority.DeliveryPolicies[0].Match.TenantPartition = "changed"
	copiedAuthority.DeliveryPolicies[0].Profiles[0] = "changed"
	if authority.PrincipalAuthority.Authority == "changed" || authority.CredentialMethods[0] != "credential" ||
		string(authority.ProvenanceProfiles[0].Parameters) != "anchors" ||
		*authority.DeliveryPolicies[0].Match.TenantPartition != "original" ||
		authority.DeliveryPolicies[0].Profiles[0] != originalReference {
		t.Fatal("editing an independent configuration changed the original")
	}
}

func TestPolicySelectionBorrowsImmutableAuthority(t *testing.T) {
	trust, _ := selectionFixture(t)
	authority := trust.AuthorityRegistry[0]
	selectedAuthority, policy, err := trust.SelectPolicy(TentativeHints{
		Scheme: authority.PrincipalAuthority.Scheme, Authority: authority.PrincipalAuthority.Authority,
		Assertion: TypedAssertion{PredicateType: PredicateTypeDeploymentV1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if &selectedAuthority.DeliveryPolicies[0] != &authority.DeliveryPolicies[0] ||
		&policy.Profiles[0] != &authority.DeliveryPolicies[0].Profiles[0] {
		t.Fatal("policy selection copied immutable source configuration")
	}
}
