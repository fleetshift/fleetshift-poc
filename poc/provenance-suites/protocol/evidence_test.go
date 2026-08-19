package protocol

import (
	"bytes"
	"testing"
)

func TestTypedEvidenceIdentityBindsTypeMediaAndBytes(t *testing.T) {
	base := TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		MediaType:      "application/test+json",
		Bytes:          []byte(`{"sig":"aaaa"}`),
	}
	identity, err := base.Identity()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	if identity == "" {
		t.Fatal("identity is empty")
	}

	same, err := TypedEvidence{
		ProvenanceType: base.ProvenanceType,
		MediaType:      base.MediaType,
		Bytes:          append([]byte(nil), base.Bytes...),
	}.Identity()
	if err != nil {
		t.Fatalf("identity of copy: %v", err)
	}
	if same != identity {
		t.Fatalf("identical evidence identity = %q, want %q", same, identity)
	}

	for _, tc := range []struct {
		name     string
		evidence TypedEvidence
	}{
		{
			name: "provenance type",
			evidence: TypedEvidence{
				ProvenanceType: "other/v1",
				MediaType:      base.MediaType,
				Bytes:          base.Bytes,
			},
		},
		{
			name: "media type",
			evidence: TypedEvidence{
				ProvenanceType: base.ProvenanceType,
				MediaType:      "application/other+json",
				Bytes:          base.Bytes,
			},
		},
		{
			name: "bytes",
			evidence: TypedEvidence{
				ProvenanceType: base.ProvenanceType,
				MediaType:      base.MediaType,
				Bytes:          []byte(`{"sig":"bbbb"}`),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.evidence.Identity()
			if err != nil {
				t.Fatalf("identity: %v", err)
			}
			if got == identity {
				t.Fatalf("changing %s left identity %q unchanged", tc.name, got)
			}
		})
	}
}

func TestInnerContentDigestIsNotOuterEvidenceIdentity(t *testing.T) {
	assertion := TypedAssertion{
		ContentType: ContentTypeDeliveryAuthorizationV1,
		Bytes:       []byte(`{"target":"east"}`),
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		t.Fatalf("content digest: %v", err)
	}
	evidence := TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		MediaType:      "application/test+json",
		Bytes:          assertion.Bytes,
	}
	evidenceID, err := evidence.Identity()
	if err != nil {
		t.Fatalf("evidence identity: %v", err)
	}
	if contentDigest == evidenceID {
		t.Fatal("inner content digest collapsed into outer evidence identity")
	}
}

func TestPrincipalEqualityRequiresCompleteTuple(t *testing.T) {
	alice := Principal{
		Scheme:    IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   "alice",
	}
	if !alice.Equal(alice) {
		t.Fatal("principal should equal itself")
	}
	otherIssuer := alice
	otherIssuer.Authority = "https://other.example.test"
	if alice.Equal(otherIssuer) {
		t.Fatal("equal subjects under different authorities merged")
	}
	otherPartition := alice
	otherPartition.TenantPartition = "acme"
	if alice.Equal(otherPartition) {
		t.Fatal("identical subjects in different tenant partitions merged")
	}
}

func TestCanonicalJSONIsDeterministicForFixedStructs(t *testing.T) {
	value := DeliveryAuthorization{
		TenantID:      "tenant-acme",
		TargetID:      "target-east",
		FulfillmentID: "fulfillment-1",
		Generation:    1,
		Action:        ActionPut,
		Payload:       []byte(`{"replicas":3}`),
	}
	first, err := MarshalCanonical(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	second, err := MarshalCanonical(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("canonical JSON was not deterministic:\n%s\n%s", first, second)
	}
}
