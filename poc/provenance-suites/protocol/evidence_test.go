package protocol

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestEncodedIsTheSharedTypedBytesForm(t *testing.T) {
	encoded := Encoded{
		MediaType: "application/vnd.example.replicas+json",
		Bytes:     []byte(`{"replicas":3}`),
	}

	manifest := TypedManifest(encoded)
	if manifest.MediaType != encoded.MediaType || string(manifest.Bytes) != string(encoded.Bytes) {
		t.Fatalf("TypedManifest did not share Encoded fields: %+v", manifest)
	}

	support := SupportMaterial(encoded)
	raw, err := json.Marshal(support)
	if err != nil {
		t.Fatalf("marshal support: %v", err)
	}
	if bytes.Contains(raw, []byte("provenance_type")) {
		t.Fatalf("support material carried a provenance type: %s", raw)
	}

	evidence := TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Encoded:        encoded,
	}
	raw, err = json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal evidence: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"media_type"`)) || !bytes.Contains(raw, []byte(`"provenance_type"`)) {
		t.Fatalf("evidence JSON missing flattened fields: %s", raw)
	}
	if bytes.Contains(raw, []byte(`"Encoded"`)) {
		t.Fatalf("Encoded nested instead of embedding: %s", raw)
	}
}

func TestTypedEvidenceIdentityBindsTypeMediaAndBytes(t *testing.T) {
	base := TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Encoded: Encoded{
			MediaType: "application/test+json",
			Bytes:     []byte(`{"sig":"aaaa"}`),
		},
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
		Encoded:        base.Encoded.Clone(),
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
				Encoded:        base.Encoded,
			},
		},
		{
			name: "media type",
			evidence: TypedEvidence{
				ProvenanceType: base.ProvenanceType,
				Encoded: Encoded{
					MediaType: "application/other+json",
					Bytes:     base.Bytes,
				},
			},
		},
		{
			name: "bytes",
			evidence: TypedEvidence{
				ProvenanceType: base.ProvenanceType,
				Encoded: Encoded{
					MediaType: base.MediaType,
					Bytes:     []byte(`{"sig":"bbbb"}`),
				},
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
		PredicateType: PredicateTypeDeploymentV1,
		Bytes:         []byte(`{"target":"east"}`),
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		t.Fatalf("content digest: %v", err)
	}
	evidence := TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Encoded: Encoded{
			MediaType: "application/test+json",
			Bytes:     assertion.Bytes,
		},
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

func TestTypedAssertionDigestBindsPredicateTypeAndBytes(t *testing.T) {
	base := TypedAssertion{
		PredicateType: PredicateTypeDeploymentV1,
		Bytes:         []byte(`{"replicas":3}`),
	}
	digest, err := base.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	changedType := base
	changedType.PredicateType = PredicateTypeManagedResourceV1
	got, err := changedType.Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if got == digest {
		t.Fatal("changing predicate type left content digest unchanged")
	}
}

func TestDeliveryScopeSignsResourceNameAndStaticPlacement(t *testing.T) {
	scope := DeliveryScope{
		TenantID:         "tenant-acme",
		TargetID:         "target-east",
		FullResourceName: "//fleetshift.io/deployments/web",
		Generation:       1,
		Action:           ActionPut,
	}
	raw, err := MarshalCanonical(scope)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"name":"//fleetshift.io/deployments/web"`)) {
		t.Fatalf("canonical JSON missing AIP-122 name: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"target_id":"target-east"`)) {
		t.Fatalf("canonical JSON missing static placement target: %s", raw)
	}
	if bytes.Contains(raw, []byte("fulfillment_id")) {
		t.Fatalf("canonical JSON still carries an RM fulfillment ID: %s", raw)
	}

	got, err := DecodeDeliveryScope(TypedAssertion{Bytes: raw})
	if err != nil {
		t.Fatalf("DecodeDeliveryScope: %v", err)
	}
	if got != scope {
		t.Fatalf("decoded scope = %+v, want %+v", got, scope)
	}
}

func TestCanonicalJSONIsDeterministicForFixedStructs(t *testing.T) {
	value := DeploymentAuthorization{
		DeliveryScope: DeliveryScope{
			TenantID:         "tenant-acme",
			TargetID:         "target-east",
			FullResourceName: "//fleetshift.io/deployments/web",
			Generation:       1,
			Action:           ActionPut,
		},
		Manifests: []TypedManifest{{
			MediaType: "application/vnd.example.replicas+json",
			Bytes:     []byte(`{"replicas":3}`),
		}},
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

func TestAuthorizationAssertionSetsOwnPredicateType(t *testing.T) {
	deployment, err := DeploymentAuthorization{
		DeliveryScope: DeliveryScope{
			TenantID:         "tenant-acme",
			TargetID:         "target-east",
			FullResourceName: "//fleetshift.io/deployments/web",
			Generation:       1,
			Action:           ActionPut,
		},
		Manifests: []TypedManifest{{
			MediaType: "application/vnd.example.replicas+json",
			Bytes:     []byte(`{"replicas":3}`),
		}},
	}.Assertion()
	if err != nil {
		t.Fatalf("deployment assertion: %v", err)
	}
	if deployment.PredicateType != PredicateTypeDeploymentV1 {
		t.Fatalf("deployment predicate = %s", deployment.PredicateType)
	}

	managed, err := ManagedResourceAuthorization{
		DeliveryScope: DeliveryScope{
			TenantID:         "tenant-acme",
			TargetID:         "target-east",
			FullResourceName: "//kind.fleetshift.io/clusters/prod",
			Generation:       1,
			Action:           ActionPut,
		},
		ResourceType: "clusters",
		Spec:         []byte(`{"region":"us-east-1"}`),
	}.Assertion()
	if err != nil {
		t.Fatalf("managed-resource assertion: %v", err)
	}
	if managed.PredicateType != PredicateTypeManagedResourceV1 {
		t.Fatalf("managed-resource predicate = %s", managed.PredicateType)
	}

	relation, err := FulfillmentRelation{
		ResourceType: "clusters",
		MediaType:    "application/vnd.example.cluster-spec+json",
	}.Assertion()
	if err != nil {
		t.Fatalf("relation assertion: %v", err)
	}
	if relation.PredicateType != PredicateTypeFulfillmentRelationV1 {
		t.Fatalf("relation predicate = %s", relation.PredicateType)
	}
}

func TestDecodeRejectsUnexpectedPredicateType(t *testing.T) {
	assertion := TypedAssertion{PredicateType: PredicateTypeManagedResourceV1, Bytes: []byte(`{}`)}
	if _, err := DecodeDeploymentAuthorization(assertion); err == nil {
		t.Fatal("decoded managed-resource assertion as deployment")
	}
}
