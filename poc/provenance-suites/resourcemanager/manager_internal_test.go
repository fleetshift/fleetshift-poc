package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/producer"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestConflictingEnrollmentPrepareAndCommitShareTheAcceptLock(t *testing.T) {
	manager, err := New(Config{TenantID: "tenant-acme", Tenant: managerTestTenant(), Trust: managerTestTrust()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := mustAliceProducer(t)
	second := mustAliceProducer(t)
	firstEnrollment, err := first.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("first enrollment: %v", err)
	}
	secondEnrollment, err := second.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("second enrollment: %v", err)
	}

	manager.mu.Lock()
	if _, err := manager.commitEnrollmentLocked(firstEnrollment); err != nil {
		manager.mu.Unlock()
		t.Fatalf("commit first: %v", err)
	}
	_, secondErr := manager.commitEnrollmentLocked(secondEnrollment)
	manager.mu.Unlock()
	if secondErr == nil {
		t.Fatal("conflicting enrollment committed under the same accept lock")
	}
	if got := manager.EvidenceLogSize(); got != 1 {
		t.Fatalf("evidence-log size = %d, want 1", got)
	}
	got, ok := manager.profile.PublicKey(first.Principal())
	if !ok || string(got) != string(first.PublicKey()) {
		t.Fatal("courier map does not hold the first enrolled key")
	}
}

func TestIdentityCollisionInRepositoryIsFatal(t *testing.T) {
	manager, err := New(Config{TenantID: "tenant-acme", Tenant: managerTestTenant(), Trust: managerTestTrust()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	user, err := producer.New(producer.Config{
		Principal: protocol.Principal{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
			Subject:   "alice",
		},
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	enrollment, err := user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatalf("enrollment: %v", err)
	}
	if _, err := manager.AcceptDirectKeyEnrollment(context.Background(), user.Principal(), enrollment); err != nil {
		t.Fatalf("accept enrollment: %v", err)
	}
	before := manager.EvidenceLogSize()

	evidence, err := user.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
		DeliveryScope: protocol.DeliveryScope{
			Tenant:           managerTestTenant(),
			TargetID:         "target-east",
			FullResourceName: "//fleetshift.io/deployments/collision",
			Generation:       1,
			Action:           protocol.ActionPut,
		},
		Manifests: []protocol.TypedManifest{{
			MediaType: "application/vnd.example.replicas+json",
			Bytes:     []byte(`{}`),
		}},
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	identity, err := evidence.Identity()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	tampered := cloneEvidence(evidence)
	tampered.Bytes = []byte("different-envelope")
	manager.mu.Lock()
	manager.evidenceByID[identity] = tampered
	manager.logIndexByEvidenceID[identity] = 0
	manager.mu.Unlock()

	_, err = manager.Compromised().PushDelivery(context.Background(), evidence)
	if !errors.Is(err, ErrEvidenceCollision) {
		t.Fatalf("error = %v, want ErrEvidenceCollision", err)
	}
	if manager.EvidenceLogSize() != before {
		t.Fatal("collision appended a new evidence-log leaf")
	}
}

func mustAliceProducer(t *testing.T) *producer.Producer {
	t.Helper()
	user, err := producer.New(producer.Config{
		Principal: protocol.Principal{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
			Subject:   "alice",
		},
	})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	return user
}

func managerTestTrust() protocol.TrustConfiguration {
	profile := protocol.ProfileConfig{ProvenanceType: protocol.ProvenanceTypeDirectKeyV1}
	reference, err := profile.Digest()
	if err != nil {
		panic(err)
	}
	return protocol.TrustConfiguration{AuthorityRegistry: []protocol.AuthorityConfig{{
		PrincipalAuthority: protocol.PrincipalAuthority{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example.test"},
		ProvenanceProfiles: []protocol.ProfileConfig{profile}, DeliveryPolicies: []protocol.DeliveryPolicy{
			{Match: protocol.PolicyMatch{PredicateType: directkey.PredicateTypeEnrollmentV1}, Provenance: protocol.RequirementRequired, Profiles: []protocol.Digest{reference}, RequireEvidenceLog: true},
			{Match: protocol.PolicyMatch{PredicateType: protocol.PredicateTypeDeploymentV1}, Provenance: protocol.RequirementRequired, Profiles: []protocol.Digest{reference}, RequireEvidenceLog: true},
		},
	}}}
}

func TestCallerBindingIncludesExternalTenantPartition(t *testing.T) {
	for _, action := range []string{ActionEnroll, ActionDeliver} {
		t.Run(action, func(t *testing.T) {
			tenant := managerTestTenant()
			tenant.Partition = "external-acme"
			manager, err := New(Config{TenantID: "tenant-acme", Tenant: tenant, Trust: managerTestTrust()}, nil)
			if err != nil {
				t.Fatal(err)
			}
			user, err := producer.New(producer.Config{Principal: protocol.Principal{
				Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example.test", TenantPartition: "external-acme", Subject: "alice",
			}})
			if err != nil {
				t.Fatal(err)
			}
			caller := user.Principal()
			caller.TenantPartition = "external-other"
			enrollment, err := user.DirectKey().CreateEnrollment()
			if err != nil {
				t.Fatal(err)
			}
			if action == ActionEnroll {
				if _, err := manager.AcceptDirectKeyEnrollment(context.Background(), caller, enrollment); !errors.Is(err, ErrUnauthorized) {
					t.Fatalf("cross-tenant enrollment error = %v", err)
				}
			} else {
				if _, err := manager.AcceptDirectKeyEnrollment(context.Background(), user.Principal(), enrollment); err != nil {
					t.Fatal(err)
				}
				evidence, err := user.SignDeployment(context.Background(), protocol.DeploymentAuthorization{
					DeliveryScope: protocol.DeliveryScope{TargetID: "target-east", FullResourceName: "//fleetshift.io/deployments/partition", Generation: 1, Action: protocol.ActionPut},
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := manager.AcceptDelivery(context.Background(), caller, evidence); !errors.Is(err, ErrUnauthorized) {
					t.Fatalf("cross-tenant delivery error = %v", err)
				}
			}
		})
	}
}

func TestRegistrationCombinesPolicyAndMechanismRequirements(t *testing.T) {
	trust := managerTestTrust()
	for i := range trust.AuthorityRegistry[0].DeliveryPolicies {
		trust.AuthorityRegistry[0].DeliveryPolicies[i].RequireEvidenceLog = false
	}
	manager, err := New(Config{TenantID: "tenant-acme", Tenant: managerTestTenant(), Trust: trust}, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager.lookup = func(pt protocol.ProvenanceType) (protocol.ResourceManagerAPI, bool) {
		return logRequiringCourier{manager.profile}, pt == manager.profile.ProvenanceType()
	}
	user := mustAliceProducer(t)
	enrollment, err := user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcceptDirectKeyEnrollment(context.Background(), user.Principal(), enrollment); err != nil {
		t.Fatal(err)
	}
	if manager.EvidenceLogSize() != 1 {
		t.Fatal("mechanism requirement was disabled by optional policy")
	}
}

type logRequiringCourier struct{ protocol.ResourceManagerAPI }

func (logRequiringCourier) RequiresEvidenceLog() bool { return true }

func managerTestTenant() protocol.Tenant {
	return protocol.Tenant{PrincipalAuthority: managerTestTrust().AuthorityRegistry[0].PrincipalAuthority}
}

func TestRoutingTenantIDStaysInsideManager(t *testing.T) {
	const internalID = "private-routing-id"
	var requests []AuthorizationRequest
	manager, err := New(Config{TenantID: internalID, Tenant: managerTestTenant(), Trust: managerTestTrust()}, func(request AuthorizationRequest) error {
		requests = append(requests, request)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	user := mustAliceProducer(t)
	enrollment, err := user.DirectKey().CreateEnrollment()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcceptDirectKeyEnrollment(context.Background(), user.Principal(), enrollment); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []protocol.Tenant{
		user.Principal().Tenant(),
		{PrincipalAuthority: protocol.PrincipalAuthority{Scheme: user.Principal().Scheme, Authority: "https://other.example.test"}},
		{PrincipalAuthority: user.Principal().PrincipalAuthority(), Partition: "other"},
	} {
		// Sign directly through the profile to exercise the manager's own scope
		// guard, independent of the producer's ordinary scope binding.
		assertion, err := (protocol.DeploymentAuthorization{DeliveryScope: protocol.DeliveryScope{
			Tenant: tenant, TargetID: "target-east", FullResourceName: "//fleetshift.io/deployments/external", Generation: 1, Action: protocol.ActionPut,
		}}).Assertion()
		if err != nil {
			t.Fatal(err)
		}
		evidence, err := user.DirectKey().CreateEvidence(context.Background(), assertion)
		if err != nil {
			t.Fatal(err)
		}
		_, err = manager.AcceptDelivery(context.Background(), user.Principal(), evidence)
		if tenant == user.Principal().Tenant() {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("other external tenant error = %v", err)
		}
	}
	if len(requests) != 2 {
		t.Fatalf("authorization requests = %d, want 2", len(requests))
	}
	for _, request := range requests {
		if request.TenantID != internalID {
			t.Fatalf("authorization tenant = %q", request.TenantID)
		}
	}
}
