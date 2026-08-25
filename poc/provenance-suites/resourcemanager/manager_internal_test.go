package resourcemanager

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/producer"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

func TestConflictingEnrollmentPrepareAndCommitShareTheAcceptLock(t *testing.T) {
	manager := New(protocol.TenantID("tenant-acme"), nil)
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
	manager := New(protocol.TenantID("tenant-acme"), nil)
	user, err := producer.New(producer.Config{
		TenantID: "tenant-acme",
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
			TenantID:         "tenant-acme",
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
		TenantID: "tenant-acme",
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
