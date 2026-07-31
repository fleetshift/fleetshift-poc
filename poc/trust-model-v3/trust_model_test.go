package trustmodelv3

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/client"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/deliveryagent"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/testoidc"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/resourcemanager"
)

const (
	testTenant      = "tenant-acme"
	testTarget      = "target-east"
	testOIDCClient  = "fleetshift-enrollment"
	testRedirectURI = "https://client.example.test/oidc/callback"
)

func TestNewEnrollmentAndSignedDelivery(t *testing.T) {
	s := newEnrolledScenario(t)

	signed := mustSignDelivery(t, s.controlledClient, "fulfillment-1", 1, []byte(`{"replicas":3}`))
	record, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), signed)
	if err != nil {
		t.Fatalf("resource manager submit delivery: %v", err)
	}
	if err := s.agent.ReceiveDelivery(record); err != nil {
		t.Fatalf("delivery agent receive delivery: %v", err)
	}

	applied, ok := s.agent.Applied("fulfillment-1")
	if !ok {
		t.Fatal("delivery agent did not retain the applied delivery")
	}
	if got, want := string(applied.Content), `{"replicas":3}`; got != want {
		t.Fatalf("applied content = %s, want %s", got, want)
	}
	if got := s.provider.SuccessfulCodeExchanges(); got != 1 {
		t.Fatalf("OIDC authorization-code exchanges = %d, want 1", got)
	}
}

func TestEnrollmentRejectsResourceManagerKeySubstitution(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)

	attacker, err := client.New(client.Config{
		TenantID:     testTenant,
		Issuer:       s.provider.Issuer(),
		OIDCClientID: testOIDCClient,
		RedirectURI:  testRedirectURI,
		HTTPClient:   s.provider.HTTPClient(),
	})
	if err != nil {
		t.Fatalf("new attacker client: %v", err)
	}
	enrollment.ContinuityPublicKey = attacker.ContinuityPublicKey()

	s.manager.Compromised().AppendEnrollment(enrollment)
	err = s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) {
		t.Fatalf("sync substituted enrollment error = %v, want ErrEnrollment", err)
	}
}

func TestEnrollmentNonceRejectsWholeBindingReplacement(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)

	attackerPublicKey, attackerPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate attacker key: %v", err)
	}
	enrollment.ContinuityPublicKey = attackerPublicKey
	enrollment.Intent.ContinuityKeyDigest = protocol.DigestBytes(attackerPublicKey)
	enrollment.Intent.EnrollmentID = "resource-manager-replacement"
	enrollment.ProofOfPossession, err = protocol.Sign(attackerPrivateKey, "enrollment-proof-of-possession/v1", enrollment.Intent)
	if err != nil {
		t.Fatalf("sign replacement enrollment: %v", err)
	}

	s.manager.Compromised().AppendEnrollment(enrollment)
	err = s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) || !strings.Contains(err.Error(), "nonce") {
		t.Fatalf("replacement enrollment error = %v, want nonce-bound ErrEnrollment", err)
	}
}

func TestEnrollmentReplayIsRejected(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if err := s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint())); err != nil {
		t.Fatalf("sync first enrollment: %v", err)
	}

	s.manager.Compromised().AppendEnrollment(enrollment)
	err := s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) || !strings.Contains(err.Error(), "already enrolled") {
		t.Fatalf("replayed enrollment error = %v, want already-enrolled ErrEnrollment", err)
	}
}

func TestCompromisedResourceManagerCannotForgeOrAlterDelivery(t *testing.T) {
	t.Run("attacker key cannot impersonate enrolled user", func(t *testing.T) {
		s := newEnrolledScenario(t)
		attacker, err := client.New(client.Config{TenantID: testTenant})
		if err != nil {
			t.Fatalf("new attacker client: %v", err)
		}

		forged := mustSignDeliveryAs(t, attacker, s.controlledClient.IdentityID(), s.controlledClient.ContinuityStateDigest(), "forged", 1, []byte(`{"owner":"attacker"}`))
		record := s.manager.Compromised().AppendDelivery(forged)

		err = s.agent.ReceiveDelivery(record)
		if !errors.Is(err, deliveryagent.ErrSignature) {
			t.Fatalf("forged delivery error = %v, want ErrSignature", err)
		}
	})

	t.Run("signed content cannot be altered", func(t *testing.T) {
		s := newEnrolledScenario(t)
		signed := mustSignDelivery(t, s.controlledClient, "altered", 1, []byte(`{"approved":true}`))
		signed.Content = []byte(`{"approved":false}`)
		record := s.manager.Compromised().AppendDelivery(signed)

		err := s.agent.ReceiveDelivery(record)
		if !errors.Is(err, deliveryagent.ErrAttestation) {
			t.Fatalf("altered delivery error = %v, want ErrAttestation", err)
		}
	})

	t.Run("signed delivery metadata cannot be altered", func(t *testing.T) {
		s := newEnrolledScenario(t)
		signed := mustSignDelivery(t, s.controlledClient, "metadata", 1, []byte(`{"approved":true}`))
		signed.Attestation.Generation = 2
		record := s.manager.Compromised().AppendDelivery(signed)

		err := s.agent.ReceiveDelivery(record)
		if !errors.Is(err, deliveryagent.ErrSignature) {
			t.Fatalf("altered delivery metadata error = %v, want ErrSignature", err)
		}
	})
}

func TestAuthenticProvenanceDoesNotReplaceResourceManagerAuthorization(t *testing.T) {
	s := newEnrolledScenarioWithAuthorizer(t, func(req resourcemanager.AuthorizationRequest) error {
		if req.Action == resourcemanager.ActionDeliver {
			return resourcemanager.ErrUnauthorized
		}
		return nil
	})
	signed := mustSignDelivery(t, s.controlledClient, "rbac-denied", 1, []byte(`{"change":"genuine"}`))

	if _, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), signed); !errors.Is(err, resourcemanager.ErrUnauthorized) {
		t.Fatalf("normal submit error = %v, want ErrUnauthorized", err)
	}

	record := s.manager.Compromised().AppendDelivery(signed)
	if err := s.agent.ReceiveDelivery(record); err != nil {
		t.Fatalf("agent rejected authentic provenance after manager authorization bypass: %v", err)
	}
	if _, ok := s.agent.Applied("rbac-denied"); !ok {
		t.Fatal("agent did not apply genuinely signed delivery")
	}
}

func TestContinuityKeyRotation(t *testing.T) {
	s := newEnrolledScenario(t)

	before := mustSignDelivery(t, s.controlledClient, "before-rotation", 1, []byte(`{"version":1}`))
	beforeRecord, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), before)
	if err != nil {
		t.Fatalf("submit pre-rotation delivery: %v", err)
	}
	if err := s.agent.ReceiveDelivery(beforeRecord); err != nil {
		t.Fatalf("receive pre-rotation delivery: %v", err)
	}

	rotation, rotatedClient, err := s.controlledClient.PrepareRotation(s.manager.DeliveryCheckpoint())
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	if _, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation); err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	if err := s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint())); err != nil {
		t.Fatalf("sync rotation: %v", err)
	}

	after := mustSignDelivery(t, rotatedClient, "after-rotation", 1, []byte(`{"version":2}`))
	afterRecord, err := s.manager.SubmitDelivery(rotatedClient.IdentityID(), after)
	if err != nil {
		t.Fatalf("submit post-rotation delivery: %v", err)
	}
	if err := s.agent.ReceiveDelivery(afterRecord); err != nil {
		t.Fatalf("receive post-rotation delivery: %v", err)
	}

	retired := mustSignDelivery(t, s.controlledClient, "retired-key", 1, []byte(`{"version":"old"}`))
	retiredRecord := s.manager.Compromised().AppendDelivery(retired)
	if err := s.agent.ReceiveDelivery(retiredRecord); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("retired-key delivery error = %v, want ErrSigningState", err)
	}
}

func TestRotationRejectsResourceManagerKeyReplacement(t *testing.T) {
	s := newEnrolledScenario(t)
	rotation, _, err := s.controlledClient.PrepareRotation(s.manager.DeliveryCheckpoint())
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}

	attacker, err := client.New(client.Config{TenantID: testTenant})
	if err != nil {
		t.Fatalf("new attacker client: %v", err)
	}
	rotation.NewContinuityPublicKey = attacker.ContinuityPublicKey()
	s.manager.Compromised().AppendRotation(rotation)

	err = s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint()))
	if !errors.Is(err, deliveryagent.ErrRotation) {
		t.Fatalf("sync substituted rotation error = %v, want ErrRotation", err)
	}
}

func TestRotationRequiresNewKeyProofOfPossession(t *testing.T) {
	s := newEnrolledScenario(t)
	rotation, _, err := s.controlledClient.PrepareRotation(s.manager.DeliveryCheckpoint())
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	rotation.ProofByNewKey = make([]byte, ed25519.SignatureSize)
	s.manager.Compromised().AppendRotation(rotation)

	err = s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint()))
	if !errors.Is(err, deliveryagent.ErrRotation) || !strings.Contains(err.Error(), "new key") {
		t.Fatalf("rotation without successor proof error = %v, want new-key ErrRotation", err)
	}
}

func TestCompromisedResourceManagerCannotFabricateRotation(t *testing.T) {
	s := newEnrolledScenario(t)
	attackerPublicKey, attackerPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate attacker key: %v", err)
	}
	intent := protocol.RotationIntent{
		Protocol:               protocol.RotationProtocol,
		TenantID:               testTenant,
		IdentityID:             s.controlledClient.IdentityID(),
		PreviousStateDigest:    s.controlledClient.ContinuityStateDigest(),
		NewGeneration:          1,
		NewContinuityKeyDigest: protocol.DigestBytes(attackerPublicKey),
		DeliveryCutoff:         s.manager.DeliveryCheckpoint(),
	}
	oldSignature, err := protocol.Sign(attackerPrivateKey, "continuity-rotation-old-key/v1", intent)
	if err != nil {
		t.Fatalf("forge old-key authorization: %v", err)
	}
	newProof, err := protocol.Sign(attackerPrivateKey, "continuity-rotation-new-key/v1", intent)
	if err != nil {
		t.Fatalf("sign attacker proof of possession: %v", err)
	}
	s.manager.Compromised().AppendRotation(protocol.RotationPackage{
		Intent:                 intent,
		NewContinuityPublicKey: attackerPublicKey,
		SignatureByOldKey:      oldSignature,
		ProofByNewKey:          newProof,
	})

	err = s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint()))
	if !errors.Is(err, deliveryagent.ErrRotation) || !strings.Contains(err.Error(), "old key") {
		t.Fatalf("fabricated rotation error = %v, want old-key ErrRotation", err)
	}
}

func TestRotationCutoffIsLocalToAgentsThatObservedIt(t *testing.T) {
	s := newEnrolledScenario(t)
	staleAgent := newAgent(t, s)
	if err := staleAgent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(staleAgent.TrustCheckpoint())); err != nil {
		t.Fatalf("bootstrap stale agent: %v", err)
	}

	initial := mustSignDelivery(t, s.controlledClient, "initial", 1, []byte(`{"version":1}`))
	initialRecord, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), initial)
	if err != nil {
		t.Fatalf("submit initial delivery: %v", err)
	}
	if err := s.agent.ReceiveDelivery(initialRecord); err != nil {
		t.Fatalf("current agent receive initial delivery: %v", err)
	}
	if err := staleAgent.ReceiveDelivery(initialRecord); err != nil {
		t.Fatalf("stale agent receive initial delivery: %v", err)
	}

	rotation, _, err := s.controlledClient.PrepareRotation(s.manager.DeliveryCheckpoint())
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	if _, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation); err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	if err := s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint())); err != nil {
		t.Fatalf("current agent sync rotation: %v", err)
	}

	compromisedOldKeyDelivery := mustSignDelivery(t, s.controlledClient, "old-key-after-cutoff", 1, []byte(`{"attack":true}`))
	record := s.manager.Compromised().AppendDelivery(compromisedOldKeyDelivery)

	if err := s.agent.ReceiveDelivery(record); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("rotated agent error = %v, want ErrSigningState", err)
	}
	if err := staleAgent.ReceiveDelivery(record); err != nil {
		t.Fatalf("stale agent should accept on its pre-rotation view: %v", err)
	}
}

func TestEstablishedAgentRejectsDeliveryLogFork(t *testing.T) {
	s := newEnrolledScenario(t)
	oldCheckpoint := s.manager.DeliveryCheckpoint()

	first := mustSignDelivery(t, s.controlledClient, "main-branch", 1, []byte(`{"branch":"main"}`))
	firstRecord, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), first)
	if err != nil {
		t.Fatalf("submit main delivery: %v", err)
	}
	if err := s.agent.ReceiveDelivery(firstRecord); err != nil {
		t.Fatalf("receive main delivery: %v", err)
	}

	forked := mustSignDelivery(t, s.controlledClient, "fork", 1, []byte(`{"branch":"fork"}`))
	forkRecord := s.manager.Compromised().ForgeDeliveryAt(oldCheckpoint, forked)
	if err := s.agent.ReceiveDelivery(forkRecord); !errors.Is(err, deliveryagent.ErrLogFork) {
		t.Fatalf("forked delivery error = %v, want ErrLogFork", err)
	}
}

func TestEstablishedAgentRejectsTrustLogFork(t *testing.T) {
	s := newEnrolledScenario(t)
	attackerClient, err := client.New(client.Config{
		TenantID:     testTenant,
		Issuer:       s.provider.Issuer(),
		OIDCClientID: testOIDCClient,
		RedirectURI:  testRedirectURI,
		HTTPClient:   s.provider.HTTPClient(),
	})
	if err != nil {
		t.Fatalf("new fork client: %v", err)
	}
	forkEnrollment, err := attackerClient.Enroll(context.Background(), "mallory")
	if err != nil {
		t.Fatalf("create fork enrollment: %v", err)
	}
	forkRecord := s.manager.Compromised().ForgeEnrollmentAt(protocol.EmptyCheckpoint(), forkEnrollment)

	if err := s.agent.SyncTrust(context.Background(), []protocol.TrustRecord{forkRecord}); !errors.Is(err, deliveryagent.ErrTrustLogFork) {
		t.Fatalf("forked trust update error = %v, want ErrTrustLogFork", err)
	}
}

type scenario struct {
	provider         *testoidc.Provider
	controlledClient *client.Client
	manager          *resourcemanager.Manager
	agent            *deliveryagent.Agent
}

func newScenario(t *testing.T) scenario {
	t.Helper()
	return newScenarioWithAuthorizer(t, nil)
}

func newScenarioWithAuthorizer(t *testing.T, authorizer resourcemanager.Authorizer) scenario {
	t.Helper()
	provider, err := testoidc.Start(testoidc.Config{
		ClientID:    testOIDCClient,
		RedirectURI: testRedirectURI,
	})
	if err != nil {
		t.Fatalf("start OIDC provider: %v", err)
	}
	t.Cleanup(provider.Close)

	controlledClient, err := client.New(client.Config{
		TenantID:     testTenant,
		Issuer:       provider.Issuer(),
		OIDCClientID: testOIDCClient,
		RedirectURI:  testRedirectURI,
		HTTPClient:   provider.HTTPClient(),
	})
	if err != nil {
		t.Fatalf("new controlled client: %v", err)
	}
	manager := resourcemanager.New(testTenant, authorizer)
	s := scenario{
		provider:         provider,
		controlledClient: controlledClient,
		manager:          manager,
	}
	s.agent = newAgent(t, s)
	return s
}

func newEnrolledScenario(t *testing.T) scenario {
	t.Helper()
	return newEnrolledScenarioWithAuthorizer(t, nil)
}

func newEnrolledScenarioWithAuthorizer(t *testing.T, authorizer resourcemanager.Authorizer) scenario {
	t.Helper()
	s := newScenarioWithAuthorizer(t, authorizer)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if err := s.agent.SyncTrust(context.Background(), s.manager.TrustRecordsAfter(s.agent.TrustCheckpoint())); err != nil {
		t.Fatalf("sync enrollment: %v", err)
	}
	return s
}

func newAgent(t *testing.T, s scenario) *deliveryagent.Agent {
	t.Helper()
	agent, err := deliveryagent.New(deliveryagent.Config{
		TenantID:     testTenant,
		TargetID:     testTarget,
		OIDCIssuer:   s.provider.Issuer(),
		OIDCClientID: testOIDCClient,
		HTTPClient:   s.provider.HTTPClient(),
	})
	if err != nil {
		t.Fatalf("new delivery agent: %v", err)
	}
	return agent
}

func mustEnroll(t *testing.T, controlledClient *client.Client) protocol.EnrollmentPackage {
	t.Helper()
	enrollment, err := controlledClient.Enroll(context.Background(), "alice")
	if err != nil {
		t.Fatalf("enroll client: %v", err)
	}
	return enrollment
}

func mustSignDelivery(t *testing.T, controlledClient *client.Client, fulfillmentID string, generation uint64, content []byte) protocol.SignedDelivery {
	t.Helper()
	signed, err := controlledClient.SignDelivery(client.Delivery{
		TargetID:      testTarget,
		FulfillmentID: fulfillmentID,
		Generation:    generation,
		Action:        protocol.ActionPut,
		Content:       content,
	})
	if err != nil {
		t.Fatalf("sign delivery: %v", err)
	}
	return signed
}

func mustSignDeliveryAs(t *testing.T, signingClient *client.Client, identityID, stateDigest, fulfillmentID string, generation uint64, content []byte) protocol.SignedDelivery {
	t.Helper()
	signed, err := signingClient.SignDeliveryAs(identityID, stateDigest, client.Delivery{
		TargetID:      testTarget,
		FulfillmentID: fulfillmentID,
		Generation:    generation,
		Action:        protocol.ActionPut,
		Content:       content,
	})
	if err != nil {
		t.Fatalf("sign delivery as another identity: %v", err)
	}
	return signed
}
