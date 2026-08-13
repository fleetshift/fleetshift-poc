package trustmodelv3

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
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

	applied, ok := s.agent.Applied("fulfillment-1")
	if !ok {
		t.Fatal("resource manager did not push the delivery to the agent")
	}
	if got, want := string(applied.Content), `{"replicas":3}`; got != want {
		t.Fatalf("applied content = %s, want %s", got, want)
	}
	managerCheckpoint, ok := s.manager.AgentCheckpoint(testTarget)
	if !ok {
		t.Fatal("resource manager did not retain an agent checkpoint")
	}
	if got, want := managerCheckpoint, s.agent.DeliveryCheckpoint(); got != want {
		t.Fatalf("resource-manager agent checkpoint = %#v, want %#v", got, want)
	}
	if got, want := managerCheckpoint.Size, record.Index+1; got != want {
		t.Fatalf("resource-manager agent checkpoint size = %d, want %d", got, want)
	}
	if got := s.provider.SuccessfulCodeExchanges(); got != 1 {
		t.Fatalf("OIDC authorization-code exchanges = %d, want 1", got)
	}
}

func TestAcceptedRootSupportsRotationAndDeliveryWithoutLiveOIDC(t *testing.T) {
	s := newEnrolledScenario(t)
	s.provider.Close()

	rotation, rotatedClient, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	if _, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation); err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync rotation after OIDC provider became unavailable: %v", err)
	}

	signed := mustSignDelivery(t, rotatedClient, "accepted-root", 1, []byte(`{"source":"retained-root"}`))
	if _, err := s.manager.SubmitDelivery(rotatedClient.IdentityID(), signed); err != nil {
		t.Fatalf("deliver after OIDC provider became unavailable: %v", err)
	}
	if _, ok := s.agent.Applied("accepted-root"); !ok {
		t.Fatal("agent did not apply delivery from previously accepted identity history")
	}
}

func TestManagerRetryRecoversWhenAgentAppliedButAcknowledgementWasLost(t *testing.T) {
	s := newEnrolledScenario(t)

	first := mustSignDelivery(t, s.controlledClient, "ack-anchor", 1, []byte(`{"position":1}`))
	if _, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), first); err != nil {
		t.Fatalf("submit checkpoint anchor: %v", err)
	}
	managerBefore, ok := s.manager.AgentCheckpoint(testTarget)
	if !ok {
		t.Fatal("resource manager did not retain the anchor checkpoint")
	}

	s.agent.LoseNextAcknowledgement()
	second := mustSignDelivery(t, s.controlledClient, "lost-ack", 1, []byte(`{"position":2}`))
	record, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), second)
	if !errors.Is(err, deliveryagent.ErrAcknowledgementLost) {
		t.Fatalf("submit with lost acknowledgement error = %v, want ErrAcknowledgementLost", err)
	}
	if _, ok := s.agent.Applied("lost-ack"); !ok {
		t.Fatal("agent did not apply delivery before losing its acknowledgement")
	}
	if got := s.agent.DeliveryCheckpoint(); got == managerBefore {
		t.Fatalf("agent checkpoint = %#v, want it ahead of manager checkpoint %#v", got, managerBefore)
	}
	if got, _ := s.manager.AgentCheckpoint(testTarget); got != managerBefore {
		t.Fatalf("manager advanced checkpoint without acknowledgement: got %#v, want %#v", got, managerBefore)
	}

	if err := s.manager.RetryDelivery(record.Index); err != nil {
		t.Fatalf("retry delivery after lost acknowledgement: %v", err)
	}
	if got, want := s.manager.AgentCheckpoint(testTarget); !want || got != s.agent.DeliveryCheckpoint() {
		t.Fatalf("manager checkpoint after retry = %#v, %t; want agent checkpoint %#v", got, want, s.agent.DeliveryCheckpoint())
	}
	if got, want := s.agent.StaleCheckpointResponses(), uint64(1); got != want {
		t.Fatalf("agent stale-checkpoint responses = %d, want %d", got, want)
	}
}

func TestDeliveryLogCatchUpSelectivelyDisclosesTargetedRecord(t *testing.T) {
	s := newEnrolledScenario(t)

	anchor := mustSignDelivery(t, s.controlledClient, "catch-up-anchor", 1, []byte(`{"position":0}`))
	if _, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), anchor); err != nil {
		t.Fatalf("submit checkpoint anchor: %v", err)
	}
	anchorCheckpoint, ok := s.manager.AgentCheckpoint(testTarget)
	if !ok {
		t.Fatal("manager did not record the anchor checkpoint")
	}

	s.agent.FailNextDeliveriesBeforeAccepting(64)
	var selected protocol.DeliveryRecord
	for i := 0; i < 64; i++ {
		delivery := mustSignDelivery(t, s.controlledClient, fmt.Sprintf("catch-up-%d", i), 1, []byte(fmt.Sprintf(`{"position":%d}`, i+1)))
		record, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), delivery)
		if !errors.Is(err, deliveryagent.ErrDeliveryUnavailable) {
			t.Fatalf("submit catch-up delivery %d error = %v, want ErrDeliveryUnavailable", i, err)
		}
		if i == 31 {
			selected = record
		}
	}
	if got, _ := s.manager.AgentCheckpoint(testTarget); got != anchorCheckpoint {
		t.Fatalf("manager checkpoint advanced across failed pushes: got %#v, want %#v", got, anchorCheckpoint)
	}
	if got := s.agent.DeliveryCheckpoint(); got != anchorCheckpoint {
		t.Fatalf("agent checkpoint advanced across pre-accept failures: got %#v, want %#v", got, anchorCheckpoint)
	}

	if err := s.manager.RetryDelivery(selected.Index); err != nil {
		t.Fatalf("manager retry selectively disclosed historical delivery: %v", err)
	}
	attempt, ok := s.agent.LastDeliveryAttempt()
	if !ok {
		t.Fatal("agent did not retain the manager delivery-attempt summary")
	}
	if got, want := attempt.EntryIndexes, []uint64{selected.Index}; !equalIndexes(got, want) {
		t.Fatalf("disclosed delivery-log indexes = %v, want %v", got, want)
	}
	if got := attempt.ConsistencyProofHashes; got == 0 || got > 7 {
		t.Fatalf("1-to-65 consistency proof has %d hashes, want 1..7", got)
	}
	if got := attempt.InclusionProofHashes[0]; got == 0 || got > 7 {
		t.Fatalf("selected inclusion proof has %d hashes, want 1..7", got)
	}
	if got, want := s.agent.DeliveryCheckpoint().Size, uint64(65); got != want {
		t.Fatalf("delivery checkpoint size = %d, want %d", got, want)
	}
	if _, ok := s.agent.Applied("catch-up-31"); !ok {
		t.Fatal("selected catch-up delivery was not applied")
	}
	if _, ok := s.agent.Applied("catch-up-0"); ok {
		t.Fatal("unrelated catch-up delivery was disclosed and applied")
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
	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) {
		t.Fatalf("sync substituted enrollment error = %v, want ErrEnrollment", err)
	}
}

func TestVerifierTrustCheckpointDoesNotScaleWithEnrolledIdentities(t *testing.T) {
	s := newScenario(t)
	before, err := json.Marshal(s.agent.VerifierCheckpoint())
	if err != nil {
		t.Fatalf("encode initial verifier checkpoint: %v", err)
	}

	for i := 0; i < 16; i++ {
		controlledClient, err := client.New(client.Config{
			TenantID:     testTenant,
			Issuer:       s.provider.Issuer(),
			OIDCClientID: testOIDCClient,
			RedirectURI:  testRedirectURI,
			HTTPClient:   s.provider.HTTPClient(),
		})
		if err != nil {
			t.Fatalf("new controlled client %d: %v", i, err)
		}
		enrollment, err := controlledClient.Enroll(context.Background(), fmt.Sprintf("user-%d", i))
		if err != nil {
			t.Fatalf("enroll controlled client %d: %v", i, err)
		}
		if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
			t.Fatalf("submit enrollment %d: %v", i, err)
		}
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync enrollments: %v", err)
	}

	checkpoint := s.agent.VerifierCheckpoint()
	if got := len(checkpoint.Exceptions); got != 0 {
		t.Fatalf("exceptional event digests = %d, want 0", got)
	}
	after, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatalf("encode populated verifier checkpoint: %v", err)
	}
	if got, want := len(after), len(before); got != want {
		t.Fatalf("verifier checkpoint size after 16 enrollments = %d, want constant size %d", got, want)
	}
}

func TestExceptionCapacityBlocksFurtherMapAdvancement(t *testing.T) {
	s := newScenario(t)
	agent, err := deliveryagent.New(deliveryagent.Config{
		TenantID:          testTenant,
		TargetID:          testTarget,
		OIDCIssuer:        s.provider.Issuer(),
		OIDCClientID:      testOIDCClient,
		HTTPClient:        s.provider.HTTPClient(),
		ExceptionCapacity: 1,
	})
	if err != nil {
		t.Fatalf("new capacity-limited delivery agent: %v", err)
	}

	appendInvalidEnrollment := func(subject string) protocol.AuthenticatedMapUpdate {
		t.Helper()
		controlledClient, err := client.New(client.Config{
			TenantID:     testTenant,
			Issuer:       s.provider.Issuer(),
			OIDCClientID: testOIDCClient,
			RedirectURI:  testRedirectURI,
			HTTPClient:   s.provider.HTTPClient(),
		})
		if err != nil {
			t.Fatalf("new %s client: %v", subject, err)
		}
		enrollment, err := controlledClient.Enroll(context.Background(), subject)
		if err != nil {
			t.Fatalf("enroll %s: %v", subject, err)
		}
		attacker, err := client.New(client.Config{TenantID: testTenant})
		if err != nil {
			t.Fatalf("new %s attacker: %v", subject, err)
		}
		enrollment.ContinuityPublicKey = attacker.ContinuityPublicKey()
		return s.manager.Compromised().AppendEnrollment(enrollment)
	}

	first := appendInvalidEnrollment("invalid-1")
	if err := agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(agent.MapRoot())); !errors.Is(err, deliveryagent.ErrEnrollment) {
		t.Fatalf("first invalid enrollment error = %v, want ErrEnrollment", err)
	}
	if got, want := agent.MapRoot(), first.Root; got != want {
		t.Fatalf("map root after retained exception = %q, want %q", got, want)
	}
	if got := len(agent.VerifierCheckpoint().Exceptions); got != 1 {
		t.Fatalf("exceptional event digests = %d, want 1", got)
	}
	exception := agent.VerifierCheckpoint().Exceptions[0]
	if got, want := exception.IdentityID, first.KeyHistory.Event.IdentityID; got != want {
		t.Fatalf("exception identity = %q, want %q", got, want)
	}
	if got, want := exception.Sequence, first.KeyHistory.Event.Sequence; got != want {
		t.Fatalf("exception sequence = %d, want %d", got, want)
	}
	if got, want := exception.EventDigest, first.KeyHistory.Event.Hash; got != want {
		t.Fatalf("exception digest = %q, want %q", got, want)
	}
	validClient, err := client.New(client.Config{
		TenantID:     testTenant,
		Issuer:       s.provider.Issuer(),
		OIDCClientID: testOIDCClient,
		RedirectURI:  testRedirectURI,
		HTTPClient:   s.provider.HTTPClient(),
	})
	if err != nil {
		t.Fatalf("new valid client: %v", err)
	}
	validEnrollment, err := validClient.Enroll(context.Background(), "valid-between-exceptions")
	if err != nil {
		t.Fatalf("enroll valid client: %v", err)
	}
	validUpdate, err := s.manager.SubmitEnrollment(validEnrollment)
	if err != nil {
		t.Fatalf("submit valid enrollment: %v", err)
	}
	if err := agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(agent.MapRoot())); err != nil {
		t.Fatalf("full exception set blocked a valid map update: %v", err)
	}
	if got, want := agent.MapRoot(), validUpdate.Root; got != want {
		t.Fatalf("map root after valid update = %q, want %q", got, want)
	}

	appendInvalidEnrollment("invalid-2")
	if err := agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(agent.MapRoot())); !errors.Is(err, deliveryagent.ErrExceptionCapacity) {
		t.Fatalf("second invalid enrollment error = %v, want ErrExceptionCapacity", err)
	}
	if got, want := agent.MapRoot(), validUpdate.Root; got != want {
		t.Fatalf("map root advanced after exception capacity filled: got %q, want %q", got, want)
	}
}

func TestExceptionalIdentityDescendantsReuseOneExceptionSlot(t *testing.T) {
	s := newScenario(t)
	agent, err := deliveryagent.New(deliveryagent.Config{
		TenantID:          testTenant,
		TargetID:          testTarget,
		OIDCIssuer:        s.provider.Issuer(),
		OIDCClientID:      testOIDCClient,
		HTTPClient:        s.provider.HTTPClient(),
		ExceptionCapacity: 1,
	})
	if err != nil {
		t.Fatalf("new capacity-limited delivery agent: %v", err)
	}

	enrollment := mustEnroll(t, s.controlledClient)
	attacker, err := client.New(client.Config{TenantID: testTenant})
	if err != nil {
		t.Fatalf("new attacker client: %v", err)
	}
	enrollment.ContinuityPublicKey = attacker.ContinuityPublicKey()
	invalidEnrollment := s.manager.Compromised().AppendEnrollment(enrollment)
	if err := agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(agent.MapRoot())); !errors.Is(err, deliveryagent.ErrEnrollment) {
		t.Fatalf("sync invalid enrollment error = %v, want ErrEnrollment", err)
	}
	if got, want := agent.MapRoot(), invalidEnrollment.Root; got != want {
		t.Fatalf("map root after invalid enrollment = %q, want %q", got, want)
	}

	// This rotation is structurally append-only but descends from the already
	// exceptional enrollment. It must not consume another exception slot.
	rotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare descendant rotation: %v", err)
	}
	descendant := s.manager.Compromised().AppendRotation(rotation)
	if err := agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(agent.MapRoot())); err != nil {
		t.Fatalf("sync descendant of exceptional identity: %v", err)
	}
	if got, want := agent.MapRoot(), descendant.MapUpdate.Root; got != want {
		t.Fatalf("map root after exceptional descendant = %q, want %q", got, want)
	}
	if got := len(agent.VerifierCheckpoint().Exceptions); got != 1 {
		t.Fatalf("exception entries after descendant = %d, want 1", got)
	}
}

func TestExceptionalEnrollmentCannotAuthorizeDelivery(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	attacker, err := client.New(client.Config{TenantID: testTenant})
	if err != nil {
		t.Fatalf("new attacker client: %v", err)
	}
	enrollment.ContinuityPublicKey = attacker.ContinuityPublicKey()
	s.manager.Compromised().AppendEnrollment(enrollment)
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); !errors.Is(err, deliveryagent.ErrEnrollment) {
		t.Fatalf("sync exceptional enrollment error = %v, want ErrEnrollment", err)
	}
	if err := s.manager.RegisterAgent(testTarget, s.agent); err != nil {
		t.Fatalf("register delivery agent: %v", err)
	}
	attackerState := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            testTenant,
		IdentityID:          enrollment.IdentityID,
		ContinuityPublicKey: attacker.ContinuityPublicKey(),
	}
	attackerStateDigest, err := attackerState.Digest()
	if err != nil {
		t.Fatalf("digest attacker continuity state: %v", err)
	}
	forged := mustSignDeliveryAs(t, attacker, enrollment.IdentityID, attackerStateDigest, "exceptional-enrollment", 1, []byte(`{"attack":true}`))
	if _, err := s.manager.SubmitDelivery(enrollment.IdentityID, forged); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("delivery rooted in exceptional enrollment error = %v, want ErrSigningState", err)
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
	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) || !strings.Contains(err.Error(), "nonce") {
		t.Fatalf("replacement enrollment error = %v, want nonce-bound ErrEnrollment", err)
	}
}

func TestEnrollmentRejectsAuthenticatedMapKeySubstitution(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	enrollment.IdentityID = protocol.IdentityID(testTenant, s.provider.Issuer(), "mallory")

	s.manager.Compromised().AppendEnrollment(enrollment)
	err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) || !strings.Contains(err.Error(), "claimed identity") {
		t.Fatalf("map-key substitution error = %v, want claimed-identity ErrEnrollment", err)
	}
}

func TestEnrollmentReplayIsRejected(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync first enrollment: %v", err)
	}

	s.manager.Compromised().AppendEnrollment(enrollment)
	err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrEnrollment) || !strings.Contains(err.Error(), "already enrolled") {
		t.Fatalf("replayed enrollment error = %v, want already-enrolled ErrEnrollment", err)
	}
}

func TestAuthenticatedMapUpdateReusesUnchangedSiblingPath(t *testing.T) {
	alice := "identity-alice"
	bob := "identity-bob"
	aliceEnrollment, err := protocol.NewKeyHistoryUpdate(
		protocol.EmptyKeyHistoryHead(alice),
		nil,
		"state-alice-0",
		protocol.KeyEvent{Kind: protocol.KeyEventEnrollment},
	)
	if err != nil {
		t.Fatalf("build Alice enrollment history: %v", err)
	}
	bobEnrollment, err := protocol.NewKeyHistoryUpdate(
		protocol.EmptyKeyHistoryHead(bob),
		nil,
		"state-bob-0",
		protocol.KeyEvent{Kind: protocol.KeyEventEnrollment},
	)
	if err != nil {
		t.Fatalf("build Bob enrollment history: %v", err)
	}
	aliceRotation, err := protocol.NewKeyHistoryUpdate(
		aliceEnrollment.Head,
		[]protocol.KeyEventRecord{aliceEnrollment.Event},
		"state-alice-1",
		protocol.KeyEvent{Kind: protocol.KeyEventRotation},
	)
	if err != nil {
		t.Fatalf("build Alice rotation history: %v", err)
	}

	heads := map[string]protocol.KeyHistoryHead{
		alice: aliceEnrollment.Head,
		bob:   bobEnrollment.Head,
	}
	oldRoot, err := protocol.KeyHistoryMapRoot(testTenant, heads)
	if err != nil {
		t.Fatalf("compute old map root: %v", err)
	}
	update, err := protocol.NewAuthenticatedMapUpdate(testTenant, heads, aliceRotation)
	if err != nil {
		t.Fatalf("build authenticated map update: %v", err)
	}
	if got, want := update.PreviousRoot, oldRoot; got != want {
		t.Fatalf("previous map root = %q, want %q", got, want)
	}
	if got, want := len(update.SiblingBitmap), 32; got != want {
		t.Fatalf("sibling bitmap = %d bytes, want %d", got, want)
	}
	if got, want := len(update.SiblingHashes), 1; got != want {
		t.Fatalf("non-empty sibling hashes = %d, want %d", got, want)
	}
	nextHeads := map[string]protocol.KeyHistoryHead{
		alice: aliceRotation.Head,
		bob:   bobEnrollment.Head,
	}
	wantRoot, err := protocol.KeyHistoryMapRoot(testTenant, nextHeads)
	if err != nil {
		t.Fatalf("compute expected successor root: %v", err)
	}
	if update.Root != wantRoot {
		t.Fatalf("successor map root = %q, want %q", update.Root, wantRoot)
	}
	verifiedHead, err := protocol.VerifyAuthenticatedMapUpdate(testTenant, oldRoot, update)
	if err != nil {
		t.Fatalf("verify authenticated map update: %v", err)
	}
	if verifiedHead != aliceRotation.Head {
		t.Fatalf("verified head = %#v, want %#v", verifiedHead, aliceRotation.Head)
	}

	tampered := update
	tampered.SiblingBitmap = append([]byte(nil), update.SiblingBitmap...)
	tampered.SiblingHashes = append([]string(nil), update.SiblingHashes...)
	tampered.SiblingHashes[0] = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, err := protocol.VerifyAuthenticatedMapUpdate(testTenant, oldRoot, tampered); err == nil {
		t.Fatal("tampered sibling path unexpectedly verified")
	}
}

func TestAuthenticatedMapUpdateProvesAbsentLeaf(t *testing.T) {
	alice := "identity-alice"
	bob := "identity-bob"
	bobEnrollment, err := protocol.NewKeyHistoryUpdate(
		protocol.EmptyKeyHistoryHead(bob),
		nil,
		"state-bob-0",
		protocol.KeyEvent{Kind: protocol.KeyEventEnrollment},
	)
	if err != nil {
		t.Fatalf("build Bob enrollment history: %v", err)
	}
	aliceEnrollment, err := protocol.NewKeyHistoryUpdate(
		protocol.EmptyKeyHistoryHead(alice),
		nil,
		"state-alice-0",
		protocol.KeyEvent{Kind: protocol.KeyEventEnrollment},
	)
	if err != nil {
		t.Fatalf("build Alice enrollment history: %v", err)
	}

	heads := map[string]protocol.KeyHistoryHead{bob: bobEnrollment.Head}
	oldRoot, err := protocol.KeyHistoryMapRoot(testTenant, heads)
	if err != nil {
		t.Fatalf("compute old map root: %v", err)
	}
	update, err := protocol.NewAuthenticatedMapUpdate(testTenant, heads, aliceEnrollment)
	if err != nil {
		t.Fatalf("build authenticated map insertion: %v", err)
	}
	if update.PreviousHead != nil {
		t.Fatalf("absence proof contains previous head %#v", *update.PreviousHead)
	}
	if _, err := protocol.VerifyAuthenticatedMapUpdate(testTenant, oldRoot, update); err != nil {
		t.Fatalf("verify authenticated map insertion: %v", err)
	}
	wantRoot, err := protocol.KeyHistoryMapRoot(testTenant, map[string]protocol.KeyHistoryHead{
		alice: aliceEnrollment.Head,
		bob:   bobEnrollment.Head,
	})
	if err != nil {
		t.Fatalf("compute expected insertion root: %v", err)
	}
	if update.Root != wantRoot {
		t.Fatalf("insertion root = %q, want %q", update.Root, wantRoot)
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

		err = s.agent.ReceiveDelivery(record, mustDeliveryProof(t, s.manager, s.agent, record, record.Index))
		if !errors.Is(err, deliveryagent.ErrSignature) {
			t.Fatalf("forged delivery error = %v, want ErrSignature", err)
		}
	})

	t.Run("signed content cannot be altered", func(t *testing.T) {
		s := newEnrolledScenario(t)
		signed := mustSignDelivery(t, s.controlledClient, "altered", 1, []byte(`{"approved":true}`))
		signed.Content = []byte(`{"approved":false}`)
		record := s.manager.Compromised().AppendDelivery(signed)

		err := s.agent.ReceiveDelivery(record, mustDeliveryProof(t, s.manager, s.agent, record, record.Index))
		if !errors.Is(err, deliveryagent.ErrAttestation) {
			t.Fatalf("altered delivery error = %v, want ErrAttestation", err)
		}
	})

	t.Run("signed delivery metadata cannot be altered", func(t *testing.T) {
		s := newEnrolledScenario(t)
		signed := mustSignDelivery(t, s.controlledClient, "metadata", 1, []byte(`{"approved":true}`))
		signed.Attestation.Generation = 2
		record := s.manager.Compromised().AppendDelivery(signed)

		err := s.agent.ReceiveDelivery(record, mustDeliveryProof(t, s.manager, s.agent, record, record.Index))
		if !errors.Is(err, deliveryagent.ErrSignature) {
			t.Fatalf("altered delivery metadata error = %v, want ErrSignature", err)
		}
	})
}

func TestCompromisedResourceManagerCannotTamperIdentityTrustProof(t *testing.T) {
	t.Run("sparse-map membership", func(t *testing.T) {
		s := newEnrolledScenario(t)
		signed := mustSignDelivery(t, s.controlledClient, "tampered-map-proof", 1, []byte(`{"approved":true}`))
		record := s.manager.Compromised().AppendDelivery(signed)
		proof := mustDeliveryProof(t, s.manager, s.agent, record, record.Index)
		proof.Identity.Map.Head.Root = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

		if err := s.agent.ReceiveDelivery(record, proof); !errors.Is(err, deliveryagent.ErrSigningState) {
			t.Fatalf("tampered map-membership proof error = %v, want ErrSigningState", err)
		}
	})

	t.Run("key history", func(t *testing.T) {
		s := newEnrolledScenario(t)
		signed := mustSignDelivery(t, s.controlledClient, "tampered-history-proof", 1, []byte(`{"approved":true}`))
		record := s.manager.Compromised().AppendDelivery(signed)
		proof := mustDeliveryProof(t, s.manager, s.agent, record, record.Index)
		proof.Identity.SigningEvent.Event.Event.Enrollment.ContinuityPublicKey[0] ^= 0xff

		if err := s.agent.ReceiveDelivery(record, proof); !errors.Is(err, deliveryagent.ErrSigningState) {
			t.Fatalf("tampered key-history proof error = %v, want ErrSigningState", err)
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
	if err := s.agent.ReceiveDelivery(record, mustDeliveryProof(t, s.manager, s.agent, record, record.Index)); err != nil {
		t.Fatalf("agent rejected authentic provenance after manager authorization bypass: %v", err)
	}
	if _, ok := s.agent.Applied("rbac-denied"); !ok {
		t.Fatal("agent did not apply genuinely signed delivery")
	}
}

func TestContinuityKeyRotation(t *testing.T) {
	s := newEnrolledScenario(t)

	before := mustSignDelivery(t, s.controlledClient, "before-rotation", 1, []byte(`{"version":1}`))
	if _, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), before); err != nil {
		t.Fatalf("submit pre-rotation delivery: %v", err)
	}

	// Signing time is deliberately irrelevant to the cutoff. This delivery is
	// authentic, but assigning it a position after the marker must retire it.
	queuedOld := mustSignDelivery(t, s.controlledClient, "queued-old-key", 1, []byte(`{"version":"queued"}`))

	rotation, rotatedClient, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	commit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation)
	if err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	if commit.Marker.Event.Kind != protocol.DeliveryLogEventRotation {
		t.Fatalf("rotation marker kind = %q, want %q", commit.Marker.Event.Kind, protocol.DeliveryLogEventRotation)
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync rotation: %v", err)
	}

	after := mustSignDelivery(t, rotatedClient, "after-rotation", 1, []byte(`{"version":2}`))
	if _, err := s.manager.SubmitDelivery(rotatedClient.IdentityID(), after); err != nil {
		t.Fatalf("submit post-rotation delivery: %v", err)
	}

	retiredRecord := s.manager.Compromised().AppendDelivery(queuedOld)
	if err := s.agent.ReceiveDelivery(retiredRecord, mustDeliveryProof(t, s.manager, s.agent, retiredRecord, retiredRecord.Index)); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("retired-key delivery error = %v, want ErrSigningState", err)
	}
}

func TestSuccessorKeyDeliveryBeforeRotationMarkerIsRejected(t *testing.T) {
	s := newEnrolledScenario(t)
	rotation, rotatedClient, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	early := mustSignDelivery(t, rotatedClient, "early-new-key", 1, []byte(`{"version":"early"}`))
	earlyRecord := s.manager.Compromised().AppendDelivery(early)
	commit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation)
	if err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync rotation: %v", err)
	}
	if err := s.agent.ReceiveDelivery(earlyRecord, mustDeliveryProof(t, s.manager, s.agent, earlyRecord, earlyRecord.Index, commit.Marker.Index)); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("early successor delivery error = %v, want ErrSigningState", err)
	}
}

func TestCurrentMapLeafCommitsPerIdentityKeyHistory(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}

	historical := mustSignDelivery(t, s.controlledClient, "historical", 1, []byte(`{"version":1}`))
	historicalRecord := mustSubmitUndelivered(t, s.manager, s.controlledClient.IdentityID(), historical)
	rotation, rotatedClient, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	commit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation)
	if err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	current := mustSignDelivery(t, rotatedClient, "current", 1, []byte(`{"version":2}`))
	currentRecord := mustSubmitUndelivered(t, s.manager, rotatedClient.IdentityID(), current)

	// This agent accepts the latest root without retaining the identity leaf.
	// The manager can later prove deliveries on either side of the marker from
	// its retained history.
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync key history: %v", err)
	}
	s.provider.Close()
	identityProof, err := s.manager.IdentityTrustProof(s.controlledClient.IdentityID(), rotatedClient.ContinuityStateDigest())
	if err != nil {
		t.Fatalf("build current identity proof: %v", err)
	}
	head := identityProof.Map.Head
	if got, want := head.Size, uint64(2); got != want {
		t.Fatalf("key-history size = %d, want %d", got, want)
	}
	if got, want := head.CurrentStateDigest, rotatedClient.ContinuityStateDigest(); got != want {
		t.Fatalf("current state digest = %q, want %q", got, want)
	}
	if err := s.manager.RegisterAgent(testTarget, s.agent); err != nil {
		t.Fatalf("register delivery agent: %v", err)
	}
	if err := s.manager.RetryDelivery(historicalRecord.Index); err != nil {
		t.Fatalf("manager deliver historical state using its retiring-marker proof: %v", err)
	}
	assertLastDeliveryAttemptIndexes(t, s.agent, commit.Marker.Index, historicalRecord.Index)
	assertLastIdentityProofSequences(t, s.agent, 0, 1)
	if err := s.manager.RetryDelivery(currentRecord.Index); err != nil {
		t.Fatalf("manager deliver current state using its establishing-marker proof: %v", err)
	}
	assertLastDeliveryAttemptIndexes(t, s.agent, commit.Marker.Index, currentRecord.Index)
	assertLastIdentityProofSequences(t, s.agent, 1)
}

func TestHistoricalMiddleKeyIsBoundedByAdjacentMarkers(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}

	firstRotation, middleClient, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare first rotation: %v", err)
	}
	firstCommit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), firstRotation)
	if err != nil {
		t.Fatalf("submit first rotation: %v", err)
	}
	middle := mustSignDelivery(t, middleClient, "middle-valid", 1, []byte(`{"state":1}`))
	middleRecord := mustSubmitUndelivered(t, s.manager, middleClient.IdentityID(), middle)
	queuedMiddle := mustSignDelivery(t, middleClient, "middle-too-late", 1, []byte(`{"state":"late"}`))

	secondRotation, currentClient, err := middleClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare second rotation: %v", err)
	}
	secondCommit, err := s.manager.SubmitRotation(middleClient.IdentityID(), secondRotation)
	if err != nil {
		t.Fatalf("submit second rotation: %v", err)
	}
	lateMiddleRecord := s.manager.Compromised().AppendDelivery(queuedMiddle)
	current := mustSignDelivery(t, currentClient, "middle-successor", 1, []byte(`{"state":2}`))
	currentRecord := mustSubmitUndelivered(t, s.manager, currentClient.IdentityID(), current)

	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync key-history updates: %v", err)
	}
	if err := s.manager.RegisterAgent(testTarget, s.agent); err != nil {
		t.Fatalf("register delivery agent: %v", err)
	}
	if err := s.manager.RetryDelivery(middleRecord.Index); err != nil {
		t.Fatalf("manager deliver middle state using both adjacent-marker proofs: %v", err)
	}
	assertLastDeliveryAttemptIndexes(t, s.agent, firstCommit.Marker.Index, secondCommit.Marker.Index, middleRecord.Index)
	assertLastIdentityProofSequences(t, s.agent, 1, 2)
	if err := s.agent.ReceiveDelivery(lateMiddleRecord, mustDeliveryProof(t, s.manager, s.agent, lateMiddleRecord, lateMiddleRecord.Index)); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("middle-state delivery after retiring marker error = %v, want ErrSigningState", err)
	}
	if err := s.manager.RetryDelivery(currentRecord.Index); err != nil {
		t.Fatalf("manager deliver successor state: %v", err)
	}
	assertLastDeliveryAttemptIndexes(t, s.agent, secondCommit.Marker.Index, currentRecord.Index)
	assertLastIdentityProofSequences(t, s.agent, 2)
}

func TestIdentityTrustProofSelectsAtMostSigningAndSuccessorEvents(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}

	controlledClient := s.controlledClient
	stateDigests := []string{controlledClient.ContinuityStateDigest()}
	var middleRecord protocol.DeliveryRecord
	for generation := 1; generation <= 8; generation++ {
		rotation, successor, err := controlledClient.PrepareRotation()
		if err != nil {
			t.Fatalf("prepare rotation %d: %v", generation, err)
		}
		commit, err := s.manager.SubmitRotation(controlledClient.IdentityID(), rotation)
		if err != nil {
			t.Fatalf("submit rotation %d: %v", generation, err)
		}
		if commit.MapUpdate.Predecessor == nil {
			t.Fatalf("rotation %d map update has no predecessor-event proof", generation)
		}
		if got, want := commit.MapUpdate.Predecessor.Event.Sequence, uint64(generation-1); got != want {
			t.Fatalf("rotation %d predecessor sequence = %d, want %d", generation, got, want)
		}
		if commit.MapUpdate.RotationRecord == nil {
			t.Fatalf("rotation %d map update has no marker record", generation)
		}
		controlledClient = successor
		stateDigests = append(stateDigests, successor.ContinuityStateDigest())
		if generation == 4 {
			middleDelivery := mustSignDelivery(t, controlledClient, "long-history-middle", 1, []byte(`{"generation":4}`))
			middleRecord = mustSubmitUndelivered(t, s.manager, controlledClient.IdentityID(), middleDelivery)
		}
	}
	currentDelivery := mustSignDelivery(t, controlledClient, "long-history-current", 1, []byte(`{"generation":8}`))
	currentRecord := mustSubmitUndelivered(t, s.manager, controlledClient.IdentityID(), currentDelivery)
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync long key history: %v", err)
	}

	middle, err := s.manager.IdentityTrustProof(s.controlledClient.IdentityID(), stateDigests[4])
	if err != nil {
		t.Fatalf("construct middle-state proof: %v", err)
	}
	if got, want := middle.SigningEvent.Event.Sequence, uint64(4); got != want {
		t.Fatalf("middle signing-event sequence = %d, want %d", got, want)
	}
	if middle.SuccessorEvent == nil {
		t.Fatal("middle-state proof has no retiring successor event")
	}
	if got, want := middle.SuccessorEvent.Event.Sequence, uint64(5); got != want {
		t.Fatalf("middle successor sequence = %d, want %d", got, want)
	}
	if got := len(middle.SigningEvent.InclusionProof); got > 4 {
		t.Fatalf("middle signing-event proof has %d hashes, want <= 4", got)
	}
	if got := len(middle.SuccessorEvent.InclusionProof); got > 4 {
		t.Fatalf("middle successor-event proof has %d hashes, want <= 4", got)
	}

	current, err := s.manager.IdentityTrustProof(s.controlledClient.IdentityID(), stateDigests[8])
	if err != nil {
		t.Fatalf("construct current-state proof: %v", err)
	}
	if got, want := current.SigningEvent.Event.Sequence, uint64(8); got != want {
		t.Fatalf("current signing-event sequence = %d, want %d", got, want)
	}
	if current.SuccessorEvent != nil {
		t.Fatalf("current-state proof unexpectedly contains successor event at sequence %d", current.SuccessorEvent.Event.Sequence)
	}
	if got, want := len(current.Map.SiblingBitmap), 32; got != want {
		t.Fatalf("map sibling bitmap = %d bytes, want %d", got, want)
	}
	if got := len(current.Map.SiblingHashes); got != 0 {
		t.Fatalf("single-identity map proof contains %d non-empty sibling hashes, want 0", got)
	}

	if err := s.manager.RegisterAgent(testTarget, s.agent); err != nil {
		t.Fatalf("register delivery agent: %v", err)
	}
	if err := s.manager.RetryDelivery(middleRecord.Index); err != nil {
		t.Fatalf("push middle delivery from long history: %v", err)
	}
	assertLastIdentityProofSequences(t, s.agent, 4, 5)
	middleAttempt, _ := s.agent.LastDeliveryAttempt()
	for i, count := range middleAttempt.IdentityInclusionProofHashes {
		if count > 4 {
			t.Fatalf("pushed middle identity proof %d has %d hashes, want <= 4", i, count)
		}
	}
	if err := s.manager.RetryDelivery(currentRecord.Index); err != nil {
		t.Fatalf("push current delivery from long history: %v", err)
	}
	assertLastIdentityProofSequences(t, s.agent, 8)
}

func TestDeferredRotationMarkerMustBeProvenBeforeHistoricalDelivery(t *testing.T) {
	s := newScenario(t)
	enrollment := mustEnroll(t, s.controlledClient)
	if _, err := s.manager.SubmitEnrollment(enrollment); err != nil {
		t.Fatalf("submit enrollment: %v", err)
	}
	historical := mustSignDelivery(t, s.controlledClient, "deferred-marker", 1, []byte(`{"version":1}`))
	historicalRecord := mustSubmitUndelivered(t, s.manager, s.controlledClient.IdentityID(), historical)
	rotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	commit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation)
	if err != nil {
		t.Fatalf("submit rotation: %v", err)
	}

	// Structural map state can move ahead of the delivery-log checkpoint, but
	// neither side of that key boundary is usable until its marker is proven.
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync key history before marker: %v", err)
	}
	err = s.agent.ReceiveDelivery(historicalRecord, mustDeliveryProof(t, s.manager, s.agent, historicalRecord, historicalRecord.Index))
	if !errors.Is(err, deliveryagent.ErrSigningState) || !strings.Contains(err.Error(), "marker") {
		t.Fatalf("historical delivery before marker proof error = %v, want marker ErrSigningState", err)
	}
	if err := s.agent.AdvanceDeliveryLog(mustDeliveryLogUpdate(t, s.manager, s.agent, commit.Marker.Index)); err != nil {
		t.Fatalf("prove deferred marker: %v", err)
	}
	if err := s.agent.ReceiveDelivery(historicalRecord, mustDeliveryProof(t, s.manager, s.agent, historicalRecord, commit.Marker.Index, historicalRecord.Index)); err != nil {
		t.Fatalf("retry historical delivery after marker proof: %v", err)
	}
}

func TestPerIdentityRotationMarkersMustAdvance(t *testing.T) {
	s := newEnrolledScenario(t)
	firstRotation, rotatedClient, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare first rotation: %v", err)
	}
	firstCommit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), firstRotation)
	if err != nil {
		t.Fatalf("submit first rotation: %v", err)
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync first rotation: %v", err)
	}

	secondRotation, _, err := rotatedClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare second rotation: %v", err)
	}
	s.manager.Compromised().AppendRotationMapUpdate(secondRotation, firstCommit.Marker.Reference())
	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrRotation) || !strings.Contains(err.Error(), "advance") {
		t.Fatalf("non-advancing marker error = %v, want advancing-marker ErrRotation", err)
	}
}

func TestRotationRejectsNonRotationCutoffRecord(t *testing.T) {
	s := newEnrolledScenario(t)
	prior := mustSignDelivery(t, s.controlledClient, "not-a-marker", 1, []byte(`{"version":1}`))
	priorRecord, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), prior)
	if err != nil {
		t.Fatalf("submit prior delivery: %v", err)
	}

	rotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	s.manager.Compromised().AppendRotationMapUpdate(rotation, protocol.DeliveryLogReference{
		Index: priorRecord.Index,
		Hash:  priorRecord.Hash,
	})

	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrRotation) || !strings.Contains(err.Error(), "marker") {
		t.Fatalf("rotation with delivery as marker error = %v, want marker ErrRotation", err)
	}
}

func TestRotationHistoryMustReferenceMatchingMarkerPackage(t *testing.T) {
	s := newEnrolledScenario(t)
	markerRotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare marker rotation: %v", err)
	}
	historyRotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare history rotation: %v", err)
	}
	marker := s.manager.Compromised().AppendRotationMarker(markerRotation)
	if err := s.agent.AdvanceDeliveryLog(mustDeliveryLogUpdate(t, s.manager, s.agent, marker.Index)); err != nil {
		t.Fatalf("advance rotation marker: %v", err)
	}
	s.manager.Compromised().AppendRotationMapUpdate(historyRotation, marker.Reference())

	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrRotation) || !strings.Contains(err.Error(), "marker") {
		t.Fatalf("mismatched marker package error = %v, want marker ErrRotation", err)
	}
}

func TestRotationRejectsResourceManagerKeyReplacement(t *testing.T) {
	s := newEnrolledScenario(t)
	rotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}

	attacker, err := client.New(client.Config{TenantID: testTenant})
	if err != nil {
		t.Fatalf("new attacker client: %v", err)
	}
	rotation.NewContinuityPublicKey = attacker.ContinuityPublicKey()
	s.manager.Compromised().AppendRotation(rotation)

	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrRotation) {
		t.Fatalf("sync substituted rotation error = %v, want ErrRotation", err)
	}
}

func TestRotationRequiresNewKeyProofOfPossession(t *testing.T) {
	s := newEnrolledScenario(t)
	rotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	rotation.ProofByNewKey = make([]byte, ed25519.SignatureSize)
	s.manager.Compromised().AppendRotation(rotation)

	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
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

	err = s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot()))
	if !errors.Is(err, deliveryagent.ErrRotation) || !strings.Contains(err.Error(), "old key") {
		t.Fatalf("fabricated rotation error = %v, want old-key ErrRotation", err)
	}
}

func TestRotationCutoffIsLocalToAgentsThatObservedIt(t *testing.T) {
	s := newEnrolledScenario(t)
	staleAgent := newAgent(t, s)
	if err := staleAgent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(staleAgent.MapRoot())); err != nil {
		t.Fatalf("bootstrap stale agent: %v", err)
	}

	initial := mustSignDelivery(t, s.controlledClient, "initial", 1, []byte(`{"version":1}`))
	initialRecord, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), initial)
	if err != nil {
		t.Fatalf("submit initial delivery: %v", err)
	}
	// The second verifier is deliberately not the manager's active target route;
	// seed the common pre-rotation view before testing selective compromised
	// routing to the two locally witnessed histories.
	if err := staleAgent.ReceiveDelivery(initialRecord, mustDeliveryProof(t, s.manager, staleAgent, initialRecord, initialRecord.Index)); err != nil {
		t.Fatalf("stale agent receive initial delivery: %v", err)
	}

	rotation, _, err := s.controlledClient.PrepareRotation()
	if err != nil {
		t.Fatalf("prepare rotation: %v", err)
	}
	commit, err := s.manager.SubmitRotation(s.controlledClient.IdentityID(), rotation)
	if err != nil {
		t.Fatalf("submit rotation: %v", err)
	}
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("current agent sync rotation: %v", err)
	}

	compromisedOldKeyDelivery := mustSignDelivery(t, s.controlledClient, "old-key-after-cutoff", 1, []byte(`{"attack":true}`))
	record := s.manager.Compromised().AppendDelivery(compromisedOldKeyDelivery)
	if err := s.agent.ReceiveDelivery(record, mustDeliveryProof(t, s.manager, s.agent, record, commit.Marker.Index, record.Index)); !errors.Is(err, deliveryagent.ErrSigningState) {
		t.Fatalf("rotated agent error = %v, want ErrSigningState", err)
	}
	if err := staleAgent.ReceiveDelivery(record, mustDeliveryProof(t, s.manager, staleAgent, record, record.Index)); err != nil {
		t.Fatalf("stale agent should accept on its pre-rotation view: %v", err)
	}
}

func TestEstablishedAgentRejectsDeliveryLogFork(t *testing.T) {
	s := newEnrolledScenario(t)
	oldCheckpoint := s.manager.DeliveryCheckpoint()

	first := mustSignDelivery(t, s.controlledClient, "main-branch", 1, []byte(`{"branch":"main"}`))
	if _, err := s.manager.SubmitDelivery(s.controlledClient.IdentityID(), first); err != nil {
		t.Fatalf("submit main delivery: %v", err)
	}

	forked := mustSignDelivery(t, s.controlledClient, "fork", 1, []byte(`{"branch":"fork"}`))
	forkRecord, forkUpdate := s.manager.Compromised().ForgeDeliveryAt(oldCheckpoint, forked)
	if err := s.agent.ReceiveDelivery(forkRecord, protocol.DeliveryProof{Log: forkUpdate}); !errors.Is(err, deliveryagent.ErrLogFork) {
		t.Fatalf("forked delivery error = %v, want ErrLogFork", err)
	}
}

func TestEstablishedAgentRejectsAuthenticatedMapFork(t *testing.T) {
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
	forkUpdate := s.manager.Compromised().ForgeEnrollmentFromEmptyMap(forkEnrollment)

	if err := s.agent.SyncMap(context.Background(), []protocol.AuthenticatedMapUpdate{forkUpdate}); !errors.Is(err, deliveryagent.ErrMapFork) {
		t.Fatalf("forked map update error = %v, want ErrMapFork", err)
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
	if err := s.agent.SyncMap(context.Background(), s.manager.MapUpdatesAfter(s.agent.MapRoot())); err != nil {
		t.Fatalf("sync enrollment: %v", err)
	}
	if err := s.manager.RegisterAgent(testTarget, s.agent); err != nil {
		t.Fatalf("register delivery agent: %v", err)
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

func mustSubmitUndelivered(t *testing.T, manager *resourcemanager.Manager, callerID string, delivery protocol.SignedDelivery) protocol.DeliveryRecord {
	t.Helper()
	record, err := manager.SubmitDelivery(callerID, delivery)
	if !errors.Is(err, resourcemanager.ErrAgentUnavailable) {
		t.Fatalf("submit delivery without a registered agent error = %v, want ErrAgentUnavailable", err)
	}
	return record
}

func assertLastDeliveryAttemptIndexes(t *testing.T, agent *deliveryagent.Agent, want ...uint64) {
	t.Helper()
	attempt, ok := agent.LastDeliveryAttempt()
	if !ok {
		t.Fatal("agent did not retain the manager delivery-attempt summary")
	}
	if !equalIndexes(attempt.EntryIndexes, want) {
		t.Fatalf("disclosed delivery-log indexes = %v, want %v", attempt.EntryIndexes, want)
	}
}

func assertLastIdentityProofSequences(t *testing.T, agent *deliveryagent.Agent, want ...uint64) {
	t.Helper()
	attempt, ok := agent.LastDeliveryAttempt()
	if !ok {
		t.Fatal("agent did not retain the manager delivery-attempt summary")
	}
	if !equalIndexes(attempt.IdentityEventSequences, want) {
		t.Fatalf("disclosed identity-event sequences = %v, want %v", attempt.IdentityEventSequences, want)
	}
	if got, want := attempt.MapSiblingBitmapBytes, 32; got != want {
		t.Fatalf("map sibling bitmap = %d bytes, want %d", got, want)
	}
	if got := attempt.MapSiblingHashes; got != 0 {
		t.Fatalf("single-identity delivery proof disclosed %d non-empty map siblings, want 0", got)
	}
}

func equalIndexes(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func mustDeliveryLogUpdate(t *testing.T, manager *resourcemanager.Manager, agent *deliveryagent.Agent, indexes ...uint64) protocol.DeliveryLogUpdate {
	t.Helper()
	update, err := manager.DeliveryLogUpdate(agent.DeliveryCheckpoint(), indexes...)
	if err != nil {
		t.Fatalf("build delivery-log update: %v", err)
	}
	return update
}

func mustDeliveryProof(t *testing.T, manager *resourcemanager.Manager, agent *deliveryagent.Agent, record protocol.DeliveryRecord, indexes ...uint64) protocol.DeliveryProof {
	t.Helper()
	if record.Event.Delivery == nil {
		t.Fatal("delivery proof requested for a non-delivery record")
	}
	identityProof, err := manager.IdentityTrustProofAt(record.Event.Delivery.Attestation.IdentityID, record.Event.Delivery.Attestation.SigningStateDigest, agent.MapRoot())
	if err != nil {
		t.Fatalf("build identity trust proof: %v", err)
	}
	return protocol.DeliveryProof{
		Log:      mustDeliveryLogUpdate(t, manager, agent, indexes...),
		Identity: identityProof,
	}
}
