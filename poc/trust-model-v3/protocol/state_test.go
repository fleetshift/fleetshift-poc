package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestSuccessorStateDigestIsBoundByRotationAuthorization(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	previous, previousDigest, err := EnrolledContinuityState("tenant-acme", "identity-alice", publicKey)
	if err != nil {
		t.Fatalf("enrolled state: %v", err)
	}
	newPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate successor key: %v", err)
	}
	state, authorization, err := SuccessorContinuityState(previous, previousDigest, newPublicKey)
	if err != nil {
		t.Fatalf("successor state: %v", err)
	}
	if authorization.PreviousStateDigest != previousDigest {
		t.Fatalf("authorization previous digest = %q, want %q", authorization.PreviousStateDigest, previousDigest)
	}
	if authorization.NewStateDigest == previousDigest {
		t.Fatal("successor digest collapsed to the predecessor digest")
	}
	if got, err := state.Digest(); err != nil || got != authorization.NewStateDigest {
		t.Fatalf("state digest = %q, %v; want authorization %q", got, err, authorization.NewStateDigest)
	}

	reconstructed, digest, err := ReconstructSuccessorState(RotationPackage{
		Authorization:          authorization,
		NewGeneration:          state.Generation,
		NewContinuityPublicKey: newPublicKey,
	})
	if err != nil {
		t.Fatalf("reconstruct successor: %v", err)
	}
	if digest != authorization.NewStateDigest || reconstructed.Generation != 1 {
		t.Fatalf("reconstructed digest = %q generation %d", digest, reconstructed.Generation)
	}
}

func TestDeliveryCommitmentIsRecomputedFromPackage(t *testing.T) {
	delivery := SignedDelivery{
		Attestation: ContentAttestation{
			Protocol:           DeliveryProtocol,
			TenantID:           "tenant-acme",
			IdentityID:         "identity-alice",
			SigningStateDigest: "state-0",
			TargetID:           "target-east",
			DeliveryID:         "delivery-1",
			FulfillmentID:      "fulfillment-1",
			Generation:         1,
			Action:             ActionPut,
			ContentDigest:      DigestBytes([]byte(`{"replicas":3}`)),
		},
		Content: []byte(`{"replicas":3}`),
	}
	record, err := NewDeliveryLogRecord(0, delivery)
	if err != nil {
		t.Fatalf("delivery log record: %v", err)
	}
	if record.Event.Commitment == nil || record.Delivery == nil {
		t.Fatal("delivery record omitted compact commitment or package")
	}
	if err := VerifyDeliveryMatchesCommitment(delivery, *record.Event.Commitment); err != nil {
		t.Fatalf("recompute commitment: %v", err)
	}
	tampered := delivery
	tampered.Content = []byte(`{"replicas":9}`)
	if err := VerifyDeliveryMatchesCommitment(tampered, *record.Event.Commitment); err == nil {
		t.Fatal("tampered package unexpectedly matched the commitment")
	}
}
