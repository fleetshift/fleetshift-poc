// Package protocol defines the purpose-separated objects exchanged among the
// controlled client, resource manager, and delivery agent.
package protocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	EnrollmentProtocol      = "fleetshift.dev/trust-v3/enrollment/v1"
	ContinuityStateProtocol = "fleetshift.dev/trust-v3/continuity-state/v1"
	RotationProtocol        = "fleetshift.dev/trust-v3/rotation/v1"
	DeliveryProtocol        = "fleetshift.dev/trust-v3/content-delivery/v1"

	TrustEventEnrollment = "enrollment"
	TrustEventRotation   = "rotation"

	ActionPut    = "put"
	ActionRemove = "remove"

	GenesisHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
)

var ErrInvalidRecord = errors.New("invalid hash-chain record")

// Checkpoint is the unoptimized replacement for a Merkle-log checkpoint. Size
// is the number of records already observed and Root is the last record hash.
type Checkpoint struct {
	Size uint64 `json:"size"`
	Root string `json:"root"`
}

func EmptyCheckpoint() Checkpoint {
	return Checkpoint{Root: GenesisHash}
}

type EnrollmentIntent struct {
	Protocol            string `json:"protocol"`
	TenantID            string `json:"tenant_id"`
	ExpectedIssuer      string `json:"expected_issuer"`
	EnrollmentClientID  string `json:"enrollment_client_id"`
	ContinuityKeyDigest string `json:"continuity_key_digest"`
	EnrollmentID        string `json:"enrollment_id"`
}

type EnrollmentPackage struct {
	Intent              EnrollmentIntent `json:"intent"`
	ContinuityPublicKey []byte           `json:"continuity_public_key"`
	ProofOfPossession   []byte           `json:"proof_of_possession"`
	IDToken             string           `json:"id_token"`
}

type ContinuityState struct {
	Protocol            string `json:"protocol"`
	TenantID            string `json:"tenant_id"`
	IdentityID          string `json:"identity_id"`
	Generation          uint64 `json:"generation"`
	ContinuityPublicKey []byte `json:"continuity_public_key"`
	PreviousStateDigest string `json:"previous_state_digest,omitempty"`
}

func (s ContinuityState) Digest() (string, error) {
	return ObjectDigest(s)
}

type RotationIntent struct {
	Protocol               string     `json:"protocol"`
	TenantID               string     `json:"tenant_id"`
	IdentityID             string     `json:"identity_id"`
	PreviousStateDigest    string     `json:"previous_state_digest"`
	NewGeneration          uint64     `json:"new_generation"`
	NewContinuityKeyDigest string     `json:"new_continuity_key_digest"`
	DeliveryCutoff         Checkpoint `json:"delivery_cutoff"`
}

type RotationPackage struct {
	Intent                 RotationIntent `json:"intent"`
	NewContinuityPublicKey []byte         `json:"new_continuity_public_key"`
	SignatureByOldKey      []byte         `json:"signature_by_old_key"`
	ProofByNewKey          []byte         `json:"proof_by_new_key"`
}

type ContentAttestation struct {
	Protocol           string `json:"protocol"`
	TenantID           string `json:"tenant_id"`
	IdentityID         string `json:"identity_id"`
	SigningStateDigest string `json:"signing_state_digest"`
	TargetID           string `json:"target_id"`
	FulfillmentID      string `json:"fulfillment_id"`
	Generation         uint64 `json:"generation"`
	Action             string `json:"action"`
	ContentDigest      string `json:"content_digest"`
}

type SignedDelivery struct {
	Attestation ContentAttestation `json:"attestation"`
	Content     []byte             `json:"content"`
	Signature   []byte             `json:"signature"`
}

type TrustEvent struct {
	Kind       string             `json:"kind"`
	Enrollment *EnrollmentPackage `json:"enrollment,omitempty"`
	Rotation   *RotationPackage   `json:"rotation,omitempty"`
}

type TrustRecord struct {
	Sequence     uint64     `json:"sequence"`
	PreviousHash string     `json:"previous_hash"`
	Event        TrustEvent `json:"event"`
	Hash         string     `json:"hash"`
}

type DeliveryRecord struct {
	Index        uint64         `json:"index"`
	PreviousHash string         `json:"previous_hash"`
	Delivery     SignedDelivery `json:"delivery"`
	Hash         string         `json:"hash"`
}

func NewTrustRecord(previous Checkpoint, event TrustEvent) (TrustRecord, error) {
	record := TrustRecord{
		Sequence:     previous.Size,
		PreviousHash: previous.Root,
		Event:        event,
	}
	hash, err := ObjectDigest(trustRecordMaterial(record))
	if err != nil {
		return TrustRecord{}, err
	}
	record.Hash = hash
	return record, nil
}

func VerifyTrustRecord(previous Checkpoint, record TrustRecord) (Checkpoint, error) {
	if record.Sequence != previous.Size || record.PreviousHash != previous.Root {
		return previous, fmt.Errorf("%w: trust record continues (%d, %s), want (%d, %s)", ErrInvalidRecord, record.Sequence, record.PreviousHash, previous.Size, previous.Root)
	}
	want, err := ObjectDigest(trustRecordMaterial(record))
	if err != nil {
		return previous, err
	}
	if record.Hash != want {
		return previous, fmt.Errorf("%w: trust record hash %q, want %q", ErrInvalidRecord, record.Hash, want)
	}
	return Checkpoint{Size: previous.Size + 1, Root: record.Hash}, nil
}

func NewDeliveryRecord(previous Checkpoint, delivery SignedDelivery) (DeliveryRecord, error) {
	record := DeliveryRecord{
		Index:        previous.Size,
		PreviousHash: previous.Root,
		Delivery:     delivery,
	}
	hash, err := ObjectDigest(deliveryRecordMaterial(record))
	if err != nil {
		return DeliveryRecord{}, err
	}
	record.Hash = hash
	return record, nil
}

func VerifyDeliveryRecord(previous Checkpoint, record DeliveryRecord) (Checkpoint, error) {
	if record.Index != previous.Size || record.PreviousHash != previous.Root {
		return previous, fmt.Errorf("%w: delivery record continues (%d, %s), want (%d, %s)", ErrInvalidRecord, record.Index, record.PreviousHash, previous.Size, previous.Root)
	}
	want, err := ObjectDigest(deliveryRecordMaterial(record))
	if err != nil {
		return previous, err
	}
	if record.Hash != want {
		return previous, fmt.Errorf("%w: delivery record hash %q, want %q", ErrInvalidRecord, record.Hash, want)
	}
	return Checkpoint{Size: previous.Size + 1, Root: record.Hash}, nil
}

func (r TrustRecord) Checkpoint() Checkpoint {
	return Checkpoint{Size: r.Sequence + 1, Root: r.Hash}
}

func (r DeliveryRecord) Checkpoint() Checkpoint {
	return Checkpoint{Size: r.Index + 1, Root: r.Hash}
}

func EnrollmentNonce(intent EnrollmentIntent) (string, error) {
	return ObjectDigest(intent)
}

func IdentityID(tenantID, issuer, subject string) string {
	identity := struct {
		Protocol string `json:"protocol"`
		TenantID string `json:"tenant_id"`
		Issuer   string `json:"issuer"`
		Subject  string `json:"subject"`
	}{
		Protocol: "fleetshift.dev/trust-v3/identity/v1",
		TenantID: tenantID,
		Issuer:   issuer,
		Subject:  subject,
	}
	digest, err := ObjectDigest(identity)
	if err != nil {
		panic(err) // A fixed struct containing strings is always JSON encodable.
	}
	return digest
}

func DigestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func ObjectDigest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode digest object: %w", err)
	}
	return DigestBytes(encoded), nil
}

func Sign(privateKey ed25519.PrivateKey, purpose string, value any) ([]byte, error) {
	payload, err := signaturePayload(purpose, value)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(privateKey, payload), nil
}

func Verify(publicKey []byte, purpose string, value any, signature []byte) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid Ed25519 public key length %d", len(publicKey))
	}
	payload, err := signaturePayload(purpose, value)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), payload, signature) {
		return errors.New("Ed25519 signature verification failed")
	}
	return nil
}

func signaturePayload(purpose string, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode signed object: %w", err)
	}
	prefix := []byte("fleetshift.dev/trust-v3/signature/" + purpose + "\x00")
	return append(prefix, encoded...), nil
}

func trustRecordMaterial(record TrustRecord) any {
	return struct {
		Sequence     uint64     `json:"sequence"`
		PreviousHash string     `json:"previous_hash"`
		Event        TrustEvent `json:"event"`
	}{record.Sequence, record.PreviousHash, record.Event}
}

func deliveryRecordMaterial(record DeliveryRecord) any {
	return struct {
		Index        uint64         `json:"index"`
		PreviousHash string         `json:"previous_hash"`
		Delivery     SignedDelivery `json:"delivery"`
	}{record.Index, record.PreviousHash, record.Delivery}
}
