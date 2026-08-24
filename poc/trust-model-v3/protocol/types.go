// Package protocol defines the purpose-separated objects exchanged among the
// controlled client, resource manager, and delivery agent.
package protocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/sparsemap"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
)

const (
	// EnrollmentProtocol is the design's user-key-enrollment-v1 identifier.
	EnrollmentProtocol      = "user-key-enrollment-v1"
	ContinuityStateProtocol = "fleetshift.dev/trust-v3/continuity-state/v1"
	RotationProtocol        = "fleetshift.dev/trust-v3/rotation/v1"
	DeliveryProtocol        = "fleetshift.dev/trust-v3/content-delivery/v1"
	KeyHistoryHeadProtocol  = "fleetshift.dev/trust-v3/key-history-head/v1"
	KeyHistoryMapProtocol   = "fleetshift.dev/trust-v3/key-history-map/v1"
	TrustManifestProtocol   = "fleetshift.dev/trust-v3/trust-manifest/v1"
	IdentityProtocol        = "fleetshift.dev/trust-v3/identity/v1"

	TrustUpdatePolicyProvisioned = "provisioned"

	KeyEventEnrollment = "enrollment"
	KeyEventRotation   = "rotation"

	DeliveryLogEventDelivery = "delivery"
	DeliveryLogEventRotation = "rotation"

	ActionPut    = "put"
	ActionRemove = "remove"
)

var (
	ErrInvalidRecord     = errors.New("invalid Merkle-log record")
	ErrInvalidKeyHistory = errors.New("invalid key history")
	ErrInvalidMapProof   = errors.New("invalid authenticated-map proof")
)

// Checkpoint identifies an RFC 6962 Merkle-log version. Size is the number of
// leaves and Root is the tree root at exactly that size.
type Checkpoint struct {
	Size uint64 `json:"size"`
	Root string `json:"root"`
}

func EmptyCheckpoint() Checkpoint {
	return Checkpoint{Root: encodeHash(rfc6962.DefaultHasher.EmptyRoot())}
}

// NewCheckpoint constructs and validates a checkpoint from an RFC 6962 root.
func NewCheckpoint(size uint64, root []byte) (Checkpoint, error) {
	if len(root) != rfc6962.DefaultHasher.Size() {
		return Checkpoint{}, fmt.Errorf("Merkle root has length %d, want %d", len(root), rfc6962.DefaultHasher.Size())
	}
	checkpoint := Checkpoint{Size: size, Root: encodeHash(root)}
	if _, err := validateCheckpoint(checkpoint); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

// TenantTrustManifest is the provisioned tenant trust root. This POC uses a
// bootstrap-only manifest (TrustUpdatePolicyProvisioned); signed manifest
// rotation and TUF remain future work.
type TenantTrustManifest struct {
	Protocol                      string   `json:"protocol"`
	TenantID                      string   `json:"tenant_id"`
	Version                       uint64   `json:"version"`
	PreviousManifestDigest        string   `json:"previous_manifest_digest,omitempty"`
	TrustUpdatePolicy             string   `json:"trust_update_policy"`
	OIDCIssuer                    string   `json:"oidc_issuer"`
	EnrollmentClientID            string   `json:"enrollment_client_id"`
	PermittedIDTokenAlgorithms    []string `json:"permitted_id_token_algorithms,omitempty"`
	PermittedContinuityAlgorithms []string `json:"permitted_continuity_algorithms,omitempty"`
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
	IdentityID          string           `json:"identity_id"`
	ContinuityPublicKey []byte           `json:"continuity_public_key"`
	ProofOfPossession   []byte           `json:"proof_of_possession"`
	IDToken             string           `json:"id_token"`
}

// ContinuityState is the design's per-generation identity state. The public
// key is carried here because this POC has no separate content-addressed
// evidence store; ContinuityKeyDigest is the committed key binding.
type ContinuityState struct {
	Protocol             string `json:"protocol"`
	TenantID             string `json:"tenant_id"`
	IdentityID           string `json:"identity_id"`
	Generation           uint64 `json:"generation"`
	ContinuityPublicKey  []byte `json:"continuity_public_key"`
	ContinuityKeyDigest  string `json:"continuity_key_digest"`
	RecoveryPolicyDigest string `json:"recovery_policy_digest,omitempty"`
	PreviousStateDigest  string `json:"previous_state_digest,omitempty"`
	TransitionDigest     string `json:"transition_digest,omitempty"`
}

func (s ContinuityState) Digest() (string, error) {
	return ObjectDigest(s)
}

// RotationAuthorization is the cutoff-free signed object: the old key binds
// the predecessor and successor state digests, and the new key proves
// possession of the same authorization. Neither signature covers a manager
// checkpoint.
type RotationAuthorization struct {
	Protocol            string `json:"protocol"`
	TenantID            string `json:"tenant_id"`
	IdentityID          string `json:"identity_id"`
	PreviousStateDigest string `json:"previous_state_digest"`
	NewStateDigest      string `json:"new_state_digest"`
}

type RotationPackage struct {
	Authorization          RotationAuthorization `json:"authorization"`
	NewGeneration          uint64                `json:"new_generation"`
	NewContinuityPublicKey []byte                `json:"new_continuity_public_key"`
	SignatureByOldKey      []byte                `json:"signature_by_old_key"`
	ProofByNewKey          []byte                `json:"proof_by_new_key"`
}

type ContentAttestation struct {
	Protocol           string `json:"protocol"`
	TenantID           string `json:"tenant_id"`
	IdentityID         string `json:"identity_id"`
	SigningStateDigest string `json:"signing_state_digest"`
	TargetID           string `json:"target_id"`
	DeliveryID         string `json:"delivery_id"`
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

// DeliveryCommitment is the compact delivery-log leaf for an infrastructure
// delivery. The signed package is support material, not part of the leaf.
type DeliveryCommitment struct {
	TenantID              string `json:"tenant_id"`
	DeliveryID            string `json:"delivery_id"`
	FulfillmentID         string `json:"fulfillment_id"`
	TargetID              string `json:"target_id"`
	Generation            uint64 `json:"generation"`
	Action                string `json:"action"`
	SigningIdentityID     string `json:"signing_identity_id"`
	SigningStateDigest    string `json:"signing_state_digest"`
	DeliveryPackageDigest string `json:"delivery_package_digest"`
}

// KeyRotationMarker is the compact delivery-log leaf for a key transition.
type KeyRotationMarker struct {
	TenantID                    string `json:"tenant_id"`
	IdentityID                  string `json:"identity_id"`
	RotationAuthorizationDigest string `json:"rotation_authorization_digest"`
}

// DeliveryLogReference binds a key-history event to one exact record in the
// tenant delivery log. Index alone is insufficient because it would allow a
// compromised log operator to reinterpret the same position on another fork.
type DeliveryLogReference struct {
	Index uint64 `json:"index"`
	Hash  string `json:"hash"`
}

// KeyEvent is one identity-local key-history event. RotationMarker is assigned
// by the delivery-log sequencer and is therefore deliberately outside the
// client-signed RotationAuthorization.
type KeyEvent struct {
	Kind           string                `json:"kind"`
	Enrollment     *EnrollmentPackage    `json:"enrollment,omitempty"`
	Rotation       *RotationPackage      `json:"rotation,omitempty"`
	RotationMarker *DeliveryLogReference `json:"rotation_marker,omitempty"`
}

// KeyEventRecord is a leaf in one identity's RFC 6962 key-event Merkle log.
type KeyEventRecord struct {
	IdentityID           string   `json:"identity_id"`
	Sequence             uint64   `json:"sequence"`
	Event                KeyEvent `json:"event"`
	ResultingStateDigest string   `json:"resulting_state_digest"`
	Hash                 string   `json:"hash"`
}

// VerifyKeyEventRecord checks the record's RFC 6962 leaf hash and returns the
// validated leaf hash for append-only storage adapters.
func VerifyKeyEventRecord(record KeyEventRecord) ([]byte, error) {
	_, leafHash, err := verifiedKeyEventLeaf(record)
	return leafHash, err
}

// KeyEventInclusionProof authenticates one selectively retrieved identity
// event under a KeyHistoryHead.
type KeyEventInclusionProof struct {
	Event          KeyEventRecord `json:"event"`
	InclusionProof []string       `json:"inclusion_proof"`
}

// ExceptionalEvent is the rare durable semantic state needed when a
// structurally committed event fails validation. Identity and sequence make
// descendant checks possible without retrieving the complete identity log.
type ExceptionalEvent struct {
	IdentityID           string `json:"identity_id"`
	Sequence             uint64 `json:"sequence"`
	EventDigest          string `json:"event_digest"`
	ResultingStateDigest string `json:"resulting_state_digest"`
}

// KeyHistoryHead is the authenticated-map value for a federated identity.
// Root and Size commit the append-only event history; CurrentStateDigest is a
// compact lookup hint whose value is also committed by the latest event.
type KeyHistoryHead struct {
	Protocol           string `json:"protocol"`
	IdentityID         string `json:"identity_id"`
	Size               uint64 `json:"size"`
	Root               string `json:"root"`
	CurrentStateDigest string `json:"current_state_digest,omitempty"`
}

// KeyHistoryUpdate proves that one identity's append-only key-event history
// advances from PreviousHead to Head.
type KeyHistoryUpdate struct {
	PreviousHead     KeyHistoryHead `json:"previous_head"`
	Event            KeyEventRecord `json:"event"`
	Head             KeyHistoryHead `json:"head"`
	InclusionProof   []string       `json:"inclusion_proof"`
	ConsistencyProof []string       `json:"consistency_proof"`
}

// AuthenticatedMapUpdate proves one sparse-map leaf replacement. Recomputing
// PreviousRoot from PreviousHead and SiblingHashes authenticates the old leaf
// against the verifier's retained root. Reusing that exact path with the new
// key-history head produces Root and proves that no other leaf changed.
type AuthenticatedMapUpdate struct {
	PreviousRoot        string                  `json:"previous_root"`
	Root                string                  `json:"root"`
	PreviousHead        *KeyHistoryHead         `json:"previous_head,omitempty"`
	KeyHistory          KeyHistoryUpdate        `json:"key_history"`
	SiblingBitmap       []byte                  `json:"sibling_bitmap"`
	SiblingHashes       []string                `json:"sibling_hashes,omitempty"`
	Predecessor         *KeyEventInclusionProof `json:"predecessor,omitempty"`
	RotationRecord      *DeliveryRecord         `json:"rotation_record,omitempty"`
	MarkerLogCheckpoint *Checkpoint             `json:"marker_log_checkpoint,omitempty"`
	MarkerLogInclusion  []string                `json:"marker_log_inclusion,omitempty"`
}

// KeyHistoryMapProof proves that Head is the current value for one identity
// under a tenant's accepted sparse-map root.
type KeyHistoryMapProof struct {
	Head          KeyHistoryHead `json:"head"`
	SiblingBitmap []byte         `json:"sibling_bitmap"`
	SiblingHashes []string       `json:"sibling_hashes,omitempty"`
}

// IdentityTrustProof puts identity-local storage and proof assembly on the
// manager. A verifier authenticates the current head through Map and
// reconstructs only the signing state plus its immediate successor when that
// state is historical.
type IdentityTrustProof struct {
	Map            KeyHistoryMapProof      `json:"map"`
	SigningEvent   KeyEventInclusionProof  `json:"signing_event"`
	SuccessorEvent *KeyEventInclusionProof `json:"successor_event,omitempty"`
}

type DeliveryLogEvent struct {
	Kind       string              `json:"kind"`
	Commitment *DeliveryCommitment `json:"commitment,omitempty"`
	Marker     *KeyRotationMarker  `json:"marker,omitempty"`
}

// DeliveryRecord is one delivery-log leaf plus the support preimage needed to
// recompute that leaf. Hash covers only the compact Event; Delivery and
// Rotation are not part of the Merkle leaf.
type DeliveryRecord struct {
	Index    uint64           `json:"index"`
	Event    DeliveryLogEvent `json:"event"`
	Hash     string           `json:"hash"`
	Delivery *SignedDelivery  `json:"delivery,omitempty"`
	Rotation *RotationPackage `json:"rotation,omitempty"`
}

// DeliveryLogEntryProof discloses one selected log record and proves its
// inclusion under DeliveryLogUpdate.Checkpoint.
type DeliveryLogEntryProof struct {
	Record         DeliveryRecord `json:"record"`
	InclusionProof []string       `json:"inclusion_proof"`
}

// DeliveryLogUpdate proves an append-only transition from a verifier's
// retained checkpoint and may disclose any selected leaves. Unrelated tenant
// leaves are not sent to that verifier. From is the checkpoint the consistency
// proof was constructed from; it is transport metadata so a manager using a
// stale cached checkpoint can be distinguished from a log fork.
type DeliveryLogUpdate struct {
	From             Checkpoint              `json:"from"`
	Checkpoint       Checkpoint              `json:"checkpoint"`
	ConsistencyProof []string                `json:"consistency_proof"`
	Entries          []DeliveryLogEntryProof `json:"entries,omitempty"`
}

// DeliveryProof contains the evidence a targeted agent needs for one push:
// map catch-up from its retained root, tenant-log position, and identity/key
// provenance.
type DeliveryProof struct {
	MapUpdates []AuthenticatedMapUpdate `json:"map_updates,omitempty"`
	Log        DeliveryLogUpdate        `json:"log"`
	Identity   IdentityTrustProof       `json:"identity"`
}

func NewDeliveryRecord(index uint64, event DeliveryLogEvent) (DeliveryRecord, error) {
	record := DeliveryRecord{
		Index: index,
		Event: event,
	}
	data, err := json.Marshal(deliveryRecordMaterial(record))
	if err != nil {
		return DeliveryRecord{}, fmt.Errorf("encode delivery-log leaf: %w", err)
	}
	record.Hash = encodeHash(rfc6962.DefaultHasher.HashLeaf(data))
	return record, nil
}

// VerifyDeliveryRecord checks the DeliveryRecord's claimed hash matches its data.
func VerifyDeliveryRecord(record DeliveryRecord) ([]byte, error) {
	data, err := json.Marshal(deliveryRecordMaterial(record))
	if err != nil {
		return nil, fmt.Errorf("%w: encode delivery-log leaf: %v", ErrInvalidRecord, err)
	}
	want := rfc6962.DefaultHasher.HashLeaf(data)
	got, err := decodeHash(record.Hash)
	if err != nil {
		return nil, fmt.Errorf("%w: delivery record hash: %v", ErrInvalidRecord, err)
	}
	if !bytes.Equal(got, want) {
		return nil, fmt.Errorf("%w: delivery record hash %q, want %q", ErrInvalidRecord, record.Hash, encodeHash(want))
	}
	return want, nil
}

func (r DeliveryRecord) Reference() DeliveryLogReference {
	return DeliveryLogReference{Index: r.Index, Hash: r.Hash}
}

// VerifyDeliveryLogUpdate verifies the RFC 6962 consistency proof from
// previous to update.Checkpoint and every selectively disclosed inclusion.
func VerifyDeliveryLogUpdate(previous Checkpoint, update DeliveryLogUpdate) error {
	previousRoot, err := validateCheckpoint(previous)
	if err != nil {
		return fmt.Errorf("%w: previous checkpoint: %v", ErrInvalidRecord, err)
	}
	root, err := validateCheckpoint(update.Checkpoint)
	if err != nil {
		return fmt.Errorf("%w: successor checkpoint: %v", ErrInvalidRecord, err)
	}
	if update.Checkpoint.Size < previous.Size {
		return fmt.Errorf("%w: successor size %d is before retained size %d", ErrInvalidRecord, update.Checkpoint.Size, previous.Size)
	}

	consistency, err := decodeProof(update.ConsistencyProof)
	if err != nil {
		return fmt.Errorf("%w: consistency proof: %v", ErrInvalidRecord, err)
	}
	switch previous.Size {
	case 0:
		if len(consistency) != 0 {
			return fmt.Errorf("%w: consistency proof from empty tree must be empty", ErrInvalidRecord)
		}
	case update.Checkpoint.Size:
		if len(consistency) != 0 || !bytes.Equal(previousRoot, root) {
			return fmt.Errorf("%w: equal-size checkpoint changed root or supplied a proof", ErrInvalidRecord)
		}
	default:
		if err := proof.VerifyConsistency(
			rfc6962.DefaultHasher,
			previous.Size,
			update.Checkpoint.Size,
			consistency,
			previousRoot,
			root,
		); err != nil {
			return fmt.Errorf("%w: consistency proof: %v", ErrInvalidRecord, err)
		}
	}

	seen := make(map[uint64]struct{}, len(update.Entries))
	for i, entry := range update.Entries {
		if _, duplicate := seen[entry.Record.Index]; duplicate {
			return fmt.Errorf("%w: duplicate disclosed index %d", ErrInvalidRecord, entry.Record.Index)
		}
		seen[entry.Record.Index] = struct{}{}
		if entry.Record.Index >= update.Checkpoint.Size {
			return fmt.Errorf("%w: disclosed index %d is beyond checkpoint size %d", ErrInvalidRecord, entry.Record.Index, update.Checkpoint.Size)
		}
		leafHash, err := VerifyDeliveryRecord(entry.Record)
		if err != nil {
			return fmt.Errorf("%w: entry %d: %v", ErrInvalidRecord, i, err)
		}
		inclusion, err := decodeProof(entry.InclusionProof)
		if err != nil {
			return fmt.Errorf("%w: entry %d inclusion proof: %v", ErrInvalidRecord, i, err)
		}
		if err := proof.VerifyInclusion(
			rfc6962.DefaultHasher,
			entry.Record.Index,
			update.Checkpoint.Size,
			leafHash,
			inclusion,
			root,
		); err != nil {
			return fmt.Errorf("%w: entry %d inclusion proof: %v", ErrInvalidRecord, i, err)
		}
	}
	return nil
}

func EmptyKeyHistoryHead(identityID string) KeyHistoryHead {
	return KeyHistoryHead{
		Protocol:   KeyHistoryHeadProtocol,
		IdentityID: identityID,
		Root:       EmptyCheckpoint().Root,
	}
}

// NewKeyEventRecord constructs the next identity-local event and its RFC 6962
// leaf hash without loading any prior event bodies. A proof-store adapter can
// append the returned hash to its retained compact frontier, then pass the
// resulting root and proofs to NewKeyHistoryUpdateFromAppend.
func NewKeyEventRecord(previous KeyHistoryHead, resultingStateDigest string, event KeyEvent) (KeyEventRecord, []byte, error) {
	if _, err := validateKeyHistoryHead(previous); err != nil {
		return KeyEventRecord{}, nil, fmt.Errorf("%w: malformed previous head: %v", ErrInvalidKeyHistory, err)
	}
	if resultingStateDigest == "" {
		return KeyEventRecord{}, nil, fmt.Errorf("%w: resulting state digest is required", ErrInvalidKeyHistory)
	}
	if previous.Size == math.MaxUint64 {
		return KeyEventRecord{}, nil, fmt.Errorf("%w: key history is at maximum uint64 size", ErrInvalidKeyHistory)
	}
	record := KeyEventRecord{
		IdentityID:           previous.IdentityID,
		Sequence:             previous.Size,
		Event:                event,
		ResultingStateDigest: resultingStateDigest,
	}
	data, err := json.Marshal(keyEventRecordMaterial(record))
	if err != nil {
		return KeyEventRecord{}, nil, fmt.Errorf("%w: encode key-event leaf: %v", ErrInvalidKeyHistory, err)
	}
	leafHash := rfc6962.DefaultHasher.HashLeaf(data)
	record.Hash = encodeHash(leafHash)
	return record, leafHash, nil
}

// NewKeyHistoryUpdateFromAppend assembles a wire update from one newly stored
// event plus proof-store output. It is the production-shaped constructor: no
// historical event bodies are required, and the supplied inclusion and
// consistency proofs are reverified before the update is returned.
func NewKeyHistoryUpdateFromAppend(previous KeyHistoryHead, record KeyEventRecord, successorRoot []byte, inclusion, consistency [][]byte) (KeyHistoryUpdate, error) {
	if _, err := validateKeyHistoryHead(previous); err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: malformed previous head: %v", ErrInvalidKeyHistory, err)
	}
	if previous.Size == math.MaxUint64 {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: key history is at maximum uint64 size", ErrInvalidKeyHistory)
	}
	checkpoint, err := NewCheckpoint(previous.Size+1, successorRoot)
	if err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: successor root: %v", ErrInvalidKeyHistory, err)
	}
	update := KeyHistoryUpdate{
		PreviousHead: previous,
		Event:        record,
		Head: KeyHistoryHead{
			Protocol:           KeyHistoryHeadProtocol,
			IdentityID:         previous.IdentityID,
			Size:               checkpoint.Size,
			Root:               checkpoint.Root,
			CurrentStateDigest: record.ResultingStateDigest,
		},
		InclusionProof:   encodeProof(inclusion),
		ConsistencyProof: encodeProof(consistency),
	}
	if _, err := VerifyKeyHistoryUpdate(previous, update); err != nil {
		return KeyHistoryUpdate{}, err
	}
	return update, nil
}

// NewKeyHistoryUpdate builds one append to an RFC 6962 per-identity key-event
// log. retainedRecords are the manager's stored prefix; generated proofs, not
// those records, are sent to verifiers. It is a convenience constructor for
// fixtures; managers should retain a proof-store frontier and use
// NewKeyHistoryUpdateFromAppend instead of replaying the prefix.
func NewKeyHistoryUpdate(previous KeyHistoryHead, retainedRecords []KeyEventRecord, resultingStateDigest string, event KeyEvent) (KeyHistoryUpdate, error) {
	if _, err := validateKeyHistoryHead(previous); err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: malformed previous head: %v", ErrInvalidKeyHistory, err)
	}
	if resultingStateDigest == "" {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: resulting state digest is required", ErrInvalidKeyHistory)
	}
	if previous.Size == math.MaxUint64 {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: key history is at maximum uint64 size", ErrInvalidKeyHistory)
	}
	if uint64(len(retainedRecords)) != previous.Size {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: retained history has %d records, previous head has size %d", ErrInvalidKeyHistory, len(retainedRecords), previous.Size)
	}

	tree := merklelog.New()
	for i, retained := range retainedRecords {
		if retained.IdentityID != previous.IdentityID || retained.Sequence != uint64(i) {
			return KeyHistoryUpdate{}, fmt.Errorf("%w: retained key event %d has wrong identity or sequence", ErrInvalidKeyHistory, i)
		}
		data, leafHash, err := verifiedKeyEventLeaf(retained)
		if err != nil {
			return KeyHistoryUpdate{}, err
		}
		index, appendedHash, err := tree.Append(data)
		if err != nil {
			return KeyHistoryUpdate{}, fmt.Errorf("%w: rebuild retained history: %v", ErrInvalidKeyHistory, err)
		}
		if index != retained.Sequence || !bytes.Equal(appendedHash, leafHash) {
			return KeyHistoryUpdate{}, fmt.Errorf("%w: retained key event %d does not match Merkle leaf", ErrInvalidKeyHistory, i)
		}
	}
	previousRoot, err := tree.Root()
	if err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: compute retained history root: %v", ErrInvalidKeyHistory, err)
	}
	acceptedPreviousRoot, err := decodeHash(previous.Root)
	if err != nil || !bytes.Equal(previousRoot, acceptedPreviousRoot) {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: retained records do not reconstruct previous history root", ErrInvalidKeyHistory)
	}

	record, leafHash, err := NewKeyEventRecord(previous, resultingStateDigest, event)
	if err != nil {
		return KeyHistoryUpdate{}, err
	}
	index, appendedHash, err := tree.AppendHash(leafHash)
	if err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: append key-event leaf: %v", ErrInvalidKeyHistory, err)
	}
	if index != record.Sequence || !bytes.Equal(appendedHash, leafHash) {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: key event assigned index %d, want %d", ErrInvalidKeyHistory, index, record.Sequence)
	}
	root, err := tree.Root()
	if err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: compute key-history root: %v", ErrInvalidKeyHistory, err)
	}
	inclusion, err := tree.InclusionProof(record.Sequence, tree.Size())
	if err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: construct key-event inclusion proof: %v", ErrInvalidKeyHistory, err)
	}
	consistency, err := tree.ConsistencyProof(previous.Size, tree.Size())
	if err != nil {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: construct key-history consistency proof: %v", ErrInvalidKeyHistory, err)
	}
	return NewKeyHistoryUpdateFromAppend(previous, record, root, inclusion, consistency)
}

func VerifyKeyHistoryUpdate(previous KeyHistoryHead, update KeyHistoryUpdate) (KeyHistoryHead, error) {
	previousRoot, err := validateKeyHistoryHead(previous)
	if err != nil {
		return previous, fmt.Errorf("%w: accepted head: %v", ErrInvalidKeyHistory, err)
	}
	if update.PreviousHead != previous {
		return previous, fmt.Errorf("%w: update does not start at accepted head", ErrInvalidKeyHistory)
	}
	if previous.Size == math.MaxUint64 {
		return previous, fmt.Errorf("%w: key history is at maximum uint64 size", ErrInvalidKeyHistory)
	}
	record := update.Event
	if record.IdentityID != previous.IdentityID || record.Sequence != previous.Size || record.ResultingStateDigest == "" {
		return previous, fmt.Errorf("%w: key event does not extend accepted head", ErrInvalidKeyHistory)
	}
	_, leafHash, err := verifiedKeyEventLeaf(record)
	if err != nil {
		return previous, err
	}
	wantHead := KeyHistoryHead{
		Protocol:           KeyHistoryHeadProtocol,
		IdentityID:         previous.IdentityID,
		Size:               previous.Size + 1,
		Root:               update.Head.Root,
		CurrentStateDigest: record.ResultingStateDigest,
	}
	if update.Head != wantHead {
		return previous, fmt.Errorf("%w: resulting head does not match key event", ErrInvalidKeyHistory)
	}
	root, err := validateKeyHistoryHead(update.Head)
	if err != nil {
		return previous, fmt.Errorf("%w: successor head: %v", ErrInvalidKeyHistory, err)
	}
	inclusion, err := decodeProof(update.InclusionProof)
	if err != nil {
		return previous, fmt.Errorf("%w: key-event inclusion proof: %v", ErrInvalidKeyHistory, err)
	}
	if err := proof.VerifyInclusion(
		rfc6962.DefaultHasher,
		record.Sequence,
		update.Head.Size,
		leafHash,
		inclusion,
		root,
	); err != nil {
		return previous, fmt.Errorf("%w: key-event inclusion proof: %v", ErrInvalidKeyHistory, err)
	}
	consistency, err := decodeProof(update.ConsistencyProof)
	if err != nil {
		return previous, fmt.Errorf("%w: key-history consistency proof: %v", ErrInvalidKeyHistory, err)
	}
	if previous.Size == 0 {
		if len(consistency) != 0 {
			return previous, fmt.Errorf("%w: consistency proof from empty history must be empty", ErrInvalidKeyHistory)
		}
	} else if err := proof.VerifyConsistency(
		rfc6962.DefaultHasher,
		previous.Size,
		update.Head.Size,
		consistency,
		previousRoot,
		root,
	); err != nil {
		return previous, fmt.Errorf("%w: key-history consistency proof: %v", ErrInvalidKeyHistory, err)
	}
	return wantHead, nil
}

// VerifyKeyHistoryRecords reconstructs the complete identity-local Merkle log
// and verifies that it produces head. It is an audit and fixture helper, not a
// production proof-assembly path.
func VerifyKeyHistoryRecords(head KeyHistoryHead, records []KeyEventRecord) error {
	root, err := validateKeyHistoryHead(head)
	if err != nil {
		return fmt.Errorf("%w: malformed head: %v", ErrInvalidKeyHistory, err)
	}
	if uint64(len(records)) != head.Size {
		return fmt.Errorf("%w: history has %d records, head has size %d", ErrInvalidKeyHistory, len(records), head.Size)
	}
	tree := merklelog.New()
	for i, record := range records {
		if record.IdentityID != head.IdentityID || record.Sequence != uint64(i) {
			return fmt.Errorf("%w: key event %d has wrong identity or sequence", ErrInvalidKeyHistory, i)
		}
		data, leafHash, err := verifiedKeyEventLeaf(record)
		if err != nil {
			return err
		}
		index, appendedHash, err := tree.Append(data)
		if err != nil {
			return fmt.Errorf("%w: rebuild history: %v", ErrInvalidKeyHistory, err)
		}
		if index != record.Sequence || !bytes.Equal(appendedHash, leafHash) {
			return fmt.Errorf("%w: key event %d does not match Merkle leaf", ErrInvalidKeyHistory, i)
		}
	}
	reconstructed, err := tree.Root()
	if err != nil {
		return fmt.Errorf("%w: compute reconstructed root: %v", ErrInvalidKeyHistory, err)
	}
	if !bytes.Equal(reconstructed, root) {
		return fmt.Errorf("%w: records do not reconstruct the accepted history root", ErrInvalidKeyHistory)
	}
	if len(records) == 0 {
		if head.CurrentStateDigest != "" {
			return fmt.Errorf("%w: empty history has a current state digest", ErrInvalidKeyHistory)
		}
		return nil
	}
	if records[len(records)-1].ResultingStateDigest != head.CurrentStateDigest {
		return fmt.Errorf("%w: final event does not produce the head's current state", ErrInvalidKeyHistory)
	}
	return nil
}

// NewKeyEventInclusionProof is a fixture convenience that rebuilds a retained
// history before selecting one event with logarithmic authentication to
// head.Root. Production managers should use indexed event and Merkle-node
// reads, as resourcemanager.Manager does.
func NewKeyEventInclusionProof(head KeyHistoryHead, records []KeyEventRecord, sequence uint64) (KeyEventInclusionProof, error) {
	if err := VerifyKeyHistoryRecords(head, records); err != nil {
		return KeyEventInclusionProof{}, err
	}
	if sequence >= head.Size {
		return KeyEventInclusionProof{}, fmt.Errorf("%w: key-event sequence %d is beyond history size %d", ErrInvalidKeyHistory, sequence, head.Size)
	}
	tree := merklelog.New()
	for _, record := range records {
		data, _, err := verifiedKeyEventLeaf(record)
		if err != nil {
			return KeyEventInclusionProof{}, err
		}
		if _, _, err := tree.Append(data); err != nil {
			return KeyEventInclusionProof{}, fmt.Errorf("%w: rebuild history for inclusion proof: %v", ErrInvalidKeyHistory, err)
		}
	}
	inclusion, err := tree.InclusionProof(sequence, head.Size)
	if err != nil {
		return KeyEventInclusionProof{}, fmt.Errorf("%w: construct key-event inclusion proof: %v", ErrInvalidKeyHistory, err)
	}
	return KeyEventInclusionProof{
		Event:          cloneKeyEventRecord(records[sequence]),
		InclusionProof: encodeProof(inclusion),
	}, nil
}

// VerifyKeyEventInclusionProof authenticates one selectively disclosed event
// against an accepted identity history head.
func VerifyKeyEventInclusionProof(head KeyHistoryHead, eventProof KeyEventInclusionProof) error {
	root, err := validateKeyHistoryHead(head)
	if err != nil {
		return fmt.Errorf("%w: malformed head: %v", ErrInvalidKeyHistory, err)
	}
	record := eventProof.Event
	if record.IdentityID != head.IdentityID || record.Sequence >= head.Size {
		return fmt.Errorf("%w: selected event does not belong to accepted history", ErrInvalidKeyHistory)
	}
	_, leafHash, err := verifiedKeyEventLeaf(record)
	if err != nil {
		return err
	}
	inclusion, err := decodeProof(eventProof.InclusionProof)
	if err != nil {
		return fmt.Errorf("%w: selected-event inclusion proof: %v", ErrInvalidKeyHistory, err)
	}
	if err := proof.VerifyInclusion(
		rfc6962.DefaultHasher,
		record.Sequence,
		head.Size,
		leafHash,
		inclusion,
		root,
	); err != nil {
		return fmt.Errorf("%w: selected-event inclusion proof: %v", ErrInvalidKeyHistory, err)
	}
	return nil
}

// NewAuthenticatedMapUpdate constructs the CONIKS sparse-map proof for
// replacing the one identity leaf changed by historyUpdate. It rebuilds the
// map from heads as a fixture convenience; production-shaped managers should
// retain a versioned sparse-node store and call
// NewAuthenticatedMapUpdateFromProof.
func NewAuthenticatedMapUpdate(tenantID string, heads map[string]KeyHistoryHead, historyUpdate KeyHistoryUpdate) (AuthenticatedMapUpdate, error) {
	if tenantID == "" {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: tenant is required", ErrInvalidMapProof)
	}
	identityID := historyUpdate.Event.IdentityID
	previousHead, exists := heads[identityID]
	if exists {
		if previousHead != historyUpdate.PreviousHead {
			return AuthenticatedMapUpdate{}, fmt.Errorf("%w: map leaf does not match previous key-history head", ErrInvalidKeyHistory)
		}
	} else if historyUpdate.PreviousHead != EmptyKeyHistoryHead(identityID) {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: absent map leaf does not start at empty key history", ErrInvalidKeyHistory)
	}
	if _, err := VerifyKeyHistoryUpdate(historyUpdate.PreviousHead, historyUpdate); err != nil {
		return AuthenticatedMapUpdate{}, err
	}

	tree, err := buildKeyHistoryMap(tenantID, heads)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	key := keyHistoryMapKey(tenantID, identityID)
	siblingBitmap, siblingHashes, err := tree.CompressedProof(key[:])
	if err != nil {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: construct sparse-map proof: %v", ErrInvalidMapProof, err)
	}
	var previousLeaf *KeyHistoryHead
	if exists {
		previous := previousHead
		previousLeaf = &previous
	}
	update, err := NewAuthenticatedMapUpdateFromProof(
		tenantID,
		encodeHash(tree.Root()),
		previousLeaf,
		historyUpdate,
		siblingBitmap,
		siblingHashes,
	)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	nextValueHash, err := keyHistoryMapValueHash(historyUpdate.Head)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	root, err := tree.Set(key[:], nextValueHash)
	if err != nil {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: write successor sparse-map leaf: %v", ErrInvalidMapProof, err)
	}
	claimedRoot, err := decodeHash(update.Root)
	if err != nil || !bytes.Equal(claimedRoot, root) {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: sparse-map writer and replacement proof produced different roots", ErrInvalidMapProof)
	}
	return update, nil
}

// NewAuthenticatedMapUpdateFromProof assembles and revalidates a one-leaf map
// replacement from a retained compressed sibling path. It requires no other
// identities' heads and is the intended proof-store boundary for the server.
func NewAuthenticatedMapUpdateFromProof(tenantID, currentRoot string, previousHead *KeyHistoryHead, historyUpdate KeyHistoryUpdate, siblingBitmap []byte, siblingHashes [][]byte) (AuthenticatedMapUpdate, error) {
	if tenantID == "" {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: tenant is required", ErrInvalidMapProof)
	}
	identityID := historyUpdate.Event.IdentityID
	if previousHead == nil {
		if historyUpdate.PreviousHead != EmptyKeyHistoryHead(identityID) {
			return AuthenticatedMapUpdate{}, fmt.Errorf("%w: absent map leaf does not start at empty key history", ErrInvalidKeyHistory)
		}
	} else if *previousHead != historyUpdate.PreviousHead {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: previous map leaf and key-history head differ", ErrInvalidKeyHistory)
	}
	if _, err := VerifyKeyHistoryUpdate(historyUpdate.PreviousHead, historyUpdate); err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	acceptedRoot, err := decodeHash(currentRoot)
	if err != nil {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: retained map root: %v", ErrInvalidMapProof, err)
	}
	key := keyHistoryMapKey(tenantID, identityID)
	var previousValueHash []byte
	if previousHead != nil {
		previousValueHash, err = keyHistoryMapValueHash(*previousHead)
		if err != nil {
			return AuthenticatedMapUpdate{}, err
		}
	}
	provedPreviousRoot, err := sparsemap.RootFromCompressedProof(tenantID, key[:], previousValueHash, siblingBitmap, siblingHashes)
	if err != nil {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: reconstruct previous sparse-map root: %v", ErrInvalidMapProof, err)
	}
	if !bytes.Equal(provedPreviousRoot, acceptedRoot) {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: previous leaf proof does not reconstruct the retained map root", ErrInvalidMapProof)
	}
	nextValueHash, err := keyHistoryMapValueHash(historyUpdate.Head)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	provedRoot, err := sparsemap.RootFromCompressedProof(tenantID, key[:], nextValueHash, siblingBitmap, siblingHashes)
	if err != nil {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: reconstruct successor sparse-map root: %v", ErrInvalidMapProof, err)
	}
	var previousLeaf *KeyHistoryHead
	if previousHead != nil {
		copy := *previousHead
		previousLeaf = &copy
	}
	return AuthenticatedMapUpdate{
		PreviousRoot:  currentRoot,
		Root:          encodeHash(provedRoot),
		PreviousHead:  previousLeaf,
		KeyHistory:    historyUpdate,
		SiblingBitmap: append([]byte(nil), siblingBitmap...),
		SiblingHashes: encodeProof(siblingHashes),
	}, nil
}

// VerifyAuthenticatedMapUpdate verifies both proof layers: the identity-local
// key history extends its accepted head, and replacing only that leaf on the
// supplied sparse-map path transforms currentRoot into update.Root.
func VerifyAuthenticatedMapUpdate(tenantID, currentRoot string, update AuthenticatedMapUpdate) (KeyHistoryHead, error) {
	if tenantID == "" {
		return KeyHistoryHead{}, fmt.Errorf("%w: tenant is required", ErrInvalidMapProof)
	}
	if update.PreviousRoot != currentRoot {
		return KeyHistoryHead{}, fmt.Errorf("%w: map update starts at root %q, want %q", ErrInvalidMapProof, update.PreviousRoot, currentRoot)
	}
	identityID := update.KeyHistory.Event.IdentityID
	previousHistoryHead := EmptyKeyHistoryHead(identityID)
	if update.PreviousHead != nil {
		previousHistoryHead = *update.PreviousHead
	}
	if previousHistoryHead != update.KeyHistory.PreviousHead {
		return KeyHistoryHead{}, fmt.Errorf("%w: previous map leaf and key-history head differ", ErrInvalidKeyHistory)
	}
	nextHead, err := VerifyKeyHistoryUpdate(previousHistoryHead, update.KeyHistory)
	if err != nil {
		return KeyHistoryHead{}, err
	}
	acceptedRoot, err := decodeHash(currentRoot)
	if err != nil {
		return KeyHistoryHead{}, fmt.Errorf("%w: retained map root: %v", ErrInvalidMapProof, err)
	}
	siblings, err := decodeProof(update.SiblingHashes)
	if err != nil {
		return KeyHistoryHead{}, fmt.Errorf("%w: sibling path: %v", ErrInvalidMapProof, err)
	}
	key := keyHistoryMapKey(tenantID, identityID)
	var previousValueHash []byte
	if update.PreviousHead != nil {
		previousValueHash, err = keyHistoryMapValueHash(*update.PreviousHead)
		if err != nil {
			return KeyHistoryHead{}, err
		}
	}
	computedPreviousRoot, err := sparsemap.RootFromCompressedProof(tenantID, key[:], previousValueHash, update.SiblingBitmap, siblings)
	if err != nil {
		return KeyHistoryHead{}, fmt.Errorf("%w: previous leaf proof: %v", ErrInvalidMapProof, err)
	}
	if !bytes.Equal(computedPreviousRoot, acceptedRoot) {
		return KeyHistoryHead{}, fmt.Errorf("%w: previous leaf proof does not produce the retained root", ErrInvalidMapProof)
	}
	nextValueHash, err := keyHistoryMapValueHash(nextHead)
	if err != nil {
		return KeyHistoryHead{}, err
	}
	computedRoot, err := sparsemap.RootFromCompressedProof(tenantID, key[:], nextValueHash, update.SiblingBitmap, siblings)
	if err != nil {
		return KeyHistoryHead{}, fmt.Errorf("%w: successor leaf proof: %v", ErrInvalidMapProof, err)
	}
	claimedRoot, err := decodeHash(update.Root)
	if err != nil {
		return KeyHistoryHead{}, fmt.Errorf("%w: successor map root: %v", ErrInvalidMapProof, err)
	}
	if !bytes.Equal(computedRoot, claimedRoot) {
		return KeyHistoryHead{}, fmt.Errorf("%w: successor leaf proof does not produce claimed root", ErrInvalidMapProof)
	}
	return nextHead, nil
}

// KeyHistoryMapRoot computes the tenant-separated CONIKS sparse-map root.
func KeyHistoryMapRoot(tenantID string, heads map[string]KeyHistoryHead) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("%w: tenant is required", ErrInvalidMapProof)
	}
	tree, err := buildKeyHistoryMap(tenantID, heads)
	if err != nil {
		return "", err
	}
	return encodeHash(tree.Root()), nil
}

// NewKeyHistoryMapProof constructs a membership proof for identityID's
// current key-history head. The manager retains heads; the verifier retains
// only the root against which this proof is checked. It rebuilds the map as a
// fixture convenience; managers with a versioned node store should call
// NewKeyHistoryMapProofFromProof.
func NewKeyHistoryMapProof(tenantID string, heads map[string]KeyHistoryHead, identityID string) (KeyHistoryMapProof, error) {
	head, ok := heads[identityID]
	if !ok {
		return KeyHistoryMapProof{}, fmt.Errorf("%w: identity %q has no key-history head", ErrInvalidMapProof, identityID)
	}
	tree, err := buildKeyHistoryMap(tenantID, heads)
	if err != nil {
		return KeyHistoryMapProof{}, err
	}
	key := keyHistoryMapKey(tenantID, identityID)
	bitmap, siblings, err := tree.CompressedProof(key[:])
	if err != nil {
		return KeyHistoryMapProof{}, fmt.Errorf("%w: construct membership proof: %v", ErrInvalidMapProof, err)
	}
	return NewKeyHistoryMapProofFromProof(tenantID, encodeHash(tree.Root()), head, bitmap, siblings)
}

// NewKeyHistoryMapProofFromProof assembles and validates one membership proof
// returned by a versioned sparse-node store. No other identity leaf is loaded.
func NewKeyHistoryMapProofFromProof(tenantID, currentRoot string, head KeyHistoryHead, siblingBitmap []byte, siblingHashes [][]byte) (KeyHistoryMapProof, error) {
	mapProof := KeyHistoryMapProof{
		Head:          head,
		SiblingBitmap: append([]byte(nil), siblingBitmap...),
		SiblingHashes: encodeProof(siblingHashes),
	}
	if err := VerifyKeyHistoryMapProof(tenantID, currentRoot, mapProof); err != nil {
		return KeyHistoryMapProof{}, err
	}
	return mapProof, nil
}

// VerifyKeyHistoryMapProof authenticates one current identity head without
// requiring the verifier to retain any other identity's leaf.
func VerifyKeyHistoryMapProof(tenantID, currentRoot string, mapProof KeyHistoryMapProof) error {
	acceptedRoot, err := decodeHash(currentRoot)
	if err != nil {
		return fmt.Errorf("%w: retained map root: %v", ErrInvalidMapProof, err)
	}
	if _, err := validateKeyHistoryHead(mapProof.Head); err != nil {
		return fmt.Errorf("%w: malformed key-history head: %v", ErrInvalidMapProof, err)
	}
	siblings, err := decodeProof(mapProof.SiblingHashes)
	if err != nil {
		return fmt.Errorf("%w: sibling path: %v", ErrInvalidMapProof, err)
	}
	key := keyHistoryMapKey(tenantID, mapProof.Head.IdentityID)
	valueHash, err := keyHistoryMapValueHash(mapProof.Head)
	if err != nil {
		return err
	}
	computed, err := sparsemap.RootFromCompressedProof(tenantID, key[:], valueHash, mapProof.SiblingBitmap, siblings)
	if err != nil {
		return fmt.Errorf("%w: membership proof: %v", ErrInvalidMapProof, err)
	}
	if !bytes.Equal(computed, acceptedRoot) {
		return fmt.Errorf("%w: membership proof does not produce retained root", ErrInvalidMapProof)
	}
	return nil
}

func buildKeyHistoryMap(tenantID string, heads map[string]KeyHistoryHead) (*sparsemap.Tree, error) {
	tree := sparsemap.New(tenantID)
	for identityID, head := range heads {
		if identityID == "" || head.IdentityID != identityID {
			return nil, fmt.Errorf("%w: malformed key-history map leaf for identity %q", ErrInvalidMapProof, identityID)
		}
		if _, err := validateKeyHistoryHead(head); err != nil {
			return nil, fmt.Errorf("%w: malformed key-history map leaf for identity %q: %v", ErrInvalidMapProof, identityID, err)
		}
		key := keyHistoryMapKey(tenantID, identityID)
		valueHash, err := keyHistoryMapValueHash(head)
		if err != nil {
			return nil, err
		}
		if _, err := tree.Set(key[:], valueHash); err != nil {
			return nil, fmt.Errorf("%w: write key-history map leaf for %q: %v", ErrInvalidMapProof, identityID, err)
		}
	}
	return tree, nil
}

// KeyHistoryMapKey returns the tenant-separated 32-byte sparse-map key for an
// identity. It is exported for proof-store adapters; callers should treat the
// returned bytes as opaque.
func KeyHistoryMapKey(tenantID, identityID string) []byte {
	key := keyHistoryMapKey(tenantID, identityID)
	return append([]byte(nil), key[:]...)
}

// KeyHistoryMapValueHash returns the canonical 32-byte sparse-map value for a
// key-history head. It is exported for proof-store adapters.
func KeyHistoryMapValueHash(head KeyHistoryHead) ([]byte, error) {
	if _, err := validateKeyHistoryHead(head); err != nil {
		return nil, fmt.Errorf("%w: malformed key-history map value: %v", ErrInvalidMapProof, err)
	}
	return keyHistoryMapValueHash(head)
}

func keyHistoryMapValueHash(head KeyHistoryHead) ([]byte, error) {
	material := struct {
		Protocol string         `json:"protocol"`
		Head     KeyHistoryHead `json:"head"`
	}{
		Protocol: "fleetshift.dev/trust-v3/key-history-map-value/v1",
		Head:     head,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return nil, fmt.Errorf("%w: encode key-history map value: %v", ErrInvalidMapProof, err)
	}
	digest := sha256.Sum256(encoded)
	return digest[:], nil
}

func keyHistoryMapKey(tenantID, identityID string) [sha256.Size]byte {
	material := struct {
		Protocol   string `json:"protocol"`
		TenantID   string `json:"tenant_id"`
		IdentityID string `json:"identity_id"`
	}{
		Protocol:   "fleetshift.dev/trust-v3/key-history-map-key/v1",
		TenantID:   tenantID,
		IdentityID: identityID,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		panic(err) // A fixed struct containing strings is always JSON encodable.
	}
	return sha256.Sum256(encoded)
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
		Protocol: IdentityProtocol,
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
	return encodeHash(digest[:])
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

func deliveryRecordMaterial(record DeliveryRecord) any {
	return struct {
		Protocol string           `json:"protocol"`
		Event    DeliveryLogEvent `json:"event"`
	}{"fleetshift.dev/trust-v3/delivery-log-leaf/v1", record.Event}
}

func keyEventRecordMaterial(record KeyEventRecord) any {
	return struct {
		Protocol             string   `json:"protocol"`
		IdentityID           string   `json:"identity_id"`
		Sequence             uint64   `json:"sequence"`
		Event                KeyEvent `json:"event"`
		ResultingStateDigest string   `json:"resulting_state_digest"`
	}{"fleetshift.dev/trust-v3/key-history-leaf/v1", record.IdentityID, record.Sequence, record.Event, record.ResultingStateDigest}
}

func cloneKeyEventRecord(in KeyEventRecord) KeyEventRecord {
	out := in
	out.Event = in.Event
	if in.Event.Enrollment != nil {
		enrollment := *in.Event.Enrollment
		enrollment.ContinuityPublicKey = append([]byte(nil), in.Event.Enrollment.ContinuityPublicKey...)
		enrollment.ProofOfPossession = append([]byte(nil), in.Event.Enrollment.ProofOfPossession...)
		out.Event.Enrollment = &enrollment
	}
	if in.Event.Rotation != nil {
		rotation := *in.Event.Rotation
		rotation.NewContinuityPublicKey = append([]byte(nil), in.Event.Rotation.NewContinuityPublicKey...)
		rotation.SignatureByOldKey = append([]byte(nil), in.Event.Rotation.SignatureByOldKey...)
		rotation.ProofByNewKey = append([]byte(nil), in.Event.Rotation.ProofByNewKey...)
		out.Event.Rotation = &rotation
	}
	if in.Event.RotationMarker != nil {
		marker := *in.Event.RotationMarker
		out.Event.RotationMarker = &marker
	}
	return out
}

func verifiedKeyEventLeaf(record KeyEventRecord) ([]byte, []byte, error) {
	data, err := json.Marshal(keyEventRecordMaterial(record))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: encode key-event leaf: %v", ErrInvalidKeyHistory, err)
	}
	want := rfc6962.DefaultHasher.HashLeaf(data)
	got, err := decodeHash(record.Hash)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: key-event leaf hash: %v", ErrInvalidKeyHistory, err)
	}
	if !bytes.Equal(got, want) {
		return nil, nil, fmt.Errorf("%w: key-event leaf hash %q, want %q", ErrInvalidKeyHistory, record.Hash, encodeHash(want))
	}
	return data, want, nil
}

func validateCheckpoint(checkpoint Checkpoint) ([]byte, error) {
	root, err := decodeHash(checkpoint.Root)
	if err != nil {
		return nil, err
	}
	if checkpoint.Size == 0 && !bytes.Equal(root, rfc6962.DefaultHasher.EmptyRoot()) {
		return nil, errors.New("size-zero checkpoint does not use the RFC 6962 empty root")
	}
	return root, nil
}

func validateKeyHistoryHead(head KeyHistoryHead) ([]byte, error) {
	if head.Protocol != KeyHistoryHeadProtocol || head.IdentityID == "" {
		return nil, errors.New("protocol or identity is missing")
	}
	root, err := validateCheckpoint(Checkpoint{Size: head.Size, Root: head.Root})
	if err != nil {
		return nil, err
	}
	if head.Size == 0 && head.CurrentStateDigest != "" {
		return nil, errors.New("empty key history has a current state")
	}
	if head.Size > 0 && head.CurrentStateDigest == "" {
		return nil, errors.New("non-empty key history has no current state")
	}
	return root, nil
}

func encodeHash(hash []byte) string {
	return "sha256:" + hex.EncodeToString(hash)
}

// EncodeHash returns the canonical wire encoding for a SHA-256 hash produced
// by a retained proof store.
func EncodeHash(hash []byte) (string, error) {
	if len(hash) != sha256.Size {
		return "", fmt.Errorf("SHA-256 hash has length %d, want %d", len(hash), sha256.Size)
	}
	return encodeHash(hash), nil
}

func decodeHash(encoded string) ([]byte, error) {
	if len(encoded) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(encoded, "sha256:") {
		return nil, fmt.Errorf("hash %q is not a SHA-256 digest", encoded)
	}
	hash, err := hex.DecodeString(strings.TrimPrefix(encoded, "sha256:"))
	if err != nil {
		return nil, fmt.Errorf("decode SHA-256 digest: %w", err)
	}
	if encodeHash(hash) != encoded {
		return nil, errors.New("SHA-256 digest is not in canonical lowercase form")
	}
	return hash, nil
}

func encodeProof(hashes [][]byte) []string {
	proof := make([]string, len(hashes))
	for i, hash := range hashes {
		proof[i] = encodeHash(hash)
	}
	return proof
}

// EncodeProof returns the canonical wire encoding for Merkle proof hashes.
func EncodeProof(hashes [][]byte) []string {
	return encodeProof(hashes)
}

func decodeProof(encoded []string) ([][]byte, error) {
	hashes := make([][]byte, len(encoded))
	for i, value := range encoded {
		hash, err := decodeHash(value)
		if err != nil {
			return nil, fmt.Errorf("hash %d: %w", i, err)
		}
		hashes[i] = hash
	}
	return hashes, nil
}
