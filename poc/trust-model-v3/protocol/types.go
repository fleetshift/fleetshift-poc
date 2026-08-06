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
	KeyHistoryHeadProtocol  = "fleetshift.dev/trust-v3/key-history-head/v1"
	KeyHistoryMapProtocol   = "fleetshift.dev/trust-v3/key-history-map/v1"

	KeyEventEnrollment = "enrollment"
	KeyEventRotation   = "rotation"

	DeliveryLogEventDelivery = "delivery"
	DeliveryLogEventRotation = "rotation"

	ActionPut    = "put"
	ActionRemove = "remove"

	GenesisHash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
)

var (
	ErrInvalidRecord     = errors.New("invalid hash-chain record")
	ErrInvalidKeyHistory = errors.New("invalid key history")
	ErrInvalidMapProof   = errors.New("invalid authenticated-map proof")
)

const keyHistoryMapDepth = sha256.Size * 8

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
	IdentityID          string           `json:"identity_id"`
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
	Protocol               string `json:"protocol"`
	TenantID               string `json:"tenant_id"`
	IdentityID             string `json:"identity_id"`
	PreviousStateDigest    string `json:"previous_state_digest"`
	NewGeneration          uint64 `json:"new_generation"`
	NewContinuityKeyDigest string `json:"new_continuity_key_digest"`
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

// DeliveryLogReference binds a key-history event to one exact record in the
// tenant delivery log. Index alone is insufficient because it would allow a
// compromised log operator to reinterpret the same position on another fork.
type DeliveryLogReference struct {
	Index uint64 `json:"index"`
	Hash  string `json:"hash"`
}

// KeyEvent is one identity-local key-history event. RotationMarker is assigned
// by the delivery-log sequencer and is therefore deliberately outside the
// client-signed RotationIntent.
type KeyEvent struct {
	Kind           string                `json:"kind"`
	Enrollment     *EnrollmentPackage    `json:"enrollment,omitempty"`
	Rotation       *RotationPackage      `json:"rotation,omitempty"`
	RotationMarker *DeliveryLogReference `json:"rotation_marker,omitempty"`
}

// KeyEventRecord is a record in one identity's append-only key-event history.
// Its hash commits both to the predecessor event and to the resulting key
// state selected by the authenticated map.
type KeyEventRecord struct {
	IdentityID           string   `json:"identity_id"`
	Sequence             uint64   `json:"sequence"`
	PreviousHash         string   `json:"previous_hash"`
	Event                KeyEvent `json:"event"`
	ResultingStateDigest string   `json:"resulting_state_digest"`
	Hash                 string   `json:"hash"`
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
	PreviousHead KeyHistoryHead `json:"previous_head"`
	Event        KeyEventRecord `json:"event"`
	Head         KeyHistoryHead `json:"head"`
}

// AuthenticatedMapUpdate proves one sparse-map leaf replacement. Recomputing
// PreviousRoot from PreviousHead and SiblingHashes authenticates the old leaf
// against the verifier's retained root. Reusing that exact path with the new
// key-history head produces Root and proves that no other leaf changed.
type AuthenticatedMapUpdate struct {
	PreviousRoot  string           `json:"previous_root"`
	Root          string           `json:"root"`
	PreviousHead  *KeyHistoryHead  `json:"previous_head,omitempty"`
	KeyHistory    KeyHistoryUpdate `json:"key_history"`
	SiblingHashes []string         `json:"sibling_hashes"`
}

type DeliveryLogEvent struct {
	Kind     string           `json:"kind"`
	Delivery *SignedDelivery  `json:"delivery,omitempty"`
	Rotation *RotationPackage `json:"rotation,omitempty"`
}

type DeliveryRecord struct {
	Index        uint64           `json:"index"`
	PreviousHash string           `json:"previous_hash"`
	Event        DeliveryLogEvent `json:"event"`
	Hash         string           `json:"hash"`
}

func NewDeliveryRecord(previous Checkpoint, event DeliveryLogEvent) (DeliveryRecord, error) {
	record := DeliveryRecord{
		Index:        previous.Size,
		PreviousHash: previous.Root,
		Event:        event,
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

func (r DeliveryRecord) Checkpoint() Checkpoint {
	return Checkpoint{Size: r.Index + 1, Root: r.Hash}
}

func (r DeliveryRecord) Reference() DeliveryLogReference {
	return DeliveryLogReference{Index: r.Index, Hash: r.Hash}
}

func EmptyKeyHistoryHead(identityID string) KeyHistoryHead {
	return KeyHistoryHead{
		Protocol:   KeyHistoryHeadProtocol,
		IdentityID: identityID,
		Root:       GenesisHash,
	}
}

func NewKeyHistoryUpdate(previous KeyHistoryHead, resultingStateDigest string, event KeyEvent) (KeyHistoryUpdate, error) {
	if previous.Protocol != KeyHistoryHeadProtocol || previous.IdentityID == "" || previous.Root == "" {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: malformed previous head", ErrInvalidKeyHistory)
	}
	if resultingStateDigest == "" {
		return KeyHistoryUpdate{}, fmt.Errorf("%w: resulting state digest is required", ErrInvalidKeyHistory)
	}
	record := KeyEventRecord{
		IdentityID:           previous.IdentityID,
		Sequence:             previous.Size,
		PreviousHash:         previous.Root,
		Event:                event,
		ResultingStateDigest: resultingStateDigest,
	}
	hash, err := ObjectDigest(keyEventRecordMaterial(record))
	if err != nil {
		return KeyHistoryUpdate{}, err
	}
	record.Hash = hash
	head := KeyHistoryHead{
		Protocol:           KeyHistoryHeadProtocol,
		IdentityID:         previous.IdentityID,
		Size:               previous.Size + 1,
		Root:               hash,
		CurrentStateDigest: resultingStateDigest,
	}
	return KeyHistoryUpdate{PreviousHead: previous, Event: record, Head: head}, nil
}

func VerifyKeyHistoryUpdate(previous KeyHistoryHead, update KeyHistoryUpdate) (KeyHistoryHead, error) {
	if update.PreviousHead != previous {
		return previous, fmt.Errorf("%w: update does not start at accepted head", ErrInvalidKeyHistory)
	}
	record := update.Event
	if record.IdentityID != previous.IdentityID || record.Sequence != previous.Size || record.PreviousHash != previous.Root {
		return previous, fmt.Errorf("%w: key event does not extend accepted head", ErrInvalidKeyHistory)
	}
	wantHash, err := ObjectDigest(keyEventRecordMaterial(record))
	if err != nil {
		return previous, err
	}
	if record.Hash != wantHash {
		return previous, fmt.Errorf("%w: key event hash %q, want %q", ErrInvalidKeyHistory, record.Hash, wantHash)
	}
	wantHead := KeyHistoryHead{
		Protocol:           KeyHistoryHeadProtocol,
		IdentityID:         previous.IdentityID,
		Size:               previous.Size + 1,
		Root:               record.Hash,
		CurrentStateDigest: record.ResultingStateDigest,
	}
	if update.Head != wantHead {
		return previous, fmt.Errorf("%w: resulting head does not match key event", ErrInvalidKeyHistory)
	}
	return wantHead, nil
}

// NewAuthenticatedMapUpdate constructs the sparse-map proof for replacing the
// one identity leaf changed by historyUpdate.
func NewAuthenticatedMapUpdate(heads map[string]KeyHistoryHead, historyUpdate KeyHistoryUpdate) (AuthenticatedMapUpdate, error) {
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

	previousRoot, siblingHashes, provenHead, err := keyHistoryMapProof(heads, identityID)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	if exists {
		if provenHead == nil || *provenHead != previousHead {
			return AuthenticatedMapUpdate{}, fmt.Errorf("%w: sparse-map proof returned the wrong previous head", ErrInvalidKeyHistory)
		}
	} else if provenHead != nil {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: sparse-map absence proof returned a head", ErrInvalidKeyHistory)
	}
	var previousLeaf *KeyHistoryHead
	if exists {
		previous := previousHead
		previousLeaf = &previous
	}
	verifiedPreviousRoot, err := keyHistoryMapRootFromProof(identityID, previousLeaf, siblingHashes)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	if verifiedPreviousRoot != previousRoot {
		return AuthenticatedMapUpdate{}, fmt.Errorf("%w: generated previous leaf proof does not reconstruct the map root", ErrInvalidMapProof)
	}
	root, err := keyHistoryMapRootFromProof(identityID, &historyUpdate.Head, siblingHashes)
	if err != nil {
		return AuthenticatedMapUpdate{}, err
	}
	update := AuthenticatedMapUpdate{
		PreviousRoot:  previousRoot,
		Root:          root,
		KeyHistory:    historyUpdate,
		PreviousHead:  previousLeaf,
		SiblingHashes: siblingHashes,
	}
	return update, nil
}

// VerifyAuthenticatedMapUpdate verifies both proof layers: the identity-local
// key history extends its accepted head, and replacing only that leaf on the
// supplied sparse-map path transforms currentRoot into update.Root.
func VerifyAuthenticatedMapUpdate(currentRoot string, update AuthenticatedMapUpdate) (KeyHistoryHead, error) {
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
	computedPreviousRoot, err := keyHistoryMapRootFromProof(identityID, update.PreviousHead, update.SiblingHashes)
	if err != nil {
		return KeyHistoryHead{}, err
	}
	if computedPreviousRoot != currentRoot {
		return KeyHistoryHead{}, fmt.Errorf("%w: previous leaf proof produces root %q, want %q", ErrInvalidMapProof, computedPreviousRoot, currentRoot)
	}
	computedRoot, err := keyHistoryMapRootFromProof(identityID, &nextHead, update.SiblingHashes)
	if err != nil {
		return KeyHistoryHead{}, err
	}
	if computedRoot != update.Root {
		return KeyHistoryHead{}, fmt.Errorf("%w: successor leaf proof produces root %q, want %q", ErrInvalidMapProof, computedRoot, update.Root)
	}
	return nextHead, nil
}

// KeyHistoryMapRoot computes the root of the sparse authenticated map. The POC
// rebuilds it in memory; verifiers update roots from compact sibling paths.
func KeyHistoryMapRoot(heads map[string]KeyHistoryHead) (string, error) {
	entries, err := keyHistoryMapEntries(heads)
	if err != nil {
		return "", err
	}
	emptyHashes, err := emptyKeyHistoryMapHashes()
	if err != nil {
		return "", err
	}
	return sparseMapRoot(entries, 0, emptyHashes)
}

type keyHistoryMapEntry struct {
	identityID string
	key        [sha256.Size]byte
	head       KeyHistoryHead
}

func keyHistoryMapEntries(heads map[string]KeyHistoryHead) ([]keyHistoryMapEntry, error) {
	entries := make([]keyHistoryMapEntry, 0, len(heads))
	for identityID, head := range heads {
		if identityID == "" || head.Protocol != KeyHistoryHeadProtocol || head.IdentityID != identityID || head.Root == "" {
			return nil, fmt.Errorf("%w: malformed key-history map leaf for identity %q", ErrInvalidMapProof, identityID)
		}
		entries = append(entries, keyHistoryMapEntry{
			identityID: identityID,
			key:        keyHistoryMapKey(identityID),
			head:       head,
		})
	}
	return entries, nil
}

func keyHistoryMapProof(heads map[string]KeyHistoryHead, identityID string) (string, []string, *KeyHistoryHead, error) {
	if identityID == "" {
		return "", nil, nil, fmt.Errorf("%w: identity is required", ErrInvalidMapProof)
	}
	entries, err := keyHistoryMapEntries(heads)
	if err != nil {
		return "", nil, nil, err
	}
	emptyHashes, err := emptyKeyHistoryMapHashes()
	if err != nil {
		return "", nil, nil, err
	}
	root, siblingHashes, err := sparseMapProof(entries, keyHistoryMapKey(identityID), 0, emptyHashes)
	if err != nil {
		return "", nil, nil, err
	}
	if head, ok := heads[identityID]; ok {
		return root, siblingHashes, &head, nil
	}
	return root, siblingHashes, nil, nil
}

func keyHistoryMapRootFromProof(identityID string, head *KeyHistoryHead, siblingHashes []string) (string, error) {
	if identityID == "" {
		return "", fmt.Errorf("%w: identity is required", ErrInvalidMapProof)
	}
	if len(siblingHashes) != keyHistoryMapDepth {
		return "", fmt.Errorf("%w: sibling path has %d hashes, want %d", ErrInvalidMapProof, len(siblingHashes), keyHistoryMapDepth)
	}
	emptyHashes, err := emptyKeyHistoryMapHashes()
	if err != nil {
		return "", err
	}
	current := emptyHashes[0]
	if head != nil {
		if head.Protocol != KeyHistoryHeadProtocol || head.IdentityID != identityID || head.Root == "" {
			return "", fmt.Errorf("%w: malformed key-history map leaf", ErrInvalidMapProof)
		}
		current, err = keyHistoryMapLeafHash(identityID, *head)
		if err != nil {
			return "", err
		}
	}

	key := keyHistoryMapKey(identityID)
	for proofIndex, sibling := range siblingHashes {
		depth := keyHistoryMapDepth - 1 - proofIndex
		height := proofIndex + 1
		if keyBit(key, depth) == 0 {
			current, err = keyHistoryMapNodeHash(height, current, sibling)
		} else {
			current, err = keyHistoryMapNodeHash(height, sibling, current)
		}
		if err != nil {
			return "", err
		}
	}
	return current, nil
}

func sparseMapRoot(entries []keyHistoryMapEntry, depth int, emptyHashes []string) (string, error) {
	if len(entries) == 0 {
		return emptyHashes[keyHistoryMapDepth-depth], nil
	}
	if depth == keyHistoryMapDepth {
		if len(entries) != 1 {
			return "", fmt.Errorf("%w: identity path collision", ErrInvalidMapProof)
		}
		return keyHistoryMapLeafHash(entries[0].identityID, entries[0].head)
	}

	left, right := partitionMapEntries(entries, depth)
	leftRoot, err := sparseMapRoot(left, depth+1, emptyHashes)
	if err != nil {
		return "", err
	}
	rightRoot, err := sparseMapRoot(right, depth+1, emptyHashes)
	if err != nil {
		return "", err
	}
	return keyHistoryMapNodeHash(keyHistoryMapDepth-depth, leftRoot, rightRoot)
}

// sparseMapProof returns sibling hashes from the leaf upward. That ordering
// lets a verifier replace the leaf and reuse every unchanged branch directly.
func sparseMapProof(entries []keyHistoryMapEntry, target [sha256.Size]byte, depth int, emptyHashes []string) (string, []string, error) {
	if depth == keyHistoryMapDepth {
		switch len(entries) {
		case 0:
			return emptyHashes[0], nil, nil
		case 1:
			if entries[0].key != target {
				return "", nil, fmt.Errorf("%w: identity path collision", ErrInvalidMapProof)
			}
			root, err := keyHistoryMapLeafHash(entries[0].identityID, entries[0].head)
			return root, nil, err
		default:
			return "", nil, fmt.Errorf("%w: identity path collision", ErrInvalidMapProof)
		}
	}

	left, right := partitionMapEntries(entries, depth)
	var branch, sibling []keyHistoryMapEntry
	if keyBit(target, depth) == 0 {
		branch, sibling = left, right
	} else {
		branch, sibling = right, left
	}
	branchRoot, siblingHashes, err := sparseMapProof(branch, target, depth+1, emptyHashes)
	if err != nil {
		return "", nil, err
	}
	siblingRoot, err := sparseMapRoot(sibling, depth+1, emptyHashes)
	if err != nil {
		return "", nil, err
	}
	siblingHashes = append(siblingHashes, siblingRoot)
	if keyBit(target, depth) == 0 {
		root, err := keyHistoryMapNodeHash(keyHistoryMapDepth-depth, branchRoot, siblingRoot)
		return root, siblingHashes, err
	}
	root, err := keyHistoryMapNodeHash(keyHistoryMapDepth-depth, siblingRoot, branchRoot)
	return root, siblingHashes, err
}

func partitionMapEntries(entries []keyHistoryMapEntry, depth int) ([]keyHistoryMapEntry, []keyHistoryMapEntry) {
	left := make([]keyHistoryMapEntry, 0, len(entries))
	right := make([]keyHistoryMapEntry, 0, len(entries))
	for _, entry := range entries {
		if keyBit(entry.key, depth) == 0 {
			left = append(left, entry)
		} else {
			right = append(right, entry)
		}
	}
	return left, right
}

func keyHistoryMapKey(identityID string) [sha256.Size]byte {
	return sha256.Sum256([]byte("fleetshift.dev/trust-v3/key-history-map-key/v1\x00" + identityID))
}

func keyBit(key [sha256.Size]byte, depth int) byte {
	return (key[depth/8] >> (7 - uint(depth%8))) & 1
}

func emptyKeyHistoryMapHashes() ([]string, error) {
	hashes := make([]string, keyHistoryMapDepth+1)
	leaf, err := ObjectDigest(struct {
		Protocol string `json:"protocol"`
	}{Protocol: "fleetshift.dev/trust-v3/key-history-map-empty/v1"})
	if err != nil {
		return nil, err
	}
	hashes[0] = leaf
	for height := 1; height <= keyHistoryMapDepth; height++ {
		hashes[height], err = keyHistoryMapNodeHash(height, hashes[height-1], hashes[height-1])
		if err != nil {
			return nil, err
		}
	}
	return hashes, nil
}

func keyHistoryMapLeafHash(identityID string, head KeyHistoryHead) (string, error) {
	return ObjectDigest(struct {
		Protocol   string         `json:"protocol"`
		IdentityID string         `json:"identity_id"`
		Head       KeyHistoryHead `json:"head"`
	}{
		Protocol:   "fleetshift.dev/trust-v3/key-history-map-leaf/v1",
		IdentityID: identityID,
		Head:       head,
	})
}

func keyHistoryMapNodeHash(height int, left, right string) (string, error) {
	return ObjectDigest(struct {
		Protocol string `json:"protocol"`
		Height   int    `json:"height"`
		Left     string `json:"left"`
		Right    string `json:"right"`
	}{
		Protocol: "fleetshift.dev/trust-v3/key-history-map-node/v1",
		Height:   height,
		Left:     left,
		Right:    right,
	})
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

func deliveryRecordMaterial(record DeliveryRecord) any {
	return struct {
		Index        uint64           `json:"index"`
		PreviousHash string           `json:"previous_hash"`
		Event        DeliveryLogEvent `json:"event"`
	}{record.Index, record.PreviousHash, record.Event}
}

func keyEventRecordMaterial(record KeyEventRecord) any {
	return struct {
		IdentityID           string   `json:"identity_id"`
		Sequence             uint64   `json:"sequence"`
		PreviousHash         string   `json:"previous_hash"`
		Event                KeyEvent `json:"event"`
		ResultingStateDigest string   `json:"resulting_state_digest"`
	}{record.IdentityID, record.Sequence, record.PreviousHash, record.Event, record.ResultingStateDigest}
}
