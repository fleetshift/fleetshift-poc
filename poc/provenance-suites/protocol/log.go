package protocol

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
)

// Checkpoint identifies an RFC 6962 Merkle-log version. Size is the number of
// leaves and Root is the tree root at exactly that size.
type Checkpoint struct {
	Size uint64 `json:"size"`
	Root Digest `json:"root"`
}

// EmptyCheckpoint is the uninitialized evidence-log position: size zero and
// the RFC 6962 empty root.
func EmptyCheckpoint() Checkpoint {
	return Checkpoint{Root: encodeDigest(rfc6962.DefaultHasher.EmptyRoot())}
}

// NewCheckpoint constructs and validates a checkpoint from an RFC 6962 root.
func NewCheckpoint(size uint64, root []byte) (Checkpoint, error) {
	encoded, err := EncodeDigest(root)
	if err != nil {
		return Checkpoint{}, err
	}
	checkpoint := Checkpoint{Size: size, Root: encoded}
	if _, err := validateCheckpoint(checkpoint); err != nil {
		return Checkpoint{}, err
	}
	return checkpoint, nil
}

// EvidenceLogUpdate proves an append-only evidence-log transition from a
// verifier's retained checkpoint. Unrelated accepted-evidence leaves are
// committed by the consistency proof and are not listed. From is the
// checkpoint the proofs were constructed from; it is transport metadata so
// a manager using a stale cached checkpoint can be distinguished from a
// log fork. Per-item inclusion lives on Item, not on this package-wide
// update.
type EvidenceLogUpdate struct {
	From             Checkpoint `json:"from"`
	Checkpoint       Checkpoint `json:"checkpoint"`
	ConsistencyProof []Digest   `json:"consistency_proof"`
}

// EvidenceLogInclusion proves that a TypedEvidence identity is a leaf of
// the evidence log at Index under a checkpoint. The verifier recomputes
// the identity from the adjacent statement and derives the RFC 6962 leaf
// hash; the inclusion does not serialize that digest. That identity
// received its index when the resource manager accepted the evidence, not
// when a delivery or outbox entry was created.
type EvidenceLogInclusion struct {
	Index          uint64   `json:"index"`
	InclusionProof []Digest `json:"inclusion_proof"`
}

// LeafHash returns the RFC 6962 leaf hash of a TypedEvidence identity.
// Index is not mixed into the hash. The honest RM assigns one canonical
// position per identity; a dishonest log that repeats the payload in two
// slots remains detectable as the same leaf hash at two indexes.
func LeafHash(identity Digest) ([]byte, error) {
	raw, err := DecodeDigest(identity)
	if err != nil {
		return nil, fmt.Errorf("leaf identity: %v", err)
	}
	return rfc6962.DefaultHasher.HashLeaf(raw), nil
}

// VerifyEvidenceLogUpdate verifies the RFC 6962 consistency proof from
// previous to update.Checkpoint. From is transport metadata and is not
// compared to previous; exact From matching is the caller's concern.
func VerifyEvidenceLogUpdate(previous Checkpoint, update EvidenceLogUpdate) error {
	previousRoot, err := validateCheckpoint(previous)
	if err != nil {
		return fmt.Errorf("%w: previous checkpoint: %v", ErrInvalidLogUpdate, err)
	}
	successorRoot, err := validateCheckpoint(update.Checkpoint)
	if err != nil {
		return fmt.Errorf("%w: successor checkpoint: %v", ErrInvalidLogUpdate, err)
	}
	if update.Checkpoint.Size < previous.Size {
		return fmt.Errorf("%w: successor size %d is before retained size %d", ErrInvalidLogUpdate, update.Checkpoint.Size, previous.Size)
	}

	consistency, err := decodeProof(update.ConsistencyProof)
	if err != nil {
		return fmt.Errorf("%w: consistency proof: %v", ErrInvalidLogUpdate, err)
	}
	switch {
	case previous.Size == 0:
		if len(consistency) != 0 {
			return fmt.Errorf("%w: consistency proof from empty tree must be empty", ErrInvalidLogUpdate)
		}
	case previous.Size == update.Checkpoint.Size:
		if len(consistency) != 0 || !bytes.Equal(previousRoot, successorRoot) {
			return fmt.Errorf("%w: equal-size checkpoint changed root or supplied a proof", ErrInvalidLogUpdate)
		}
	default:
		if err := proof.VerifyConsistency(
			rfc6962.DefaultHasher,
			previous.Size,
			update.Checkpoint.Size,
			consistency,
			previousRoot,
			successorRoot,
		); err != nil {
			return fmt.Errorf("%w: consistency proof: %v", ErrInvalidLogUpdate, err)
		}
	}
	return nil
}

// VerifyEvidenceLogInclusion verifies that evidence's TypedEvidence
// identity is the leaf at inclusion.Index under checkpoint. The leaf hash
// is recomputed from the adjacent evidence; inclusion does not carry a
// couriered digest.
func VerifyEvidenceLogInclusion(checkpoint Checkpoint, evidence TypedEvidence, inclusion EvidenceLogInclusion) error {
	identity, err := evidence.Identity()
	if err != nil {
		return fmt.Errorf("%w: evidence identity: %v", ErrInvalidLogInclusion, err)
	}
	root, err := validateCheckpoint(checkpoint)
	if err != nil {
		return fmt.Errorf("%w: checkpoint: %v", ErrInvalidLogInclusion, err)
	}
	if inclusion.Index >= checkpoint.Size {
		return fmt.Errorf("%w: index %d is beyond checkpoint size %d", ErrInvalidLogInclusion, inclusion.Index, checkpoint.Size)
	}

	leafHash, err := LeafHash(identity)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidLogInclusion, err)
	}
	proofHashes, err := decodeProof(inclusion.InclusionProof)
	if err != nil {
		return fmt.Errorf("%w: inclusion proof: %v", ErrInvalidLogInclusion, err)
	}
	if err := proof.VerifyInclusion(
		rfc6962.DefaultHasher,
		inclusion.Index,
		checkpoint.Size,
		leafHash,
		proofHashes,
		root,
	); err != nil {
		return fmt.Errorf("%w: inclusion proof: %v", ErrInvalidLogInclusion, err)
	}
	return nil
}

func validateCheckpoint(checkpoint Checkpoint) ([]byte, error) {
	root, err := decodeDigest(checkpoint.Root)
	if err != nil {
		return nil, err
	}
	if checkpoint.Size == 0 && !bytes.Equal(root, rfc6962.DefaultHasher.EmptyRoot()) {
		return nil, fmt.Errorf("size-zero checkpoint does not use the RFC 6962 empty root")
	}
	return root, nil
}

func decodeProof(encoded []Digest) ([][]byte, error) {
	hashes := make([][]byte, len(encoded))
	for i, value := range encoded {
		hash, err := decodeDigest(value)
		if err != nil {
			return nil, fmt.Errorf("hash %d: %w", i, err)
		}
		if len(hash) != sha256.Size {
			return nil, fmt.Errorf("hash %d has length %d, want %d", i, len(hash), sha256.Size)
		}
		hashes[i] = hash
	}
	return hashes, nil
}
