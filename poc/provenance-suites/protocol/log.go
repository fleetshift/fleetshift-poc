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

// EmptyCheckpoint is the uninitialized log position: size zero and the RFC 6962
// empty root.
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

// LogUpdate proves an append-only transition from a verifier's retained
// checkpoint and discloses one leaf: this package's root TypedEvidence
// identity. Unrelated tenant leaves are committed by the consistency proof
// and are not listed. From is the checkpoint the proofs were constructed
// from; it is transport metadata so a manager using a stale cached
// checkpoint can be distinguished from a log fork.
type LogUpdate struct {
	From             Checkpoint `json:"from"`
	Checkpoint       Checkpoint `json:"checkpoint"`
	ConsistencyProof []Digest   `json:"consistency_proof"`
	Index            uint64     `json:"index"`
	Leaf             Digest     `json:"leaf"`
	InclusionProof   []Digest   `json:"inclusion_proof"`
}

// LeafHash returns the RFC 6962 leaf hash of a TypedEvidence identity.
// Index is not mixed into the hash: the same identity at two positions is
// two log slots with the same payload.
func LeafHash(identity Digest) ([]byte, error) {
	raw, err := DecodeDigest(identity)
	if err != nil {
		return nil, fmt.Errorf("%w: leaf identity: %v", ErrInvalidLogUpdate, err)
	}
	return rfc6962.DefaultHasher.HashLeaf(raw), nil
}

// VerifyLogUpdate verifies the RFC 6962 consistency proof from previous to
// update.Checkpoint, inclusion of HashLeaf(Leaf) at Index under that
// checkpoint, and that Leaf equals the couriered root evidence identity.
func VerifyLogUpdate(previous Checkpoint, update LogUpdate, root TypedEvidence) error {
	identity, err := root.Identity()
	if err != nil {
		return fmt.Errorf("%w: root evidence identity: %v", ErrInvalidLogUpdate, err)
	}
	if update.Leaf != identity {
		return fmt.Errorf("%w: log leaf %q does not match root evidence identity %q", ErrInvalidLogUpdate, update.Leaf, identity)
	}

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
	if update.Index >= update.Checkpoint.Size {
		return fmt.Errorf("%w: index %d is beyond checkpoint size %d", ErrInvalidLogUpdate, update.Index, update.Checkpoint.Size)
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

	leafHash, err := LeafHash(update.Leaf)
	if err != nil {
		return err
	}
	inclusion, err := decodeProof(update.InclusionProof)
	if err != nil {
		return fmt.Errorf("%w: inclusion proof: %v", ErrInvalidLogUpdate, err)
	}
	if err := proof.VerifyInclusion(
		rfc6962.DefaultHasher,
		update.Index,
		update.Checkpoint.Size,
		leafHash,
		inclusion,
		successorRoot,
	); err != nil {
		return fmt.Errorf("%w: inclusion proof: %v", ErrInvalidLogUpdate, err)
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
