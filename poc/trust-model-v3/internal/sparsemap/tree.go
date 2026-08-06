// Package sparsemap provides the POC's in-memory adapter around Trillian's
// sparse-Merkle writer and CONIKS hashing implementation.
package sparsemap

import (
	"bytes"
	"context"
	"crypto"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/google/trillian/merkle/coniks"
	"github.com/google/trillian/merkle/smt"
	"github.com/google/trillian/merkle/smt/node"
)

const (
	// Height is the number of bits in a SHA-256 sparse-map key.
	Height = sha256.Size * 8

	hashSize = sha256.Size
)

var mapHasher = coniks.New(crypto.SHA256)

// Tree is an in-memory, 256-level sparse Merkle tree. Trillian's Writer owns
// update propagation and its CONIKS hasher owns position-bound leaf, empty,
// and internal-node hashing. This type supplies only node persistence and
// sibling-path retrieval.
//
// Tree is not safe for concurrent use. Its callers serialize mutations.
type Tree struct {
	domain string
	treeID int64
	writer *smt.Writer
	nodes  map[node.ID][]byte
	root   []byte
}

// New returns an empty sparse map separated by domain. FleetShift passes the
// tenant ID as the domain so otherwise identical maps have different roots.
func New(domain string) *Tree {
	treeID := deriveTreeID(domain)
	return &Tree{
		domain: domain,
		treeID: treeID,
		writer: smt.NewWriter(treeID, mapHasher, Height, 0),
		nodes:  make(map[node.ID][]byte),
		root:   cloneHash(mapHasher.HashEmpty(treeID, node.ID{})),
	}
}

// Root returns a copy of the current sparse-map root.
func (t *Tree) Root() []byte {
	return cloneHash(t.root)
}

// Proof returns all sibling hashes from the leaf level upward. Empty sibling
// subtrees use CONIKS' position-bound empty hash.
func (t *Tree) Proof(key []byte) ([][]byte, error) {
	id, err := keyID(t.domain, key)
	if err != nil {
		return nil, err
	}
	path := make([][]byte, 0, Height)
	for depth := uint(Height); depth > 0; depth-- {
		sibling := id.Prefix(depth).Sibling()
		hash, ok := t.nodes[sibling]
		if !ok {
			hash = mapHasher.HashEmpty(t.treeID, sibling)
		}
		if len(hash) != hashSize {
			return nil, fmt.Errorf("sparse-map sibling at depth %d has hash length %d, want %d", depth, len(hash), hashSize)
		}
		path = append(path, cloneHash(hash))
	}
	return path, nil
}

// Set replaces the leaf at key with the CONIKS commitment to valueHash and
// returns the resulting root. valueHash must already be a SHA-256 digest of
// the canonical map value.
func (t *Tree) Set(key, valueHash []byte) ([]byte, error) {
	id, err := keyID(t.domain, key)
	if err != nil {
		return nil, err
	}
	if err := validateHash("sparse-map value", valueHash); err != nil {
		return nil, err
	}

	// Run the writer against a replacement store and commit only after it has
	// successfully produced a well-formed root.
	nextNodes := cloneNodeMap(t.nodes)
	accessor := &memoryAccessor{nodes: nextNodes}
	rootUpdate, err := t.writer.Write(context.Background(), []smt.Node{{
		ID:   id,
		Hash: mapHasher.HashLeaf(t.treeID, id, valueHash),
	}}, accessor)
	if err != nil {
		return nil, fmt.Errorf("write sparse-map leaf: %w", err)
	}
	if rootUpdate.ID.BitLen() != 0 {
		return nil, fmt.Errorf("sparse-map writer returned root at depth %d", rootUpdate.ID.BitLen())
	}
	if err := validateHash("sparse-map root", rootUpdate.Hash); err != nil {
		return nil, err
	}

	t.nodes = nextNodes
	t.root = cloneHash(rootUpdate.Hash)
	return cloneHash(rootUpdate.Hash), nil
}

// RootFromProof reconstructs a sparse-map root for either a present leaf
// (non-nil valueHash) or an absent leaf (nil valueHash). Trillian's HStar3
// implementation performs the leaf-to-root topology and child ordering.
func RootFromProof(domain string, key, valueHash []byte, siblingHashes [][]byte) ([]byte, error) {
	id, err := keyID(domain, key)
	if err != nil {
		return nil, err
	}
	if len(siblingHashes) != Height {
		return nil, fmt.Errorf("sparse-map proof has %d sibling hashes, want %d", len(siblingHashes), Height)
	}

	treeID := deriveTreeID(domain)
	siblings := make(map[node.ID][]byte, Height)
	for i, hash := range siblingHashes {
		if err := validateHash(fmt.Sprintf("sparse-map sibling %d", i), hash); err != nil {
			return nil, err
		}
		depth := uint(Height - i)
		siblings[id.Prefix(depth).Sibling()] = cloneHash(hash)
	}
	if valueHash == nil {
		return absentRootFromProof(treeID, id, siblings)
	}
	if err := validateHash("sparse-map value", valueHash); err != nil {
		return nil, err
	}
	leafHash := mapHasher.HashLeaf(treeID, id, valueHash)

	updates := []smt.Node{{ID: id, Hash: leafHash}}
	hstar, err := smt.NewHStar3(updates, mapHasher.HashChildren, Height, 0)
	if err != nil {
		return nil, fmt.Errorf("initialize sparse-map proof reconstruction: %w", err)
	}
	roots, err := hstar.Update(proofAccessor{siblings: siblings})
	if err != nil {
		return nil, fmt.Errorf("reconstruct sparse-map proof: %w", err)
	}
	if len(roots) != 1 || roots[0].ID.BitLen() != 0 {
		return nil, fmt.Errorf("sparse-map proof produced %d non-canonical roots", len(roots))
	}
	if err := validateHash("sparse-map proof root", roots[0].Hash); err != nil {
		return nil, err
	}
	return cloneHash(roots[0].Hash), nil
}

// absentRootFromProof handles CONIKS' compressed empty subtrees. HashEmpty is
// position-bound and deliberately is not HashChildren(emptyLeft, emptyRight),
// so two empty children must collapse to the canonical parent empty hash.
// Once either side is non-empty, ordinary CONIKS child hashing applies.
func absentRootFromProof(treeID int64, id node.ID, siblings map[node.ID][]byte) ([]byte, error) {
	current := mapHasher.HashEmpty(treeID, id)
	currentIsEmpty := true
	for depth := uint(Height); depth > 0; depth-- {
		pathNode := id.Prefix(depth)
		siblingID := pathNode.Sibling()
		sibling, ok := siblings[siblingID]
		if !ok {
			return nil, fmt.Errorf("proof is missing sibling %v", siblingID)
		}
		siblingIsEmpty := bytes.Equal(sibling, mapHasher.HashEmpty(treeID, siblingID))
		parent := pathNode.Prefix(depth - 1)
		if currentIsEmpty && siblingIsEmpty {
			current = mapHasher.HashEmpty(treeID, parent)
			continue
		}
		if isLeftChild(pathNode) {
			current = mapHasher.HashChildren(current, sibling)
		} else {
			current = mapHasher.HashChildren(sibling, current)
		}
		currentIsEmpty = false
	}
	return cloneHash(current), nil
}

type memoryAccessor struct {
	nodes map[node.ID][]byte
}

func (a *memoryAccessor) Get(ctx context.Context, ids []node.ID) (map[node.ID][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[node.ID][]byte, len(ids))
	for _, id := range ids {
		if hash, ok := a.nodes[id]; ok {
			result[id] = cloneHash(hash)
		}
	}
	return result, nil
}

func (a *memoryAccessor) Set(ctx context.Context, updates []smt.Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, update := range updates {
		if err := validateHash("sparse-map node", update.Hash); err != nil {
			return err
		}
	}
	for _, update := range updates {
		a.nodes[update.ID] = cloneHash(update.Hash)
	}
	return nil
}

type proofAccessor struct {
	siblings map[node.ID][]byte
}

func (a proofAccessor) Get(id node.ID) ([]byte, error) {
	hash, ok := a.siblings[id]
	if !ok {
		return nil, fmt.Errorf("proof is missing sibling %v", id)
	}
	return cloneHash(hash), nil
}

func (proofAccessor) Set(node.ID, []byte) {}

func keyID(domain string, key []byte) (node.ID, error) {
	if len(key) != sha256.Size {
		return node.ID{}, fmt.Errorf("sparse-map key has length %d, want %d", len(key), sha256.Size)
	}
	domainHash := sha256.Sum256([]byte("fleetshift.dev/trust-v3/sparse-map-domain/v1\x00" + domain))
	material := make([]byte, 0, len("fleetshift.dev/trust-v3/sparse-map-key/v1\x00")+len(domainHash)+len(key))
	material = append(material, "fleetshift.dev/trust-v3/sparse-map-key/v1\x00"...)
	material = append(material, domainHash[:]...)
	material = append(material, key...)
	id := sha256.Sum256(material)
	return node.NewID(string(id[:]), Height), nil
}

func deriveTreeID(domain string) int64 {
	digest := sha256.Sum256([]byte("fleetshift.dev/trust-v3/key-history-map-tree/v1\x00" + domain))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func isLeftChild(id node.ID) bool {
	last, bits := id.LastByte()
	return last&(1<<(8-bits)) == 0
}

func validateHash(name string, hash []byte) error {
	if len(hash) != hashSize {
		return fmt.Errorf("%s has length %d, want %d", name, len(hash), hashSize)
	}
	return nil
}

func cloneHash(hash []byte) []byte {
	return append([]byte(nil), hash...)
}

func cloneHashes(hashes [][]byte) [][]byte {
	out := make([][]byte, len(hashes))
	for i, hash := range hashes {
		out[i] = cloneHash(hash)
	}
	return out
}

func cloneNodeMap(in map[node.ID][]byte) map[node.ID][]byte {
	out := make(map[node.ID][]byte, len(in))
	for id, hash := range in {
		out[id] = cloneHash(hash)
	}
	return out
}
