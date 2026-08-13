// Package merklelog adapts transparency-dev/merkle's compact-range and proof
// primitives to the POC's in-memory append-only storage.
package merklelog

import (
	"fmt"
	"math"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
)

// Tree stores all complete RFC 6962 subtree hashes needed to produce roots,
// inclusion proofs, and consistency proofs for every retained tree size. The
// Merkle topology, compact frontier, proof layout, and hashing rules come from
// transparency-dev/merkle; this type supplies only the in-memory node store.
//
// Tree is not safe for concurrent use. Its callers serialize mutations.
type Tree struct {
	factory  compact.RangeFactory
	frontier *compact.Range
	nodes    map[compact.NodeID][]byte
}

// PendingAppend is an append-only proof-store transaction. It exposes the
// successor root and proofs before making the new Merkle nodes visible through
// Tree. This lets a caller assemble and validate a larger protocol transaction
// before atomically committing the history append.
//
// PendingAppend is not safe for concurrent use. The underlying Tree must not
// be changed between BeginAppendHash and Commit.
type PendingAppend struct {
	tree      *Tree
	baseSize  uint64
	frontier  *compact.Range
	writes    map[compact.NodeID][]byte
	committed bool
}

// New returns an empty SHA-256 RFC 6962 Merkle log.
func New() *Tree {
	t := &Tree{
		factory: compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren},
		nodes:   make(map[compact.NodeID][]byte),
	}
	t.frontier = t.factory.NewEmptyRange(0)
	return t
}

// Size returns the number of leaves in the tree.
func (t *Tree) Size() uint64 {
	return t.frontier.End()
}

// Append hashes data as an RFC 6962 leaf and appends it atomically to the
// compact frontier. It returns the assigned zero-based index and leaf hash.
func (t *Tree) Append(data []byte) (uint64, []byte, error) {
	return t.AppendHash(rfc6962.DefaultHasher.HashLeaf(data))
}

// AppendHash appends an already domain-separated RFC 6962 leaf hash. This is
// useful when a protocol record carries its leaf hash and has already been
// independently revalidated by the caller.
func (t *Tree) AppendHash(leafHash []byte) (uint64, []byte, error) {
	pending, err := t.BeginAppendHash(leafHash)
	if err != nil {
		return 0, nil, err
	}
	if err := pending.Commit(); err != nil {
		return 0, nil, err
	}
	return pending.baseSize, cloneHash(leafHash), nil
}

// BeginAppendHash prepares an append of an already domain-separated RFC 6962
// leaf hash. The returned view can produce the successor root and proofs using
// only the retained compact frontier and Merkle nodes. Tree remains unchanged
// until Commit succeeds.
func (t *Tree) BeginAppendHash(leafHash []byte) (*PendingAppend, error) {
	index := t.Size()
	if index == math.MaxUint64 {
		return nil, fmt.Errorf("Merkle tree is at maximum uint64 size")
	}
	if got, want := len(leafHash), rfc6962.DefaultHasher.Size(); got != want {
		return nil, fmt.Errorf("Merkle leaf hash has length %d, want %d", got, want)
	}
	leafHash = cloneHash(leafHash)

	next, err := t.factory.NewRange(0, index, cloneHashes(t.frontier.Hashes()))
	if err != nil {
		return nil, fmt.Errorf("clone Merkle frontier: %w", err)
	}
	writes := make(map[compact.NodeID][]byte)
	if err := next.Append(leafHash, func(id compact.NodeID, hash []byte) {
		writes[id] = cloneHash(hash)
	}); err != nil {
		return nil, fmt.Errorf("append Merkle leaf: %w", err)
	}
	return &PendingAppend{
		tree:     t,
		baseSize: index,
		frontier: next,
		writes:   writes,
	}, nil
}

// Size returns the successor tree size.
func (p *PendingAppend) Size() uint64 {
	return p.frontier.End()
}

// Root returns the successor RFC 6962 root.
func (p *PendingAppend) Root() ([]byte, error) {
	if err := p.checkUsable(); err != nil {
		return nil, err
	}
	root, err := p.frontier.GetRootHash(nil)
	if err != nil {
		return nil, fmt.Errorf("compute pending Merkle root: %w", err)
	}
	return cloneHash(root), nil
}

// InclusionProof returns an RFC 6962 inclusion proof from the pending
// successor view without committing it.
func (p *PendingAppend) InclusionProof(index, size uint64) ([][]byte, error) {
	if err := p.checkUsable(); err != nil {
		return nil, err
	}
	if size > p.Size() {
		return nil, fmt.Errorf("proof tree size %d is beyond pending size %d", size, p.Size())
	}
	nodes, err := proof.Inclusion(index, size)
	if err != nil {
		return nil, fmt.Errorf("select inclusion proof nodes: %w", err)
	}
	hashes, err := p.getNodes(nodes.IDs)
	if err != nil {
		return nil, err
	}
	result, err := nodes.Rehash(hashes, rfc6962.DefaultHasher.HashChildren)
	if err != nil {
		return nil, fmt.Errorf("construct inclusion proof: %w", err)
	}
	return cloneHashes(result), nil
}

// ConsistencyProof returns an RFC 6962 consistency proof from the pending
// successor view without committing it.
func (p *PendingAppend) ConsistencyProof(size1, size2 uint64) ([][]byte, error) {
	if err := p.checkUsable(); err != nil {
		return nil, err
	}
	if size2 > p.Size() {
		return nil, fmt.Errorf("proof tree size %d is beyond pending size %d", size2, p.Size())
	}
	nodes, err := proof.Consistency(size1, size2)
	if err != nil {
		return nil, fmt.Errorf("select consistency proof nodes: %w", err)
	}
	hashes, err := p.getNodes(nodes.IDs)
	if err != nil {
		return nil, err
	}
	result, err := nodes.Rehash(hashes, rfc6962.DefaultHasher.HashChildren)
	if err != nil {
		return nil, fmt.Errorf("construct consistency proof: %w", err)
	}
	return cloneHashes(result), nil
}

// Commit makes the prepared Merkle nodes and successor frontier visible.
func (p *PendingAppend) Commit() error {
	if err := p.checkUsable(); err != nil {
		return err
	}
	for id, hash := range p.writes {
		p.tree.nodes[id] = cloneHash(hash)
	}
	p.tree.frontier = p.frontier
	p.committed = true
	return nil
}

func (p *PendingAppend) checkUsable() error {
	if p == nil || p.tree == nil || p.frontier == nil {
		return fmt.Errorf("pending Merkle append is uninitialized")
	}
	if p.committed {
		return fmt.Errorf("pending Merkle append is already committed")
	}
	if p.tree.Size() != p.baseSize {
		return fmt.Errorf("Merkle tree advanced from size %d to %d before pending append committed", p.baseSize, p.tree.Size())
	}
	return nil
}

func (p *PendingAppend) getNodes(ids []compact.NodeID) ([][]byte, error) {
	hashes := make([][]byte, len(ids))
	for i, id := range ids {
		hash, ok := p.writes[id]
		if !ok {
			hash, ok = p.tree.nodes[id]
		}
		if !ok {
			begin, end := id.Coverage()
			return nil, fmt.Errorf("Merkle node (level=%d index=%d range=[%d,%d)) is unavailable", id.Level, id.Index, begin, end)
		}
		hashes[i] = cloneHash(hash)
	}
	return hashes, nil
}

// Root returns the current tree root.
func (t *Tree) Root() ([]byte, error) {
	return t.RootAt(t.Size())
}

// RootAt returns the RFC 6962 root at a retained tree size.
func (t *Tree) RootAt(size uint64) ([]byte, error) {
	if size > t.Size() {
		return nil, fmt.Errorf("tree size %d is beyond retained size %d", size, t.Size())
	}
	if size == 0 {
		return cloneHash(rfc6962.DefaultHasher.EmptyRoot()), nil
	}

	ids := compact.RangeNodes(0, size, nil)
	hashes, err := t.getNodes(ids)
	if err != nil {
		return nil, err
	}
	r, err := t.factory.NewRange(0, size, hashes)
	if err != nil {
		return nil, fmt.Errorf("construct Merkle frontier at size %d: %w", size, err)
	}
	root, err := r.GetRootHash(nil)
	if err != nil {
		return nil, fmt.Errorf("compute Merkle root at size %d: %w", size, err)
	}
	return cloneHash(root), nil
}

// InclusionProof returns an RFC 6962 inclusion proof for index under the root
// at size.
func (t *Tree) InclusionProof(index, size uint64) ([][]byte, error) {
	if size > t.Size() {
		return nil, fmt.Errorf("proof tree size %d is beyond retained size %d", size, t.Size())
	}
	nodes, err := proof.Inclusion(index, size)
	if err != nil {
		return nil, fmt.Errorf("select inclusion proof nodes: %w", err)
	}
	hashes, err := t.getNodes(nodes.IDs)
	if err != nil {
		return nil, err
	}
	result, err := nodes.Rehash(hashes, rfc6962.DefaultHasher.HashChildren)
	if err != nil {
		return nil, fmt.Errorf("construct inclusion proof: %w", err)
	}
	return cloneHashes(result), nil
}

// ConsistencyProof returns an RFC 6962 consistency proof from size1 to size2.
// Per RFC 6962, the proof from the empty tree is empty.
func (t *Tree) ConsistencyProof(size1, size2 uint64) ([][]byte, error) {
	if size2 > t.Size() {
		return nil, fmt.Errorf("proof tree size %d is beyond retained size %d", size2, t.Size())
	}
	nodes, err := proof.Consistency(size1, size2)
	if err != nil {
		return nil, fmt.Errorf("select consistency proof nodes: %w", err)
	}
	hashes, err := t.getNodes(nodes.IDs)
	if err != nil {
		return nil, err
	}
	result, err := nodes.Rehash(hashes, rfc6962.DefaultHasher.HashChildren)
	if err != nil {
		return nil, fmt.Errorf("construct consistency proof: %w", err)
	}
	return cloneHashes(result), nil
}

func (t *Tree) getNodes(ids []compact.NodeID) ([][]byte, error) {
	hashes := make([][]byte, len(ids))
	for i, id := range ids {
		hash, ok := t.nodes[id]
		if !ok {
			begin, end := id.Coverage()
			return nil, fmt.Errorf("Merkle node (level=%d index=%d range=[%d,%d)) is unavailable", id.Level, id.Index, begin, end)
		}
		hashes[i] = cloneHash(hash)
	}
	return hashes, nil
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
