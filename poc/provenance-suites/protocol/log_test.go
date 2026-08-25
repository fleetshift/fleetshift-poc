package protocol

import (
	"bytes"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
	"github.com/transparency-dev/merkle/rfc6962"
)

func TestEmptyCheckpointUsesRFC6962EmptyRoot(t *testing.T) {
	got := EmptyCheckpoint()
	if got.Size != 0 {
		t.Fatalf("empty checkpoint size = %d, want 0", got.Size)
	}
	want, err := EncodeDigest(rfc6962.DefaultHasher.EmptyRoot())
	if err != nil {
		t.Fatalf("encode empty root: %v", err)
	}
	if got.Root != want {
		t.Fatalf("empty checkpoint root = %q, want RFC 6962 empty root %q", got.Root, want)
	}
}

func TestVerifyEvidenceLogUpdateStartsFromEmptyTree(t *testing.T) {
	tree := merklelog.New()
	evidence := testLogEvidence("first")
	update := mustAppendEvidenceLogUpdate(t, tree, EmptyCheckpoint(), evidence)

	if update.Index != 0 {
		t.Fatalf("first index = %d, want 0", update.Index)
	}
	if update.Checkpoint.Size != 1 {
		t.Fatalf("first checkpoint size = %d, want 1", update.Checkpoint.Size)
	}
	if len(update.ConsistencyProof) != 0 {
		t.Fatalf("empty-tree consistency proof has %d hashes, want 0", len(update.ConsistencyProof))
	}
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), update, evidence); err != nil {
		t.Fatalf("verify first update: %v", err)
	}
}

func TestVerifyEvidenceLogUpdateRequiresLeafToEqualRootIdentity(t *testing.T) {
	tree := merklelog.New()
	evidence := testLogEvidence("bound")
	update := mustAppendEvidenceLogUpdate(t, tree, EmptyCheckpoint(), evidence)
	other := testLogEvidence("other")

	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), update, other); !errors.Is(err, ErrInvalidLogUpdate) {
		t.Fatalf("mismatched evidence error = %v, want ErrInvalidLogUpdate", err)
	}

	update.Leaf = mustIdentity(t, other)
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), update, evidence); !errors.Is(err, ErrInvalidLogUpdate) {
		t.Fatalf("mismatched leaf error = %v, want ErrInvalidLogUpdate", err)
	}
}

func TestVerifyEvidenceLogUpdateAcceptsAppendAndRetry(t *testing.T) {
	tree := merklelog.New()
	firstEvidence := testLogEvidence("one")
	first := mustAppendEvidenceLogUpdate(t, tree, EmptyCheckpoint(), firstEvidence)
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), first, firstEvidence); err != nil {
		t.Fatalf("verify first append: %v", err)
	}

	secondEvidence := testLogEvidence("two")
	second := mustAppendEvidenceLogUpdate(t, tree, first.Checkpoint, secondEvidence)
	if second.Index != 1 {
		t.Fatalf("second index = %d, want 1", second.Index)
	}
	if second.Checkpoint.Size != 2 {
		t.Fatalf("second checkpoint size = %d, want 2", second.Checkpoint.Size)
	}
	if len(second.ConsistencyProof) == 0 {
		t.Fatal("one-to-two extension has no consistency proof")
	}
	if err := VerifyEvidenceLogUpdate(first.Checkpoint, second, secondEvidence); err != nil {
		t.Fatalf("verify second append: %v", err)
	}

	retry := mustEvidenceLogUpdate(t, tree, second.Checkpoint, second.Index, second.Leaf)
	if len(retry.ConsistencyProof) != 0 {
		t.Fatalf("equal-size retry consistency proof has %d hashes, want 0", len(retry.ConsistencyProof))
	}
	if err := VerifyEvidenceLogUpdate(second.Checkpoint, retry, secondEvidence); err != nil {
		t.Fatalf("verify same-head retry: %v", err)
	}
}

func TestVerifyEvidenceLogUpdateLeafDoesNotIncludeIndex(t *testing.T) {
	evidence := testLogEvidence("same-payload")
	identity := mustIdentity(t, evidence)
	leafHash, err := LeafHash(identity)
	if err != nil {
		t.Fatalf("leaf hash: %v", err)
	}

	tree := merklelog.New()
	if _, _, err := tree.AppendHash(leafHash); err != nil {
		t.Fatalf("append index 0: %v", err)
	}
	if _, _, err := tree.AppendHash(leafHash); err != nil {
		t.Fatalf("append index 1: %v", err)
	}

	first := mustEvidenceLogUpdate(t, tree, EmptyCheckpoint(), 0, identity)
	second := mustEvidenceLogUpdate(t, tree, EmptyCheckpoint(), 1, identity)
	if first.Leaf != second.Leaf {
		t.Fatalf("same identity produced different leaves: %q vs %q", first.Leaf, second.Leaf)
	}
	if first.Index == second.Index {
		t.Fatal("same identity at two positions collapsed to one index")
	}
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), first, evidence); err != nil {
		t.Fatalf("verify identity at index 0: %v", err)
	}
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), second, evidence); err != nil {
		t.Fatalf("verify identity at index 1: %v", err)
	}

	otherTree := merklelog.New()
	mustAppendEvidenceLogUpdate(t, otherTree, EmptyCheckpoint(), evidence)
	mustAppendEvidenceLogUpdate(t, otherTree, mustCheckpoint(t, otherTree, 1), testLogEvidence("other"))
	moved := mustEvidenceLogUpdate(t, otherTree, EmptyCheckpoint(), 0, identity)
	moved.Index = 1
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), moved, evidence); !errors.Is(err, ErrInvalidLogUpdate) {
		t.Fatalf("moved index without a matching inclusion proof error = %v, want ErrInvalidLogUpdate", err)
	}
}

func TestVerifyEvidenceLogUpdateSkipsUnrelatedLeaf(t *testing.T) {
	tree := merklelog.New()
	unrelated := testLogEvidence("other-target")
	mustAppendEvidenceLogUpdate(t, tree, EmptyCheckpoint(), unrelated)

	relevant := testLogEvidence("this-target")
	from := mustCheckpoint(t, tree, 1)
	update := mustAppendEvidenceLogUpdate(t, tree, from, relevant)
	if update.Index != 1 || update.Checkpoint.Size != 2 {
		t.Fatalf("update index = %d size = %d, want index 1 size 2", update.Index, update.Checkpoint.Size)
	}
	if update.Leaf != mustIdentity(t, relevant) {
		t.Fatal("update disclosed an unrelated leaf payload")
	}
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), mustEvidenceLogUpdate(t, tree, EmptyCheckpoint(), 1, update.Leaf), relevant); err != nil {
		t.Fatalf("verify inclusion of this leaf under a tree that also contains an unrelated leaf: %v", err)
	}
}

func TestVerifyEvidenceLogUpdateRejectsTamperingAndForks(t *testing.T) {
	tree := merklelog.New()
	firstEvidence := testLogEvidence("one")
	first := mustAppendEvidenceLogUpdate(t, tree, EmptyCheckpoint(), firstEvidence)
	secondEvidence := testLogEvidence("two")
	second := mustAppendEvidenceLogUpdate(t, tree, first.Checkpoint, secondEvidence)

	forkRoot, err := EncodeDigest(bytes.Repeat([]byte{0xff}, rfc6962.DefaultHasher.Size()))
	if err != nil {
		t.Fatalf("encode fork root: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*EvidenceLogUpdate)
	}{
		{name: "leaf", mutate: func(update *EvidenceLogUpdate) { update.Leaf = mustIdentity(t, testLogEvidence("attacker")) }},
		{name: "inclusion proof", mutate: func(update *EvidenceLogUpdate) { update.InclusionProof[0] = forkRoot }},
		{name: "consistency proof", mutate: func(update *EvidenceLogUpdate) { update.ConsistencyProof[0] = forkRoot }},
		{name: "forked root", mutate: func(update *EvidenceLogUpdate) { update.Checkpoint.Root = forkRoot }},
		{name: "skip-ahead size", mutate: func(update *EvidenceLogUpdate) { update.Checkpoint.Size = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tampered := second
			tampered.ConsistencyProof = append([]Digest(nil), second.ConsistencyProof...)
			tampered.InclusionProof = append([]Digest(nil), second.InclusionProof...)
			tc.mutate(&tampered)
			if err := VerifyEvidenceLogUpdate(first.Checkpoint, tampered, secondEvidence); !errors.Is(err, ErrInvalidLogUpdate) {
				t.Fatalf("error = %v, want ErrInvalidLogUpdate", err)
			}
		})
	}
}

func testLogEvidence(label string) TypedEvidence {
	return TypedEvidence{
		ProvenanceType: ProvenanceTypeDirectKeyV1,
		Encoded: Encoded{
			MediaType: "application/test+json",
			Bytes:     []byte(label),
		},
	}
}

func mustIdentity(t *testing.T, evidence TypedEvidence) Digest {
	t.Helper()
	identity, err := evidence.Identity()
	if err != nil {
		t.Fatalf("evidence identity: %v", err)
	}
	return identity
}

func mustCheckpoint(t *testing.T, tree *merklelog.Tree, size uint64) Checkpoint {
	t.Helper()
	root, err := tree.RootAt(size)
	if err != nil {
		t.Fatalf("root at %d: %v", size, err)
	}
	checkpoint, err := NewCheckpoint(size, root)
	if err != nil {
		t.Fatalf("checkpoint at %d: %v", size, err)
	}
	return checkpoint
}

func mustEvidenceLogUpdate(t *testing.T, tree *merklelog.Tree, from Checkpoint, index uint64, leaf Digest) EvidenceLogUpdate {
	t.Helper()
	current := mustCheckpoint(t, tree, tree.Size())
	consistency, err := tree.ConsistencyProof(from.Size, current.Size)
	if err != nil {
		t.Fatalf("consistency proof: %v", err)
	}
	inclusion, err := tree.InclusionProof(index, current.Size)
	if err != nil {
		t.Fatalf("inclusion proof: %v", err)
	}
	return EvidenceLogUpdate{
		From:             from,
		Checkpoint:       current,
		ConsistencyProof: EncodeProof(consistency),
		Index:            index,
		Leaf:             leaf,
		InclusionProof:   EncodeProof(inclusion),
	}
}

func mustAppendEvidenceLogUpdate(t *testing.T, tree *merklelog.Tree, from Checkpoint, evidence TypedEvidence) EvidenceLogUpdate {
	t.Helper()
	identity := mustIdentity(t, evidence)
	leafHash, err := LeafHash(identity)
	if err != nil {
		t.Fatalf("leaf hash: %v", err)
	}
	index, _, err := tree.AppendHash(leafHash)
	if err != nil {
		t.Fatalf("append leaf: %v", err)
	}
	return mustEvidenceLogUpdate(t, tree, from, index, identity)
}
