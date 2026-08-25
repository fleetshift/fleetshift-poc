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
	update, inclusion := mustAppendLog(t, tree, EmptyCheckpoint(), evidence)

	if inclusion.Index != 0 {
		t.Fatalf("first index = %d, want 0", inclusion.Index)
	}
	if update.Checkpoint.Size != 1 {
		t.Fatalf("first checkpoint size = %d, want 1", update.Checkpoint.Size)
	}
	if len(update.ConsistencyProof) != 0 {
		t.Fatalf("empty-tree consistency proof has %d hashes, want 0", len(update.ConsistencyProof))
	}
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), update); err != nil {
		t.Fatalf("verify first update: %v", err)
	}
	if err := VerifyEvidenceLogInclusion(update.Checkpoint, evidence, inclusion); err != nil {
		t.Fatalf("verify first inclusion: %v", err)
	}
}

func TestVerifyEvidenceLogInclusionRequiresAdjacentEvidenceIdentity(t *testing.T) {
	tree := merklelog.New()
	evidence := testLogEvidence("bound")
	update, inclusion := mustAppendLog(t, tree, EmptyCheckpoint(), evidence)
	other := testLogEvidence("other")

	if err := VerifyEvidenceLogInclusion(update.Checkpoint, other, inclusion); !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("mismatched evidence error = %v, want ErrInvalidLogInclusion", err)
	}
}

func TestVerifyEvidenceLogUpdateAcceptsAppendAndRetry(t *testing.T) {
	tree := merklelog.New()
	firstEvidence := testLogEvidence("one")
	first, firstInclusion := mustAppendLog(t, tree, EmptyCheckpoint(), firstEvidence)
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), first); err != nil {
		t.Fatalf("verify first append: %v", err)
	}
	if err := VerifyEvidenceLogInclusion(first.Checkpoint, firstEvidence, firstInclusion); err != nil {
		t.Fatalf("verify first inclusion: %v", err)
	}

	secondEvidence := testLogEvidence("two")
	second, secondInclusion := mustAppendLog(t, tree, first.Checkpoint, secondEvidence)
	if secondInclusion.Index != 1 {
		t.Fatalf("second index = %d, want 1", secondInclusion.Index)
	}
	if second.Checkpoint.Size != 2 {
		t.Fatalf("second checkpoint size = %d, want 2", second.Checkpoint.Size)
	}
	if len(second.ConsistencyProof) == 0 {
		t.Fatal("one-to-two extension has no consistency proof")
	}
	if err := VerifyEvidenceLogUpdate(first.Checkpoint, second); err != nil {
		t.Fatalf("verify second append: %v", err)
	}
	if err := VerifyEvidenceLogInclusion(second.Checkpoint, secondEvidence, secondInclusion); err != nil {
		t.Fatalf("verify second inclusion: %v", err)
	}

	retry := mustEvidenceLogUpdate(t, tree, second.Checkpoint)
	if len(retry.ConsistencyProof) != 0 {
		t.Fatalf("equal-size retry consistency proof has %d hashes, want 0", len(retry.ConsistencyProof))
	}
	if err := VerifyEvidenceLogUpdate(second.Checkpoint, retry); err != nil {
		t.Fatalf("verify same-head retry: %v", err)
	}
	retryInclusion := mustEvidenceLogInclusion(t, tree, secondInclusion.Index)
	if err := VerifyEvidenceLogInclusion(retry.Checkpoint, secondEvidence, retryInclusion); err != nil {
		t.Fatalf("verify same-head inclusion: %v", err)
	}
}

func TestVerifyEvidenceLogInclusionDoesNotIncludeIndexInLeafHash(t *testing.T) {
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

	checkpoint := mustCheckpoint(t, tree, tree.Size())
	first := mustEvidenceLogInclusion(t, tree, 0)
	second := mustEvidenceLogInclusion(t, tree, 1)
	if first.Index == second.Index {
		t.Fatal("same identity at two positions collapsed to one index")
	}
	if err := VerifyEvidenceLogInclusion(checkpoint, evidence, first); err != nil {
		t.Fatalf("verify identity at index 0: %v", err)
	}
	if err := VerifyEvidenceLogInclusion(checkpoint, evidence, second); err != nil {
		t.Fatalf("verify identity at index 1: %v", err)
	}

	otherTree := merklelog.New()
	mustAppendLog(t, otherTree, EmptyCheckpoint(), evidence)
	mustAppendLog(t, otherTree, mustCheckpoint(t, otherTree, 1), testLogEvidence("other"))
	moved := mustEvidenceLogInclusion(t, otherTree, 0)
	moved.Index = 1
	if err := VerifyEvidenceLogInclusion(mustCheckpoint(t, otherTree, otherTree.Size()), evidence, moved); !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("moved index without a matching inclusion proof error = %v, want ErrInvalidLogInclusion", err)
	}
}

func TestVerifyEvidenceLogInclusionSkipsUnrelatedLeaf(t *testing.T) {
	tree := merklelog.New()
	unrelated := testLogEvidence("other-target")
	mustAppendLog(t, tree, EmptyCheckpoint(), unrelated)

	relevant := testLogEvidence("this-target")
	from := mustCheckpoint(t, tree, 1)
	update, inclusion := mustAppendLog(t, tree, from, relevant)
	if inclusion.Index != 1 || update.Checkpoint.Size != 2 {
		t.Fatalf("update index = %d size = %d, want index 1 size 2", inclusion.Index, update.Checkpoint.Size)
	}

	fromEmpty := mustEvidenceLogUpdate(t, tree, EmptyCheckpoint())
	if err := VerifyEvidenceLogUpdate(EmptyCheckpoint(), fromEmpty); err != nil {
		t.Fatalf("verify consistency over an unrelated leaf: %v", err)
	}
	if err := VerifyEvidenceLogInclusion(fromEmpty.Checkpoint, relevant, mustEvidenceLogInclusion(t, tree, 1)); err != nil {
		t.Fatalf("verify inclusion of this identity under a tree that also contains an unrelated leaf: %v", err)
	}
}

func TestVerifyEvidenceLogUpdateRejectsTamperingAndForks(t *testing.T) {
	tree := merklelog.New()
	firstEvidence := testLogEvidence("one")
	first, _ := mustAppendLog(t, tree, EmptyCheckpoint(), firstEvidence)
	secondEvidence := testLogEvidence("two")
	second, _ := mustAppendLog(t, tree, first.Checkpoint, secondEvidence)

	forkRoot, err := EncodeDigest(bytes.Repeat([]byte{0xff}, rfc6962.DefaultHasher.Size()))
	if err != nil {
		t.Fatalf("encode fork root: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*EvidenceLogUpdate)
	}{
		{name: "consistency proof", mutate: func(update *EvidenceLogUpdate) { update.ConsistencyProof[0] = forkRoot }},
		{name: "forked root", mutate: func(update *EvidenceLogUpdate) { update.Checkpoint.Root = forkRoot }},
		{name: "skip-ahead size", mutate: func(update *EvidenceLogUpdate) { update.Checkpoint.Size = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tampered := second
			tampered.ConsistencyProof = append([]Digest(nil), second.ConsistencyProof...)
			tc.mutate(&tampered)
			if err := VerifyEvidenceLogUpdate(first.Checkpoint, tampered); !errors.Is(err, ErrInvalidLogUpdate) {
				t.Fatalf("error = %v, want ErrInvalidLogUpdate", err)
			}
		})
	}
}

func TestVerifyEvidenceLogInclusionRejectsTamperedProof(t *testing.T) {
	tree := merklelog.New()
	mustAppendLog(t, tree, EmptyCheckpoint(), testLogEvidence("one"))
	secondEvidence := testLogEvidence("two")
	update, inclusion := mustAppendLog(t, tree, mustCheckpoint(t, tree, 1), secondEvidence)

	forkRoot, err := EncodeDigest(bytes.Repeat([]byte{0xff}, rfc6962.DefaultHasher.Size()))
	if err != nil {
		t.Fatalf("encode fork root: %v", err)
	}
	tampered := inclusion
	tampered.InclusionProof = append([]Digest(nil), inclusion.InclusionProof...)
	tampered.InclusionProof[0] = forkRoot
	if err := VerifyEvidenceLogInclusion(update.Checkpoint, secondEvidence, tampered); !errors.Is(err, ErrInvalidLogInclusion) {
		t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
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

func mustEvidenceLogUpdate(t *testing.T, tree *merklelog.Tree, from Checkpoint) EvidenceLogUpdate {
	t.Helper()
	current := mustCheckpoint(t, tree, tree.Size())
	consistency, err := tree.ConsistencyProof(from.Size, current.Size)
	if err != nil {
		t.Fatalf("consistency proof: %v", err)
	}
	return EvidenceLogUpdate{
		From:             from,
		Checkpoint:       current,
		ConsistencyProof: EncodeProof(consistency),
	}
}

func mustEvidenceLogInclusion(t *testing.T, tree *merklelog.Tree, index uint64) EvidenceLogInclusion {
	t.Helper()
	inclusion, err := tree.InclusionProof(index, tree.Size())
	if err != nil {
		t.Fatalf("inclusion proof: %v", err)
	}
	return EvidenceLogInclusion{
		Index:          index,
		InclusionProof: EncodeProof(inclusion),
	}
}

func mustAppendLog(t *testing.T, tree *merklelog.Tree, from Checkpoint, evidence TypedEvidence) (EvidenceLogUpdate, EvidenceLogInclusion) {
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
	return mustEvidenceLogUpdate(t, tree, from), mustEvidenceLogInclusion(t, tree, index)
}
