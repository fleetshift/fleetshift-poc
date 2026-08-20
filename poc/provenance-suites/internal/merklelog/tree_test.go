package merklelog

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/merkle/testonly"
)

func TestTreeMatchesRFC6962VectorsAndProofVerifier(t *testing.T) {
	tree := New()
	leaves := testonly.LeafInputs()
	wantRoots := testonly.RootHashes()

	assertRoot := func(size uint64) {
		t.Helper()
		got, err := tree.RootAt(size)
		if err != nil {
			t.Fatalf("RootAt(%d): %v", size, err)
		}
		if want := wantRoots[size]; !bytes.Equal(got, want) {
			t.Fatalf("RootAt(%d) = %x, want RFC 6962 root %x", size, got, want)
		}
	}

	assertRoot(0)
	for i, leaf := range leaves {
		index, leafHash, err := tree.Append(leaf)
		if err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if index != uint64(i) {
			t.Fatalf("Append(%d) index = %d", i, index)
		}
		if want := rfc6962.DefaultHasher.HashLeaf(leaf); !bytes.Equal(leafHash, want) {
			t.Fatalf("Append(%d) leaf hash = %x, want %x", i, leafHash, want)
		}
		assertRoot(uint64(i + 1))
	}

	for size := uint64(1); size <= tree.Size(); size++ {
		root, err := tree.RootAt(size)
		if err != nil {
			t.Fatalf("RootAt(%d): %v", size, err)
		}
		for index := uint64(0); index < size; index++ {
			inclusion, err := tree.InclusionProof(index, size)
			if err != nil {
				t.Fatalf("InclusionProof(%d, %d): %v", index, size, err)
			}
			if err := proof.VerifyInclusion(
				rfc6962.DefaultHasher,
				index,
				size,
				rfc6962.DefaultHasher.HashLeaf(leaves[index]),
				inclusion,
				root,
			); err != nil {
				t.Fatalf("verify inclusion (%d, %d): %v", index, size, err)
			}
		}

		for previousSize := uint64(1); previousSize <= size; previousSize++ {
			consistency, err := tree.ConsistencyProof(previousSize, size)
			if err != nil {
				t.Fatalf("ConsistencyProof(%d, %d): %v", previousSize, size, err)
			}
			previousRoot, err := tree.RootAt(previousSize)
			if err != nil {
				t.Fatalf("RootAt(%d): %v", previousSize, err)
			}
			if err := proof.VerifyConsistency(
				rfc6962.DefaultHasher,
				previousSize,
				size,
				consistency,
				previousRoot,
				root,
			); err != nil {
				t.Fatalf("verify consistency (%d, %d): %v", previousSize, size, err)
			}
		}
	}
}

func TestTreeRejectsInvalidProofQueries(t *testing.T) {
	tree := New()
	if _, _, err := tree.AppendHash(make([]byte, rfc6962.DefaultHasher.Size()-1)); err == nil {
		t.Fatal("AppendHash accepted a short hash")
	}
	if _, _, err := tree.Append([]byte("leaf")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{name: "future root", call: func() error { _, err := tree.RootAt(2); return err }},
		{name: "future inclusion tree", call: func() error { _, err := tree.InclusionProof(0, 2); return err }},
		{name: "out of range leaf", call: func() error { _, err := tree.InclusionProof(1, 1); return err }},
		{name: "backward consistency", call: func() error { _, err := tree.ConsistencyProof(1, 0); return err }},
		{name: "future consistency", call: func() error { _, err := tree.ConsistencyProof(1, 2); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatal("query unexpectedly succeeded")
			}
		})
	}
}

func TestTreeDoesNotExposeMutableHashStorage(t *testing.T) {
	aliasTree := New()
	callerHash := rfc6962.DefaultHasher.HashLeaf([]byte("caller-owned"))
	wantCallerHash := append([]byte(nil), callerHash...)
	if _, _, err := aliasTree.AppendHash(callerHash); err != nil {
		t.Fatalf("AppendHash caller-owned leaf: %v", err)
	}
	callerHash[0] ^= 0xff
	if _, _, err := aliasTree.Append([]byte("successor")); err != nil {
		t.Fatalf("Append successor after caller mutation: %v", err)
	}
	aliasRoot, err := aliasTree.Root()
	if err != nil {
		t.Fatalf("alias tree Root: %v", err)
	}
	wantAliasRoot := rfc6962.DefaultHasher.HashChildren(wantCallerHash, rfc6962.DefaultHasher.HashLeaf([]byte("successor")))
	if !bytes.Equal(aliasRoot, wantAliasRoot) {
		t.Fatalf("caller-owned input mutated tree storage: got %x, want %x", aliasRoot, wantAliasRoot)
	}

	tree := New()
	_, leafHash, err := tree.Append([]byte("leaf-0"))
	if err != nil {
		t.Fatalf("Append leaf 0: %v", err)
	}
	inputHash := rfc6962.DefaultHasher.HashLeaf([]byte("leaf-1"))
	if _, _, err := tree.AppendHash(inputHash); err != nil {
		t.Fatalf("Append leaf 1: %v", err)
	}
	wantRoot, err := tree.RootAt(2)
	if err != nil {
		t.Fatalf("RootAt: %v", err)
	}
	proofHashes, err := tree.InclusionProof(0, 2)
	if err != nil {
		t.Fatalf("InclusionProof: %v", err)
	}

	leafHash[0] ^= 0xff
	inputHash[0] ^= 0xff
	proofHashes[0][0] ^= 0xff
	wantRoot[0] ^= 0xff

	root, err := tree.RootAt(2)
	if err != nil {
		t.Fatalf("RootAt after caller mutation: %v", err)
	}
	inclusion, err := tree.InclusionProof(0, 2)
	if err != nil {
		t.Fatalf("InclusionProof after caller mutation: %v", err)
	}
	if err := proof.VerifyInclusion(
		rfc6962.DefaultHasher,
		0,
		2,
		rfc6962.DefaultHasher.HashLeaf([]byte("leaf-0")),
		inclusion,
		root,
	); err != nil {
		t.Fatalf("caller mutated tree hash storage: %v", err)
	}
}

func TestPendingAppendBuildsProofsWithoutMutatingTree(t *testing.T) {
	tree := New()
	for i := 0; i < 7; i++ {
		if _, _, err := tree.Append([]byte(fmt.Sprintf("leaf-%d", i))); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	beforeRoot, err := tree.Root()
	if err != nil {
		t.Fatalf("root before pending append: %v", err)
	}

	leafHash := rfc6962.DefaultHasher.HashLeaf([]byte("leaf-7"))
	pending, err := tree.BeginAppendHash(leafHash)
	if err != nil {
		t.Fatalf("begin append: %v", err)
	}
	if got, want := pending.Size(), uint64(8); got != want {
		t.Fatalf("pending size = %d, want %d", got, want)
	}
	if got, want := tree.Size(), uint64(7); got != want {
		t.Fatalf("tree changed before commit: size = %d, want %d", got, want)
	}
	if root, err := tree.Root(); err != nil || !bytes.Equal(root, beforeRoot) {
		t.Fatalf("tree changed before commit: root = %x, err = %v", root, err)
	}

	pendingRoot, err := pending.Root()
	if err != nil {
		t.Fatalf("pending root: %v", err)
	}
	inclusion, err := pending.InclusionProof(7, 8)
	if err != nil {
		t.Fatalf("pending inclusion proof: %v", err)
	}
	if err := proof.VerifyInclusion(rfc6962.DefaultHasher, 7, 8, leafHash, inclusion, pendingRoot); err != nil {
		t.Fatalf("verify pending inclusion: %v", err)
	}
	consistency, err := pending.ConsistencyProof(7, 8)
	if err != nil {
		t.Fatalf("pending consistency proof: %v", err)
	}
	if err := proof.VerifyConsistency(rfc6962.DefaultHasher, 7, 8, consistency, beforeRoot, pendingRoot); err != nil {
		t.Fatalf("verify pending consistency: %v", err)
	}

	if err := pending.Commit(); err != nil {
		t.Fatalf("commit pending append: %v", err)
	}
	if got, want := tree.Size(), uint64(8); got != want {
		t.Fatalf("committed size = %d, want %d", got, want)
	}
	if root, err := tree.Root(); err != nil || !bytes.Equal(root, pendingRoot) {
		t.Fatalf("committed root = %x, want %x, err = %v", root, pendingRoot, err)
	}
	if err := pending.Commit(); err == nil {
		t.Fatal("pending append committed twice")
	}
}

func FuzzTreeProofs(f *testing.F) {
	f.Add(uint8(8), uint8(3), uint8(5))
	f.Add(uint8(31), uint8(0), uint8(30))
	f.Fuzz(func(t *testing.T, count, oldOffset, leafOffset uint8) {
		size := uint64(count%64 + 1)
		tree := New()
		leaves := make([][]byte, size)
		for i := range leaves {
			leaves[i] = []byte(fmt.Sprintf("leaf-%d", i))
			if _, _, err := tree.Append(leaves[i]); err != nil {
				t.Fatalf("Append(%d): %v", i, err)
			}
		}

		index := uint64(leafOffset) % size
		root, err := tree.RootAt(size)
		if err != nil {
			t.Fatalf("RootAt(%d): %v", size, err)
		}
		inclusion, err := tree.InclusionProof(index, size)
		if err != nil {
			t.Fatalf("InclusionProof(%d, %d): %v", index, size, err)
		}
		if err := proof.VerifyInclusion(rfc6962.DefaultHasher, index, size, rfc6962.DefaultHasher.HashLeaf(leaves[index]), inclusion, root); err != nil {
			t.Fatalf("verify inclusion: %v", err)
		}

		oldSize := uint64(oldOffset)%size + 1
		oldRoot, err := tree.RootAt(oldSize)
		if err != nil {
			t.Fatalf("RootAt(%d): %v", oldSize, err)
		}
		consistency, err := tree.ConsistencyProof(oldSize, size)
		if err != nil {
			t.Fatalf("ConsistencyProof(%d, %d): %v", oldSize, size, err)
		}
		if err := proof.VerifyConsistency(rfc6962.DefaultHasher, oldSize, size, consistency, oldRoot, root); err != nil {
			t.Fatalf("verify consistency: %v", err)
		}
	})
}
