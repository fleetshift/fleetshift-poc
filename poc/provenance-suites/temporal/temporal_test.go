package temporal

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/transparency-dev/merkle/rfc6962"
)

func TestPrepareRejectsNilUpdate(t *testing.T) {
	_, err := Prepare(RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, nil, protocol.TypedEvidence{}, nil)
	if !errors.Is(err, ErrMissingUpdate) {
		t.Fatalf("error = %v, want ErrMissingUpdate", err)
	}
}

func TestPrepare(t *testing.T) {
	fx := newLogFixture(t)
	forkRoot := mustForkRoot(t)

	t.Run("normal extension", func(t *testing.T) {
		prepared, err := Prepare(RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, &fx.u1, fx.e1, &fx.i1)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if prepared.NextState.EvidenceLog != fx.u1.Checkpoint {
			t.Fatalf("NextState = %+v, want %+v", prepared.NextState.EvidenceLog, fx.u1.Checkpoint)
		}
		if prepared.Log == nil {
			t.Fatal("prepared log verifier is nil")
		}
	})

	t.Run("equal-head retry with From equal to retained", func(t *testing.T) {
		retry := mustEvidenceLogUpdate(t, fx.tree, fx.u2.Checkpoint)
		if retry.From != fx.u2.Checkpoint {
			t.Fatalf("retry From = %+v, want retained %+v", retry.From, fx.u2.Checkpoint)
		}
		inclusion := mustEvidenceLogInclusion(t, fx.tree, fx.i2.Index)
		prepared, err := Prepare(RetainedState{EvidenceLog: fx.u2.Checkpoint}, &retry, fx.e2, &inclusion)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if prepared.NextState.EvidenceLog != fx.u2.Checkpoint {
			t.Fatalf("NextState = %+v, want %+v", prepared.NextState.EvidenceLog, fx.u2.Checkpoint)
		}
	})

	t.Run("older From is stale even when equal-size consistency would no-op", func(t *testing.T) {
		fromEmpty := mustEvidenceLogUpdate(t, fx.tree, protocol.EmptyCheckpoint())
		if err := protocol.VerifyEvidenceLogUpdate(fx.u2.Checkpoint, fromEmpty); err != nil {
			t.Fatalf("Merkle equal-size no-op against retained head: %v", err)
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u2.Checkpoint}, &fromEmpty, protocol.TypedEvidence{}, nil)
		assertStale(t, err, fx.u2.Checkpoint)
	})

	t.Run("older From with successor behind retained is stale", func(t *testing.T) {
		update := protocol.EvidenceLogUpdate{
			From:       fx.u1.Checkpoint,
			Checkpoint: protocol.EmptyCheckpoint(),
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u2.Checkpoint}, &update, protocol.TypedEvidence{}, nil)
		assertStale(t, err, fx.u2.Checkpoint)
	})

	t.Run("older From with same-size different successor root is stale", func(t *testing.T) {
		update := protocol.EvidenceLogUpdate{
			From:       fx.u1.Checkpoint,
			Checkpoint: protocol.Checkpoint{Size: fx.u2.Checkpoint.Size, Root: forkRoot},
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u2.Checkpoint}, &update, protocol.TypedEvidence{}, nil)
		assertStale(t, err, fx.u2.Checkpoint)
	})

	t.Run("same-size different From root is a fork", func(t *testing.T) {
		update := protocol.EvidenceLogUpdate{
			From:       protocol.Checkpoint{Size: fx.u2.Checkpoint.Size, Root: forkRoot},
			Checkpoint: fx.u2.Checkpoint,
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u2.Checkpoint}, &update, protocol.TypedEvidence{}, nil)
		assertFork(t, err)
	})

	t.Run("newer From is a fork", func(t *testing.T) {
		update := protocol.EvidenceLogUpdate{
			From:       fx.u2.Checkpoint,
			Checkpoint: fx.u2.Checkpoint,
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u1.Checkpoint}, &update, protocol.TypedEvidence{}, nil)
		assertFork(t, err)
	})

	t.Run("rollback successor is a fork", func(t *testing.T) {
		update := protocol.EvidenceLogUpdate{
			From:       fx.u2.Checkpoint,
			Checkpoint: fx.u1.Checkpoint,
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u2.Checkpoint}, &update, protocol.TypedEvidence{}, nil)
		assertFork(t, err)
	})

	t.Run("skip-ahead is a fork", func(t *testing.T) {
		update := protocol.EvidenceLogUpdate{
			From:       fx.u1.Checkpoint,
			Checkpoint: protocol.Checkpoint{Size: 99, Root: forkRoot},
		}
		_, err := Prepare(RetainedState{EvidenceLog: fx.u1.Checkpoint}, &update, protocol.TypedEvidence{}, nil)
		assertFork(t, err)
	})

	t.Run("bad consistency is a fork", func(t *testing.T) {
		tampered := fx.u2
		tampered.ConsistencyProof = append([]protocol.Digest(nil), fx.u2.ConsistencyProof...)
		tampered.ConsistencyProof[0] = forkRoot
		_, err := Prepare(RetainedState{EvidenceLog: fx.u1.Checkpoint}, &tampered, protocol.TypedEvidence{}, nil)
		assertFork(t, err)
	})

	t.Run("missing root inclusion", func(t *testing.T) {
		_, err := Prepare(RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, &fx.u1, fx.e1, nil)
		if !errors.Is(err, ErrMissingRootInclusion) {
			t.Fatalf("error = %v, want ErrMissingRootInclusion", err)
		}
	})

	t.Run("mismatched adjacent root evidence", func(t *testing.T) {
		_, err := Prepare(RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, &fx.u1, fx.e2, &fx.i1)
		if !errors.Is(err, protocol.ErrInvalidLogInclusion) {
			t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
		}
	})
}

func TestPrepareDoesNotReturnStateWhenRootBindingFails(t *testing.T) {
	fx := newLogFixture(t)
	prepared, err := Prepare(RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, &fx.u1, fx.e2, &fx.i1)
	if err == nil {
		t.Fatal("Prepare succeeded with mismatched root evidence")
	}
	if prepared.Log != nil || prepared.NextState.EvidenceLog != (protocol.Checkpoint{}) {
		t.Fatalf("failed Prepare returned installable state: %+v", prepared)
	}
}

func TestVerifyOccurrence(t *testing.T) {
	fx := newLogFixture(t)
	ctx := context.Background()
	i1AtHead := mustEvidenceLogInclusion(t, fx.tree, fx.i1.Index)

	t.Run("valid pair returns domain identity and index", func(t *testing.T) {
		prepared := mustPrepare(t, fx.u1.Checkpoint, fx.u2, fx.e2, fx.i2)
		got, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, i1AtHead)
		if err != nil {
			t.Fatalf("VerifyOccurrence: %v", err)
		}
		want := protocol.VerifiedEvidenceLogBinding{
			Position: protocol.LogPosition{
				Domain: protocol.LogDomainTenantEvidenceV1,
				Index:  fx.i1.Index,
			},
			Evidence: mustIdentity(t, fx.e1),
		}
		if got != want {
			t.Fatalf("binding = %+v, want %+v", got, want)
		}
	})

	t.Run("moved inclusion onto other evidence fails", func(t *testing.T) {
		prepared := mustPrepare(t, fx.u1.Checkpoint, fx.u2, fx.e2, fx.i2)
		_, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, fx.i2)
		if !errors.Is(err, protocol.ErrInvalidLogInclusion) {
			t.Fatalf("error = %v, want ErrInvalidLogInclusion", err)
		}
	})

	t.Run("second call with the same pair returns the cached binding", func(t *testing.T) {
		prepared := mustPrepare(t, fx.u1.Checkpoint, fx.u2, fx.e2, fx.i2)
		first, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, i1AtHead)
		if err != nil {
			t.Fatalf("first VerifyOccurrence: %v", err)
		}
		second, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, i1AtHead)
		if err != nil {
			t.Fatalf("second VerifyOccurrence: %v", err)
		}
		if first != second {
			t.Fatalf("cached binding = %+v, want %+v", second, first)
		}
	})
}

func TestVerifyOccurrenceRejectsSecondOccurrenceOfSameIdentity(t *testing.T) {
	evidence := testLogEvidence("same-payload")
	tree := merklelog.New()
	leafHash := mustLeafHash(t, evidence)
	if _, _, err := tree.AppendHash(leafHash); err != nil {
		t.Fatalf("append index 0: %v", err)
	}
	if _, _, err := tree.AppendHash(leafHash); err != nil {
		t.Fatalf("append index 1: %v", err)
	}
	checkpoint := mustCheckpoint(t, tree, tree.Size())
	first := mustEvidenceLogInclusion(t, tree, 0)
	second := mustEvidenceLogInclusion(t, tree, 1)
	if err := protocol.VerifyEvidenceLogInclusion(checkpoint, evidence, first); err != nil {
		t.Fatalf("Merkle inclusion at index 0: %v", err)
	}
	if err := protocol.VerifyEvidenceLogInclusion(checkpoint, evidence, second); err != nil {
		t.Fatalf("Merkle inclusion at index 1: %v", err)
	}

	update := mustEvidenceLogUpdate(t, tree, protocol.EmptyCheckpoint())
	prepared := mustPrepare(t, protocol.EmptyCheckpoint(), update, evidence, first)
	_, err := prepared.Log.VerifyOccurrence(context.Background(), evidence, second)
	if !errors.Is(err, ErrSecondOccurrence) {
		t.Fatalf("error = %v, want ErrSecondOccurrence", err)
	}
}

func TestPrepareMemoizesRootOccurrence(t *testing.T) {
	fx := newLogFixture(t)
	prepared := mustPrepare(t, protocol.EmptyCheckpoint(), fx.u1, fx.e1, fx.i1)
	ctx := context.Background()

	got, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, fx.i1)
	if err != nil {
		t.Fatalf("memo hit: %v", err)
	}
	want := protocol.VerifiedEvidenceLogBinding{
		Position: protocol.LogPosition{
			Domain: protocol.LogDomainTenantEvidenceV1,
			Index:  fx.i1.Index,
		},
		Evidence: mustIdentity(t, fx.e1),
	}
	if got != want {
		t.Fatalf("root binding = %+v, want %+v", got, want)
	}

	other := fx.i1
	other.Index = fx.i1.Index + 1
	_, err = prepared.Log.VerifyOccurrence(ctx, fx.e1, other)
	if !errors.Is(err, ErrSecondOccurrence) {
		t.Fatalf("conflicting root pair error = %v, want ErrSecondOccurrence", err)
	}
}

func TestVerifyOccurrenceDoesNotMemoizeFailure(t *testing.T) {
	fx := newLogFixture(t)
	prepared := mustPrepare(t, fx.u1.Checkpoint, fx.u2, fx.e2, fx.i2)
	ctx := context.Background()
	if _, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, fx.i2); !errors.Is(err, protocol.ErrInvalidLogInclusion) {
		t.Fatalf("mismatched pair error = %v, want ErrInvalidLogInclusion", err)
	}
	i1AtHead := mustEvidenceLogInclusion(t, fx.tree, fx.i1.Index)
	if _, err := prepared.Log.VerifyOccurrence(ctx, fx.e1, i1AtHead); err != nil {
		t.Fatalf("valid pair after failure: %v", err)
	}
}

type logFixture struct {
	tree   *merklelog.Tree
	e1, e2 protocol.TypedEvidence
	u1, u2 protocol.EvidenceLogUpdate
	i1, i2 protocol.EvidenceLogInclusion
}

func newLogFixture(t *testing.T) logFixture {
	t.Helper()
	tree := merklelog.New()
	e1 := testLogEvidence("one")
	u1, i1 := mustAppendLog(t, tree, protocol.EmptyCheckpoint(), e1)
	e2 := testLogEvidence("two")
	u2, i2 := mustAppendLog(t, tree, u1.Checkpoint, e2)
	return logFixture{tree: tree, e1: e1, e2: e2, u1: u1, u2: u2, i1: i1, i2: i2}
}

func mustPrepare(t *testing.T, retained protocol.Checkpoint, update protocol.EvidenceLogUpdate, evidence protocol.TypedEvidence, inclusion protocol.EvidenceLogInclusion) PreparedUpdate {
	t.Helper()
	prepared, err := Prepare(RetainedState{EvidenceLog: retained}, &update, evidence, &inclusion)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return prepared
}

func assertStale(t *testing.T, err error, want protocol.Checkpoint) {
	t.Helper()
	var stale *CheckpointStaleError
	if !errors.As(err, &stale) {
		t.Fatalf("error = %v, want CheckpointStaleError", err)
	}
	if stale.LatestCheckpoint() != want {
		t.Fatalf("stale checkpoint = %+v, want %+v", stale.LatestCheckpoint(), want)
	}
	if errors.Is(err, protocol.ErrInvalidLogUpdate) {
		t.Fatalf("stale error also reported as a log fork: %v", err)
	}
}

func assertFork(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, protocol.ErrInvalidLogUpdate) {
		t.Fatalf("error = %v, want ErrInvalidLogUpdate", err)
	}
	var stale *CheckpointStaleError
	if errors.As(err, &stale) {
		t.Fatalf("fork error reported as stale checkpoint: %v", err)
	}
}

func testLogEvidence(label string) protocol.TypedEvidence {
	return protocol.TypedEvidence{
		ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
		Encoded: protocol.Encoded{
			MediaType: "application/test+json",
			Bytes:     []byte(label),
		},
	}
}

func mustIdentity(t *testing.T, evidence protocol.TypedEvidence) protocol.Digest {
	t.Helper()
	identity, err := evidence.Identity()
	if err != nil {
		t.Fatalf("evidence identity: %v", err)
	}
	return identity
}

func mustLeafHash(t *testing.T, evidence protocol.TypedEvidence) []byte {
	t.Helper()
	leafHash, err := protocol.LeafHash(mustIdentity(t, evidence))
	if err != nil {
		t.Fatalf("leaf hash: %v", err)
	}
	return leafHash
}

func mustForkRoot(t *testing.T) protocol.Digest {
	t.Helper()
	root, err := protocol.EncodeDigest(bytes.Repeat([]byte{0xff}, rfc6962.DefaultHasher.Size()))
	if err != nil {
		t.Fatalf("encode fork root: %v", err)
	}
	return root
}

func mustCheckpoint(t *testing.T, tree *merklelog.Tree, size uint64) protocol.Checkpoint {
	t.Helper()
	root, err := tree.RootAt(size)
	if err != nil {
		t.Fatalf("root at %d: %v", size, err)
	}
	checkpoint, err := protocol.NewCheckpoint(size, root)
	if err != nil {
		t.Fatalf("checkpoint at %d: %v", size, err)
	}
	return checkpoint
}

func mustEvidenceLogUpdate(t *testing.T, tree *merklelog.Tree, from protocol.Checkpoint) protocol.EvidenceLogUpdate {
	t.Helper()
	current := mustCheckpoint(t, tree, tree.Size())
	consistency, err := tree.ConsistencyProof(from.Size, current.Size)
	if err != nil {
		t.Fatalf("consistency proof: %v", err)
	}
	return protocol.EvidenceLogUpdate{
		From:             from,
		Checkpoint:       current,
		ConsistencyProof: protocol.EncodeProof(consistency),
	}
}

func mustEvidenceLogInclusion(t *testing.T, tree *merklelog.Tree, index uint64) protocol.EvidenceLogInclusion {
	t.Helper()
	inclusion, err := tree.InclusionProof(index, tree.Size())
	if err != nil {
		t.Fatalf("inclusion proof: %v", err)
	}
	return protocol.EvidenceLogInclusion{
		Index:          index,
		InclusionProof: protocol.EncodeProof(inclusion),
	}
}

func mustAppendLog(t *testing.T, tree *merklelog.Tree, from protocol.Checkpoint, evidence protocol.TypedEvidence) (protocol.EvidenceLogUpdate, protocol.EvidenceLogInclusion) {
	t.Helper()
	if _, _, err := tree.AppendHash(mustLeafHash(t, evidence)); err != nil {
		t.Fatalf("append leaf: %v", err)
	}
	return mustEvidenceLogUpdate(t, tree, from), mustEvidenceLogInclusion(t, tree, tree.Size()-1)
}
