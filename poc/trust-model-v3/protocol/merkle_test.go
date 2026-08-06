package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/internal/merklelog"
)

func TestKeyHistoryUpdateHasMerkleInclusionAndConsistencyProofs(t *testing.T) {
	identityID := "identity-alice"
	previous := EmptyKeyHistoryHead(identityID)
	records := []KeyEventRecord(nil)

	first, err := NewKeyHistoryUpdate(previous, records, "state-0", KeyEvent{Kind: KeyEventEnrollment})
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	if _, err := VerifyKeyHistoryUpdate(previous, first); err != nil {
		t.Fatalf("verify first update: %v", err)
	}
	if first.Head.Root != first.Event.Hash {
		t.Fatalf("single-leaf history root = %q, want leaf hash %q", first.Head.Root, first.Event.Hash)
	}
	if len(first.ConsistencyProof) != 0 {
		t.Fatalf("empty-to-one consistency proof has %d hashes, want 0", len(first.ConsistencyProof))
	}

	records = append(records, first.Event)
	second, err := NewKeyHistoryUpdate(first.Head, records, "state-1", KeyEvent{Kind: KeyEventRotation})
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if _, err := VerifyKeyHistoryUpdate(first.Head, second); err != nil {
		t.Fatalf("verify second update: %v", err)
	}
	if second.Head.Root == second.Event.Hash {
		t.Fatal("two-leaf history root collapsed to the latest leaf hash")
	}
	if len(second.ConsistencyProof) == 0 {
		t.Fatal("one-to-two history extension has no consistency proof")
	}
	if len(second.InclusionProof) == 0 {
		t.Fatal("second key event has no inclusion proof")
	}

	for _, tc := range []struct {
		name   string
		mutate func(*KeyHistoryUpdate)
	}{
		{name: "event", mutate: func(update *KeyHistoryUpdate) { update.Event.ResultingStateDigest = "attacker-state" }},
		{name: "leaf hash", mutate: func(update *KeyHistoryUpdate) { update.Event.Hash = allFFHash }},
		{name: "head root", mutate: func(update *KeyHistoryUpdate) { update.Head.Root = allFFHash }},
		{name: "inclusion proof", mutate: func(update *KeyHistoryUpdate) { update.InclusionProof[0] = allFFHash }},
		{name: "consistency proof", mutate: func(update *KeyHistoryUpdate) { update.ConsistencyProof[0] = allFFHash }},
	} {
		t.Run("rejects tampered "+tc.name, func(t *testing.T) {
			tampered := cloneKeyHistoryUpdateForTest(second)
			tc.mutate(&tampered)
			if _, err := VerifyKeyHistoryUpdate(first.Head, tampered); !errors.Is(err, ErrInvalidKeyHistory) {
				t.Fatalf("error = %v, want ErrInvalidKeyHistory", err)
			}
		})
	}
}

func TestKeyHistoryMerkleProofsStayLogarithmic(t *testing.T) {
	identityID := "identity-alice"
	head := EmptyKeyHistoryHead(identityID)
	records := []KeyEventRecord(nil)
	for i := 0; i < 128; i++ {
		update, err := NewKeyHistoryUpdate(head, records, fmt.Sprintf("state-%d", i), KeyEvent{Kind: KeyEventRotation})
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		if _, err := VerifyKeyHistoryUpdate(head, update); err != nil {
			t.Fatalf("verify update %d: %v", i, err)
		}
		if got := len(update.InclusionProof); got > 7 {
			t.Fatalf("inclusion proof at size %d has %d hashes, want <= 7", update.Head.Size, got)
		}
		if got := len(update.ConsistencyProof); got > 8 {
			t.Fatalf("consistency proof at size %d has %d hashes, want <= 8", update.Head.Size, got)
		}
		records = append(records, update.Event)
		head = update.Head
	}
}

func TestDeliveryLogUpdateVerifiesSelectiveInclusionAndConsistency(t *testing.T) {
	tree := merklelog.New()
	records := make([]DeliveryRecord, 32)
	for i := range records {
		event := DeliveryLogEvent{
			Kind: DeliveryLogEventDelivery,
			Delivery: &SignedDelivery{Attestation: ContentAttestation{
				Protocol:      DeliveryProtocol,
				FulfillmentID: fmt.Sprintf("fulfillment-%d", i),
			}},
		}
		record, err := NewDeliveryRecord(uint64(i), event)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		encoded, err := json.Marshal(deliveryRecordMaterial(record))
		if err != nil {
			t.Fatalf("encode record %d: %v", i, err)
		}
		index, leafHash, err := tree.Append(encoded)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if index != record.Index || !bytes.Equal(leafHash, mustDecodeHashForTest(t, record.Hash)) {
			t.Fatalf("record %d does not match appended Merkle leaf", i)
		}
		records[i] = record
	}

	previousRoot, err := tree.RootAt(3)
	if err != nil {
		t.Fatalf("old root: %v", err)
	}
	currentRoot, err := tree.Root()
	if err != nil {
		t.Fatalf("current root: %v", err)
	}
	consistency, err := tree.ConsistencyProof(3, tree.Size())
	if err != nil {
		t.Fatalf("consistency proof: %v", err)
	}
	selected := []uint64{7, 31}
	entries := make([]DeliveryLogEntryProof, len(selected))
	for i, index := range selected {
		inclusion, err := tree.InclusionProof(index, tree.Size())
		if err != nil {
			t.Fatalf("inclusion proof for %d: %v", index, err)
		}
		entries[i] = DeliveryLogEntryProof{
			Record:         records[index],
			InclusionProof: encodeProof(inclusion),
		}
	}
	update := DeliveryLogUpdate{
		Checkpoint:       Checkpoint{Size: tree.Size(), Root: encodeHash(currentRoot)},
		ConsistencyProof: encodeProof(consistency),
		Entries:          entries,
	}
	previous := Checkpoint{Size: 3, Root: encodeHash(previousRoot)}
	if err := VerifyDeliveryLogUpdate(previous, update); err != nil {
		t.Fatalf("verify selective update: %v", err)
	}
	if got, want := len(update.Entries), 2; got != want {
		t.Fatalf("disclosed entries = %d, want %d", got, want)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*DeliveryLogUpdate)
	}{
		{name: "checkpoint root", mutate: func(update *DeliveryLogUpdate) { update.Checkpoint.Root = allFFHash }},
		{name: "consistency proof", mutate: func(update *DeliveryLogUpdate) { update.ConsistencyProof[0] = allFFHash }},
		{name: "record event", mutate: func(update *DeliveryLogUpdate) { update.Entries[0].Record.Event.Kind = DeliveryLogEventRotation }},
		{name: "record leaf hash", mutate: func(update *DeliveryLogUpdate) { update.Entries[0].Record.Hash = allFFHash }},
		{name: "inclusion proof", mutate: func(update *DeliveryLogUpdate) { update.Entries[0].InclusionProof[0] = allFFHash }},
	} {
		t.Run("rejects tampered "+tc.name, func(t *testing.T) {
			tampered := cloneDeliveryLogUpdateForTest(update)
			tc.mutate(&tampered)
			if err := VerifyDeliveryLogUpdate(previous, tampered); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("error = %v, want ErrInvalidRecord", err)
			}
		})
	}
}

func TestAuthenticatedMapUsesTenantSeparatedCONIKSSparseTree(t *testing.T) {
	alice := "identity-alice"
	bob := "identity-bob"
	aliceEnrollment, err := NewKeyHistoryUpdate(
		EmptyKeyHistoryHead(alice), nil, "state-alice-0", KeyEvent{Kind: KeyEventEnrollment},
	)
	if err != nil {
		t.Fatalf("Alice enrollment history: %v", err)
	}
	bobEnrollment, err := NewKeyHistoryUpdate(
		EmptyKeyHistoryHead(bob), nil, "state-bob-0", KeyEvent{Kind: KeyEventEnrollment},
	)
	if err != nil {
		t.Fatalf("Bob enrollment history: %v", err)
	}
	aliceRotation, err := NewKeyHistoryUpdate(
		aliceEnrollment.Head,
		[]KeyEventRecord{aliceEnrollment.Event},
		"state-alice-1",
		KeyEvent{Kind: KeyEventRotation},
	)
	if err != nil {
		t.Fatalf("Alice rotation history: %v", err)
	}

	heads := map[string]KeyHistoryHead{alice: aliceEnrollment.Head, bob: bobEnrollment.Head}
	oldRoot, err := KeyHistoryMapRoot("tenant-acme", heads)
	if err != nil {
		t.Fatalf("old map root: %v", err)
	}
	update, err := NewAuthenticatedMapUpdate("tenant-acme", heads, aliceRotation)
	if err != nil {
		t.Fatalf("map update: %v", err)
	}
	if update.PreviousRoot != oldRoot {
		t.Fatalf("previous root = %q, want %q", update.PreviousRoot, oldRoot)
	}
	if len(update.SiblingHashes) != 256 {
		t.Fatalf("sibling path = %d hashes, want 256", len(update.SiblingHashes))
	}
	nextHead, err := VerifyAuthenticatedMapUpdate("tenant-acme", oldRoot, update)
	if err != nil {
		t.Fatalf("verify map update: %v", err)
	}
	if nextHead != aliceRotation.Head {
		t.Fatalf("verified head = %#v, want %#v", nextHead, aliceRotation.Head)
	}

	nextHeads := map[string]KeyHistoryHead{alice: aliceRotation.Head, bob: bobEnrollment.Head}
	wantRoot, err := KeyHistoryMapRoot("tenant-acme", nextHeads)
	if err != nil {
		t.Fatalf("next map root: %v", err)
	}
	if update.Root != wantRoot {
		t.Fatalf("successor root = %q, want %q", update.Root, wantRoot)
	}
	otherTenantRoot, err := KeyHistoryMapRoot("tenant-other", nextHeads)
	if err != nil {
		t.Fatalf("other tenant map root: %v", err)
	}
	if otherTenantRoot == wantRoot {
		t.Fatal("different tenant domains produced the same sparse-map root")
	}

	tampered := cloneAuthenticatedMapUpdateForTest(update)
	tampered.SiblingHashes[0] = allFFHash
	if _, err := VerifyAuthenticatedMapUpdate("tenant-acme", oldRoot, tampered); !errors.Is(err, ErrInvalidMapProof) {
		t.Fatalf("tampered path error = %v, want ErrInvalidMapProof", err)
	}
	if _, err := VerifyAuthenticatedMapUpdate("tenant-other", oldRoot, update); !errors.Is(err, ErrInvalidMapProof) {
		t.Fatalf("cross-tenant proof error = %v, want ErrInvalidMapProof", err)
	}
}

const allFFHash = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

func mustDecodeHashForTest(t *testing.T, encoded string) []byte {
	t.Helper()
	hash, err := decodeHash(encoded)
	if err != nil {
		t.Fatalf("decode hash: %v", err)
	}
	return hash
}

func cloneKeyHistoryUpdateForTest(in KeyHistoryUpdate) KeyHistoryUpdate {
	out := in
	out.Event.Event = cloneKeyEventForTest(in.Event.Event)
	out.InclusionProof = append([]string(nil), in.InclusionProof...)
	out.ConsistencyProof = append([]string(nil), in.ConsistencyProof...)
	return out
}

func cloneKeyEventForTest(in KeyEvent) KeyEvent {
	out := in
	if in.Enrollment != nil {
		enrollment := *in.Enrollment
		out.Enrollment = &enrollment
	}
	if in.Rotation != nil {
		rotation := *in.Rotation
		out.Rotation = &rotation
	}
	if in.RotationMarker != nil {
		marker := *in.RotationMarker
		out.RotationMarker = &marker
	}
	return out
}

func cloneDeliveryLogUpdateForTest(in DeliveryLogUpdate) DeliveryLogUpdate {
	out := in
	out.ConsistencyProof = append([]string(nil), in.ConsistencyProof...)
	out.Entries = make([]DeliveryLogEntryProof, len(in.Entries))
	for i, entry := range in.Entries {
		out.Entries[i] = entry
		out.Entries[i].InclusionProof = append([]string(nil), entry.InclusionProof...)
		if entry.Record.Event.Delivery != nil {
			delivery := *entry.Record.Event.Delivery
			out.Entries[i].Record.Event.Delivery = &delivery
		}
		if entry.Record.Event.Rotation != nil {
			rotation := *entry.Record.Event.Rotation
			out.Entries[i].Record.Event.Rotation = &rotation
		}
	}
	return out
}

func cloneAuthenticatedMapUpdateForTest(in AuthenticatedMapUpdate) AuthenticatedMapUpdate {
	out := in
	out.SiblingHashes = append([]string(nil), in.SiblingHashes...)
	out.KeyHistory = cloneKeyHistoryUpdateForTest(in.KeyHistory)
	return out
}
