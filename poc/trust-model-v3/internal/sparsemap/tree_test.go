package sparsemap

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestProofAuthenticatesAbsenceMembershipAndReplacement(t *testing.T) {
	tree := New("tenant-acme")
	aliceKey := sha256.Sum256([]byte("alice"))
	aliceV1 := sha256.Sum256([]byte("alice-v1"))
	aliceV2 := sha256.Sum256([]byte("alice-v2"))
	bobKey := sha256.Sum256([]byte("bob"))
	bobV1 := sha256.Sum256([]byte("bob-v1"))

	emptyRoot := tree.Root()
	absence, err := tree.Proof(aliceKey[:])
	if err != nil {
		t.Fatalf("Alice absence proof: %v", err)
	}
	if got, want := len(absence), Height; got != want {
		t.Fatalf("absence proof hashes = %d, want %d", got, want)
	}
	provenEmptyRoot, err := RootFromProof("tenant-acme", aliceKey[:], nil, absence)
	if err != nil {
		t.Fatalf("reconstruct empty root: %v", err)
	}
	if !bytes.Equal(provenEmptyRoot, emptyRoot) {
		t.Fatalf("absence root = %x, want %x", provenEmptyRoot, emptyRoot)
	}

	aliceRoot, err := tree.Set(aliceKey[:], aliceV1[:])
	if err != nil {
		t.Fatalf("set Alice v1: %v", err)
	}
	fromSamePath, err := RootFromProof("tenant-acme", aliceKey[:], aliceV1[:], absence)
	if err != nil {
		t.Fatalf("replace absent Alice leaf: %v", err)
	}
	if !bytes.Equal(fromSamePath, aliceRoot) {
		t.Fatalf("replacement root = %x, want writer root %x", fromSamePath, aliceRoot)
	}

	if _, err := tree.Set(bobKey[:], bobV1[:]); err != nil {
		t.Fatalf("set Bob: %v", err)
	}
	oldRoot := tree.Root()
	aliceProof, err := tree.Proof(aliceKey[:])
	if err != nil {
		t.Fatalf("Alice membership proof: %v", err)
	}
	provenOldRoot, err := RootFromProof("tenant-acme", aliceKey[:], aliceV1[:], aliceProof)
	if err != nil {
		t.Fatalf("reconstruct old root: %v", err)
	}
	if !bytes.Equal(provenOldRoot, oldRoot) {
		t.Fatalf("membership root = %x, want %x", provenOldRoot, oldRoot)
	}

	wantNewRoot, err := RootFromProof("tenant-acme", aliceKey[:], aliceV2[:], aliceProof)
	if err != nil {
		t.Fatalf("reconstruct replacement root: %v", err)
	}
	newRoot, err := tree.Set(aliceKey[:], aliceV2[:])
	if err != nil {
		t.Fatalf("set Alice v2: %v", err)
	}
	if !bytes.Equal(newRoot, wantNewRoot) {
		t.Fatalf("replacement proof root = %x, want writer root %x", wantNewRoot, newRoot)
	}

	tampered := cloneHashes(aliceProof)
	tampered[0][0] ^= 0xff
	tamperedRoot, err := RootFromProof("tenant-acme", aliceKey[:], aliceV1[:], tampered)
	if err != nil {
		t.Fatalf("reconstruct tampered proof: %v", err)
	}
	if bytes.Equal(tamperedRoot, oldRoot) {
		t.Fatal("tampered sibling proof reconstructed the accepted root")
	}
}

func TestRootIsIndependentOfInsertionOrderAndDomainSeparated(t *testing.T) {
	aliceKey := sha256.Sum256([]byte("alice"))
	aliceValue := sha256.Sum256([]byte("alice-value"))
	bobKey := sha256.Sum256([]byte("bob"))
	bobValue := sha256.Sum256([]byte("bob-value"))

	left := New("tenant-acme")
	if _, err := left.Set(aliceKey[:], aliceValue[:]); err != nil {
		t.Fatalf("left set Alice: %v", err)
	}
	if _, err := left.Set(bobKey[:], bobValue[:]); err != nil {
		t.Fatalf("left set Bob: %v", err)
	}

	right := New("tenant-acme")
	if _, err := right.Set(bobKey[:], bobValue[:]); err != nil {
		t.Fatalf("right set Bob: %v", err)
	}
	if _, err := right.Set(aliceKey[:], aliceValue[:]); err != nil {
		t.Fatalf("right set Alice: %v", err)
	}

	if !bytes.Equal(left.Root(), right.Root()) {
		t.Fatalf("roots depend on insertion order: %x != %x", left.Root(), right.Root())
	}

	otherTenant := New("tenant-other")
	if _, err := otherTenant.Set(aliceKey[:], aliceValue[:]); err != nil {
		t.Fatalf("other tenant set Alice: %v", err)
	}
	if _, err := otherTenant.Set(bobKey[:], bobValue[:]); err != nil {
		t.Fatalf("other tenant set Bob: %v", err)
	}
	if bytes.Equal(left.Root(), otherTenant.Root()) {
		t.Fatal("tenant-domain-separated maps produced the same root")
	}
}

func TestProofAndRootResultsDoNotExposeMutableStorage(t *testing.T) {
	tree := New("tenant-acme")
	key := sha256.Sum256([]byte("key"))
	value := sha256.Sum256([]byte("value"))
	root, err := tree.Set(key[:], value[:])
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	path, err := tree.Proof(key[:])
	if err != nil {
		t.Fatalf("Proof: %v", err)
	}

	root[0] ^= 0xff
	path[0][0] ^= 0xff

	wantRoot := tree.Root()
	freshPath, err := tree.Proof(key[:])
	if err != nil {
		t.Fatalf("fresh Proof: %v", err)
	}
	gotRoot, err := RootFromProof("tenant-acme", key[:], value[:], freshPath)
	if err != nil {
		t.Fatalf("RootFromProof: %v", err)
	}
	if !bytes.Equal(gotRoot, wantRoot) {
		t.Fatalf("caller mutation changed tree storage: got %x, want %x", gotRoot, wantRoot)
	}
}

func TestRejectsMalformedKeysValuesAndProofs(t *testing.T) {
	tree := New("tenant-acme")
	key := make([]byte, sha256.Size)
	value := make([]byte, sha256.Size)
	path, err := tree.Proof(key)
	if err != nil {
		t.Fatalf("Proof: %v", err)
	}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{name: "short key proof", call: func() error { _, err := tree.Proof(key[:31]); return err }},
		{name: "short key set", call: func() error { _, err := tree.Set(key[:31], value); return err }},
		{name: "short value set", call: func() error { _, err := tree.Set(key, value[:31]); return err }},
		{name: "short root key", call: func() error { _, err := RootFromProof("tenant-acme", key[:31], value, path); return err }},
		{name: "short root value", call: func() error { _, err := RootFromProof("tenant-acme", key, value[:31], path); return err }},
		{name: "short path", call: func() error { _, err := RootFromProof("tenant-acme", key, value, path[:Height-1]); return err }},
		{name: "short sibling", call: func() error {
			bad := cloneHashes(path)
			bad[10] = bad[10][:31]
			_, err := RootFromProof("tenant-acme", key, value, bad)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatal("malformed input unexpectedly accepted")
			}
		})
	}
}

func FuzzProofRoundTrip(f *testing.F) {
	f.Add("tenant-acme", []byte("key"), []byte("value"))
	f.Fuzz(func(t *testing.T, domain string, keyInput, valueInput []byte) {
		key := sha256.Sum256(append([]byte("target\x00"), keyInput...))
		value := sha256.Sum256(valueInput)
		otherKey := sha256.Sum256(append([]byte("other\x00"), keyInput...))
		otherValue := sha256.Sum256(append([]byte("other\x00"), valueInput...))
		if bytes.Equal(key[:], otherKey[:]) {
			t.Skip("SHA-256 collision between test keys")
		}
		tree := New(domain)
		if _, err := tree.Set(otherKey[:], otherValue[:]); err != nil {
			t.Fatalf("Set(other): %v", err)
		}
		oldRoot := tree.Root()
		path, err := tree.Proof(key[:])
		if err != nil {
			t.Fatalf("Proof: %v", err)
		}
		provenOldRoot, err := RootFromProof(domain, key[:], nil, path)
		if err != nil {
			t.Fatalf("RootFromProof(absent): %v", err)
		}
		if !bytes.Equal(provenOldRoot, oldRoot) {
			t.Fatalf("absence root = %x, want %x", provenOldRoot, oldRoot)
		}
		newRoot, err := tree.Set(key[:], value[:])
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		provenNewRoot, err := RootFromProof(domain, key[:], value[:], path)
		if err != nil {
			t.Fatalf("RootFromProof(present): %v", err)
		}
		if !bytes.Equal(provenNewRoot, newRoot) {
			t.Fatalf("replacement root = %x, want %x", provenNewRoot, newRoot)
		}
	})
}
