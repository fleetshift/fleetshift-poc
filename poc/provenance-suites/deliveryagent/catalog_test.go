package deliveryagent

import (
	"errors"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

func TestEvidenceCatalogRejectsDuplicateIdentities(t *testing.T) {
	root := catalogItem("same", "root support")
	identical := cloneCatalogItem(root)
	supportConflict := cloneCatalogItem(root)
	supportConflict.Support.Bytes = []byte("different support")
	inclusionConflict := cloneCatalogItem(root)
	inclusionConflict.EvidenceLog = &protocol.EvidenceLogInclusion{Index: 7, InclusionProof: []protocol.Digest{protocol.DigestBytes([]byte("proof"))}}

	for _, test := range []struct {
		name  string
		clone protocol.Item
	}{
		{name: "identical attached material", clone: identical},
		{name: "conflicting support material", clone: supportConflict},
		{name: "conflicting inclusion material", clone: inclusionConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: root, Supporting: []protocol.Item{test.clone}}, catalogLookup(nil), defaultVerificationLimits())
			if !errors.Is(err, errDuplicateEvidenceIdentity) {
				t.Fatalf("newEvidenceCatalog error = %v, want duplicate identity", err)
			}
		})
	}
}

func TestEvidenceCatalogPreservesEmptyEvidenceBytesAndIdentity(t *testing.T) {
	evidence := protocol.TypedEvidence{
		ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
		Encoded: protocol.Encoded{
			MediaType: "application/test",
			Bytes:     make([]byte, 0),
		},
	}
	wantID, err := evidence.Identity()
	if err != nil {
		t.Fatalf("evidence identity: %v", err)
	}

	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{
		Root: protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: evidence}},
	}, catalogLookup(nil), defaultVerificationLimits())
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	if catalog.rootID != wantID {
		t.Fatalf("catalog root identity = %q, want %q", catalog.rootID, wantID)
	}
	if got := catalog.item(catalog.rootID).Evidence.Bytes; got == nil {
		t.Fatal("catalog changed non-nil empty evidence bytes to nil")
	}
}

func TestEvidenceCatalogEnforcesPackageBoundsAtExactLimit(t *testing.T) {
	validHash := protocol.DigestBytes([]byte("proof hash"))
	rootWithInclusion := catalogItem("r", "")
	rootWithInclusion.EvidenceLog = &protocol.EvidenceLogInclusion{Index: 0, InclusionProof: []protocol.Digest{validHash}}
	supportWithInclusion := catalogItem("s", "")
	supportWithInclusion.EvidenceLog = &protocol.EvidenceLogInclusion{Index: 1, InclusionProof: []protocol.Digest{validHash}}
	byteItem := func(body, support string) protocol.Item {
		item := catalogItem(body, support)
		item.Evidence.ProvenanceType = ""
		item.Evidence.MediaType = ""
		item.Support.MediaType = ""
		return item
	}
	tests := []struct {
		name      string
		pkg       resourcemanager.DeliveryPackage
		limits    verificationLimits
		wantLimit bool
	}{
		{
			name: "statement count exact",
			pkg: resourcemanager.DeliveryPackage{
				Root:       catalogItem("r", ""),
				Supporting: []protocol.Item{catalogItem("s", "")},
			},
			limits:    limitsWith(verificationLimits{maxStatements: 2}),
			wantLimit: false,
		},
		{
			name: "statement count over",
			pkg: resourcemanager.DeliveryPackage{
				Root:       catalogItem("r", ""),
				Supporting: []protocol.Item{catalogItem("s1", ""), catalogItem("s2", "")},
			},
			limits:    limitsWith(verificationLimits{maxStatements: 2}),
			wantLimit: true,
		},
		{
			name:   "per item evidence exact",
			pkg:    resourcemanager.DeliveryPackage{Root: byteItem(strings.Repeat("e", 4), "")},
			limits: limitsWith(verificationLimits{maxEvidencePerItem: 4}),
		},
		{
			name:      "per item evidence over",
			pkg:       resourcemanager.DeliveryPackage{Root: byteItem(strings.Repeat("e", 5), "")},
			limits:    limitsWith(verificationLimits{maxEvidencePerItem: 4}),
			wantLimit: true,
		},
		{
			name: "total evidence exact",
			pkg: resourcemanager.DeliveryPackage{
				Root:       byteItem(strings.Repeat("r", 3), ""),
				Supporting: []protocol.Item{byteItem(strings.Repeat("s", 3), "")},
			},
			limits: limitsWith(verificationLimits{maxTotalEvidence: 6}),
		},
		{
			name: "total evidence over",
			pkg: resourcemanager.DeliveryPackage{
				Root:       byteItem(strings.Repeat("r", 3), ""),
				Supporting: []protocol.Item{byteItem(strings.Repeat("s", 4), "")},
			},
			limits:    limitsWith(verificationLimits{maxTotalEvidence: 6}),
			wantLimit: true,
		},
		{
			name:   "per item support exact",
			pkg:    resourcemanager.DeliveryPackage{Root: byteItem("r", strings.Repeat("s", 4))},
			limits: limitsWith(verificationLimits{maxSupportPerItem: 4}),
		},
		{
			name:      "per item support over",
			pkg:       resourcemanager.DeliveryPackage{Root: byteItem("r", strings.Repeat("s", 5))},
			limits:    limitsWith(verificationLimits{maxSupportPerItem: 4}),
			wantLimit: true,
		},
		{
			name: "total support exact",
			pkg: resourcemanager.DeliveryPackage{
				Root:       byteItem("r", "ssss"),
				Supporting: []protocol.Item{byteItem("s", "tttt")},
			},
			limits: limitsWith(verificationLimits{maxTotalSupport: 8}),
		},
		{
			name: "total support over",
			pkg: resourcemanager.DeliveryPackage{
				Root:       byteItem("r", "ssss"),
				Supporting: []protocol.Item{byteItem("s", "ttttt")},
			},
			limits:    limitsWith(verificationLimits{maxTotalSupport: 8}),
			wantLimit: true,
		},
		{
			name: "per proof hashes exact",
			pkg: resourcemanager.DeliveryPackage{
				Root: catalogItem("r", ""),
				EvidenceLog: &protocol.EvidenceLogUpdate{
					ConsistencyProof: []protocol.Digest{validHash},
				},
			},
			limits: limitsWith(verificationLimits{maxProofHashes: 1}),
		},
		{
			name: "per proof hashes over",
			pkg: resourcemanager.DeliveryPackage{
				Root: catalogItem("r", ""),
				EvidenceLog: &protocol.EvidenceLogUpdate{
					ConsistencyProof: []protocol.Digest{validHash, validHash},
				},
			},
			limits:    limitsWith(verificationLimits{maxProofHashes: 1}),
			wantLimit: true,
		},
		{
			name: "encoded proof bytes exact",
			pkg: resourcemanager.DeliveryPackage{
				Root: rootWithInclusion,
				EvidenceLog: &protocol.EvidenceLogUpdate{
					ConsistencyProof: []protocol.Digest{validHash},
				},
			},
			limits: limitsWith(verificationLimits{maxProofBytes: 2 * len(validHash)}),
		},
		{
			name: "encoded proof bytes over",
			pkg: resourcemanager.DeliveryPackage{
				Root:       rootWithInclusion,
				Supporting: []protocol.Item{supportWithInclusion},
				EvidenceLog: &protocol.EvidenceLogUpdate{
					ConsistencyProof: []protocol.Digest{validHash},
				},
			},
			limits:    limitsWith(verificationLimits{maxProofBytes: 2 * len(validHash)}),
			wantLimit: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newEvidenceCatalog(test.pkg, catalogLookup(nil), test.limits)
			if test.wantLimit {
				if !errors.Is(err, errVerificationWorkLimit) {
					t.Fatalf("newEvidenceCatalog error = %v, want work limit", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("newEvidenceCatalog at exact limit: %v", err)
			}
		})
	}
}

func TestEvidenceCatalogBoundsEnvelopeFieldsAlongsideBytes(t *testing.T) {
	exact := protocol.Item{SignedStatement: protocol.SignedStatement{
		Evidence: protocol.TypedEvidence{
			ProvenanceType: "pp",
			Encoded: protocol.Encoded{
				MediaType: "mm",
				Bytes:     []byte("e"),
			},
		},
		Support: protocol.SupportMaterial(protocol.Encoded{
			MediaType: "sss",
			Bytes:     []byte("u"),
		}),
	}}
	_, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: exact}, catalogLookup(nil), limitsWith(verificationLimits{
		maxEvidencePerItem: 5,
		maxTotalEvidence:   5,
		maxSupportPerItem:  4,
		maxTotalSupport:    4,
	}))
	if err != nil {
		t.Fatalf("newEvidenceCatalog at exact envelope byte limits: %v", err)
	}

	provenanceOver := protocol.Item{}
	provenanceOver.Evidence.ProvenanceType = protocol.ProvenanceType(strings.Repeat("p", 5))
	evidenceMediaTypeOver := protocol.Item{}
	evidenceMediaTypeOver.Evidence.MediaType = protocol.MediaType(strings.Repeat("m", 5))
	supportMediaTypeOver := protocol.Item{}
	supportMediaTypeOver.Support.MediaType = protocol.MediaType(strings.Repeat("s", 5))
	evidenceAggregate := resourcemanager.DeliveryPackage{
		Root: protocol.Item{SignedStatement: protocol.SignedStatement{
			Evidence: protocol.TypedEvidence{ProvenanceType: "rr"},
		}},
		Supporting: []protocol.Item{{SignedStatement: protocol.SignedStatement{
			Evidence: protocol.TypedEvidence{Encoded: protocol.Encoded{MediaType: "ss"}},
		}}},
	}
	supportAggregate := resourcemanager.DeliveryPackage{
		Root: protocol.Item{SignedStatement: protocol.SignedStatement{
			Evidence: protocol.TypedEvidence{ProvenanceType: "r"},
			Support:  protocol.SupportMaterial(protocol.Encoded{MediaType: "rr"}),
		}},
		Supporting: []protocol.Item{{SignedStatement: protocol.SignedStatement{
			Evidence: protocol.TypedEvidence{ProvenanceType: "s"},
			Support:  protocol.SupportMaterial(protocol.Encoded{MediaType: "ss"}),
		}}},
	}
	tests := []struct {
		name      string
		pkg       resourcemanager.DeliveryPackage
		limits    verificationLimits
		wantLimit bool
	}{
		{
			name: "combined evidence envelope per item",
			pkg:  resourcemanager.DeliveryPackage{Root: exact},
			limits: limitsWith(verificationLimits{
				maxEvidencePerItem: 4,
				maxTotalEvidence:   5,
				maxSupportPerItem:  4,
				maxTotalSupport:    4,
			}),
			wantLimit: true,
		},
		{
			name: "combined support envelope per item",
			pkg:  resourcemanager.DeliveryPackage{Root: exact},
			limits: limitsWith(verificationLimits{
				maxEvidencePerItem: 5,
				maxTotalEvidence:   5,
				maxSupportPerItem:  3,
				maxTotalSupport:    4,
			}),
			wantLimit: true,
		},
		{
			name:      "provenance type per item",
			pkg:       resourcemanager.DeliveryPackage{Root: provenanceOver},
			limits:    limitsWith(verificationLimits{maxEvidencePerItem: 4}),
			wantLimit: true,
		},
		{
			name:      "evidence media type per item",
			pkg:       resourcemanager.DeliveryPackage{Root: evidenceMediaTypeOver},
			limits:    limitsWith(verificationLimits{maxEvidencePerItem: 4}),
			wantLimit: true,
		},
		{
			name: "evidence envelope package total exact",
			pkg:  evidenceAggregate,
			limits: limitsWith(verificationLimits{
				maxEvidencePerItem: 2,
				maxTotalEvidence:   4,
			}),
		},
		{
			name: "evidence envelope package total over",
			pkg:  evidenceAggregate,
			limits: limitsWith(verificationLimits{
				maxEvidencePerItem: 2,
				maxTotalEvidence:   3,
			}),
			wantLimit: true,
		},
		{
			name:      "support media type per item",
			pkg:       resourcemanager.DeliveryPackage{Root: supportMediaTypeOver},
			limits:    limitsWith(verificationLimits{maxSupportPerItem: 4}),
			wantLimit: true,
		},
		{
			name: "support envelope package total exact",
			pkg:  supportAggregate,
			limits: limitsWith(verificationLimits{
				maxEvidencePerItem: 1,
				maxTotalEvidence:   2,
				maxSupportPerItem:  2,
				maxTotalSupport:    4,
			}),
		},
		{
			name: "support envelope package total over",
			pkg:  supportAggregate,
			limits: limitsWith(verificationLimits{
				maxEvidencePerItem: 1,
				maxTotalEvidence:   2,
				maxSupportPerItem:  2,
				maxTotalSupport:    3,
			}),
			wantLimit: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newEvidenceCatalog(test.pkg, catalogLookup(nil), test.limits)
			if test.wantLimit {
				if !errors.Is(err, errVerificationWorkLimit) {
					t.Fatalf("newEvidenceCatalog error = %v, want envelope work limit", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("newEvidenceCatalog at exact envelope limit: %v", err)
			}
		})
	}
}

func TestEvidenceCatalogBoundsCheckpointRootStringsBeforeTemporalValidation(t *testing.T) {
	empty := protocol.EmptyCheckpoint()
	if len(empty.Root) != maxCheckpointRootBytes {
		t.Fatalf("canonical checkpoint-root bytes = %d, want exact limit %d", len(empty.Root), maxCheckpointRootBytes)
	}
	if _, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{
		Root:        catalogItem("root", ""),
		EvidenceLog: &protocol.EvidenceLogUpdate{From: empty, Checkpoint: empty},
	}, catalogLookup(nil), defaultVerificationLimits()); err != nil {
		t.Fatalf("newEvidenceCatalog with canonical checkpoint roots: %v", err)
	}

	oversizedRoot := protocol.Digest(strings.Repeat("x", maxPackageProofBytes+1))
	tests := []struct {
		name   string
		mutate func(*protocol.EvidenceLogUpdate)
	}{
		{
			name: "from checkpoint root",
			mutate: func(update *protocol.EvidenceLogUpdate) {
				update.From.Root = oversizedRoot
			},
		},
		{
			name: "successor checkpoint root",
			mutate: func(update *protocol.EvidenceLogUpdate) {
				update.Checkpoint.Root = oversizedRoot
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			update := protocol.EvidenceLogUpdate{From: empty, Checkpoint: empty}
			test.mutate(&update)
			_, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{
				Root:        catalogItem("root", ""),
				EvidenceLog: &update,
			}, catalogLookup(nil), defaultVerificationLimits())
			if !errors.Is(err, errVerificationWorkLimit) {
				t.Fatalf("newEvidenceCatalog error = %v, want checkpoint-root work limit", err)
			}
			if len(err.Error()) > 256 {
				t.Fatalf("checkpoint-root error is %d bytes, want a bounded diagnostic", len(err.Error()))
			}
			if strings.Contains(err.Error(), string(oversizedRoot)) {
				t.Fatal("checkpoint-root error echoed the couriered root")
			}
		})
	}
}

func TestEvidenceCatalogValidatesUnusedProofEncodingWithoutVerifyingIt(t *testing.T) {
	root := catalogItem("root", "")
	malformed := catalogItem("support", "")
	malformed.EvidenceLog = &protocol.EvidenceLogInclusion{
		Index:          99,
		InclusionProof: []protocol.Digest{"not-a-canonical-sha256-digest"},
	}
	if _, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: root, Supporting: []protocol.Item{malformed}}, catalogLookup(nil), defaultVerificationLimits()); !errors.Is(err, protocol.ErrInvalidLogInclusion) {
		t.Fatalf("catalog error = %v, want malformed unused inclusion encoding", err)
	}

	incorrectButWellFormed := catalogItem("unused", "")
	incorrectButWellFormed.EvidenceLog = &protocol.EvidenceLogInclusion{
		Index:          99,
		InclusionProof: []protocol.Digest{protocol.DigestBytes([]byte("not the actual proof"))},
	}
	if _, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: root, Supporting: []protocol.Item{incorrectButWellFormed}}, catalogLookup(nil), defaultVerificationLimits()); err != nil {
		t.Fatalf("catalog cryptographically verified unused inclusion: %v", err)
	}
}

func catalogItem(body, support string) protocol.Item {
	return protocol.Item{SignedStatement: protocol.SignedStatement{
		Evidence: protocol.TypedEvidence{
			ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
			Encoded: protocol.Encoded{
				MediaType: "application/test+json",
				Bytes:     []byte(body),
			},
		},
		Support: protocol.SupportMaterial(protocol.Encoded{MediaType: "application/support", Bytes: []byte(support)}),
	}}
}

func cloneCatalogItem(in protocol.Item) protocol.Item {
	out := in
	out.Evidence.Bytes = append([]byte(nil), in.Evidence.Bytes...)
	out.Support.Bytes = append([]byte(nil), in.Support.Bytes...)
	if in.EvidenceLog != nil {
		proof := *in.EvidenceLog
		proof.InclusionProof = append([]protocol.Digest(nil), in.EvidenceLog.InclusionProof...)
		out.EvidenceLog = &proof
	}
	return out
}

func catalogLookup(targets map[protocol.ProvenanceType]protocol.TargetAPI) protocol.TargetLookup {
	return func(provenanceType protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		target, ok := targets[provenanceType]
		if !ok {
			return nil, false
		}
		return target, true
	}
}

func limitsWith(overrides verificationLimits) verificationLimits {
	limits := defaultVerificationLimits()
	if overrides.maxStatements != 0 {
		limits.maxStatements = overrides.maxStatements
	}
	if overrides.maxEvidencePerItem != 0 {
		limits.maxEvidencePerItem = overrides.maxEvidencePerItem
	}
	if overrides.maxTotalEvidence != 0 {
		limits.maxTotalEvidence = overrides.maxTotalEvidence
	}
	if overrides.maxSupportPerItem != 0 {
		limits.maxSupportPerItem = overrides.maxSupportPerItem
	}
	if overrides.maxTotalSupport != 0 {
		limits.maxTotalSupport = overrides.maxTotalSupport
	}
	if overrides.maxProofHashes != 0 {
		limits.maxProofHashes = overrides.maxProofHashes
	}
	if overrides.maxProofBytes != 0 {
		limits.maxProofBytes = overrides.maxProofBytes
	}
	if overrides.maxDepth != 0 {
		limits.maxDepth = overrides.maxDepth
	}
	if overrides.maxEdges != 0 {
		limits.maxEdges = overrides.maxEdges
	}
	return limits
}
