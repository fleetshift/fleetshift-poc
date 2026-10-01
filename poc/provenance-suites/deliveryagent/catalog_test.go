package deliveryagent

import (
	"context"
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

func TestEvidenceCatalogLazilyCachesDetachedPerTypeHints(t *testing.T) {
	root := catalogItem("root", "")
	first := catalogItem("first", "first support")
	first.Evidence.ProvenanceType = "first/v1"
	first.EvidenceLog = &protocol.EvidenceLogInclusion{Index: 1, InclusionProof: []protocol.Digest{protocol.DigestBytes([]byte("first inclusion"))}}
	second := catalogItem("second", "second support")
	second.Evidence.ProvenanceType = "second/v1"
	firstTarget := &catalogTestTarget{
		provenanceType:  "first/v1",
		predicateByBody: map[string]protocol.PredicateType{"first": "wanted/v1"},
		mutateInput:     true,
	}
	secondTarget := &catalogTestTarget{
		provenanceType:  "second/v1",
		predicateByBody: map[string]protocol.PredicateType{"second": "other/v1"},
	}
	update := &protocol.EvidenceLogUpdate{ConsistencyProof: []protocol.Digest{protocol.DigestBytes([]byte("consistency"))}}
	pkg := resourcemanager.DeliveryPackage{Root: root, Supporting: []protocol.Item{first, second}, EvidenceLog: update}
	catalog, err := newEvidenceCatalog(pkg, catalogLookup(map[protocol.ProvenanceType]*catalogTestTarget{
		"first/v1":  firstTarget,
		"second/v1": secondTarget,
	}), defaultVerificationLimits())
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	if firstTarget.parseCalls != 0 || secondTarget.parseCalls != 0 {
		t.Fatal("catalog parsed hints before a selector requested them")
	}
	first.Evidence.Bytes[0] = 'Y'
	first.Support.Bytes[0] = 'Y'
	first.EvidenceLog.InclusionProof[0] = "mutated proof"
	update.ConsistencyProof[0] = "mutated consistency"
	if got, err := catalog.supportingCandidates("wanted/v1"); err != nil || len(got) != 1 {
		t.Fatalf("wanted candidates = %v, %v; want one", got, err)
	}
	if got, err := catalog.supportingCandidates("other/v1"); err != nil || len(got) != 1 {
		t.Fatalf("other candidates = %v, %v; want one", got, err)
	}
	if firstTarget.parseCalls != 1 || secondTarget.parseCalls != 1 {
		t.Fatalf("ParseHints calls = first %d, second %d; want one each", firstTarget.parseCalls, secondTarget.parseCalls)
	}
	if got := catalog.item(catalog.supporting[0]).Evidence.Bytes; string(got) != "first" {
		t.Fatalf("catalog evidence changed through ParseHints mutation: %q", got)
	}
	item := catalog.item(catalog.supporting[0])
	if got := item.Support.Bytes; string(got) != "first support" {
		t.Fatalf("catalog support snapshot = %q, want original", got)
	}
	if got := item.EvidenceLog.InclusionProof[0]; got != protocol.DigestBytes([]byte("first inclusion")) {
		t.Fatalf("catalog inclusion proof = %q, want original encoding", got)
	}
	if got := catalog.update.ConsistencyProof[0]; got != protocol.DigestBytes([]byte("consistency")) {
		t.Fatalf("catalog consistency proof = %q, want original encoding", got)
	}
	if item.EvidenceLog != catalog.byID[catalog.supporting[0]].EvidenceLog {
		t.Fatal("catalog lookup copied its immutable item")
	}
	candidates, err := catalog.supportingCandidates("wanted/v1")
	if err != nil {
		t.Fatal(err)
	}
	if &candidates[0] != &catalog.candidates["wanted/v1"][0] {
		t.Fatal("catalog lookup copied its immutable candidate list")
	}
}

func TestEvidenceCatalogCandidateLimitFailsWithoutTruncationOrParsing(t *testing.T) {
	exactTarget := &catalogTestTarget{
		provenanceType:  protocol.ProvenanceTypeDirectKeyV1,
		predicateByBody: map[string]protocol.PredicateType{"exact-support": "wanted/v1"},
	}
	exactCatalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("exact-root", ""), Supporting: []protocol.Item{catalogItem("exact-support", "")}}, catalogLookup(map[protocol.ProvenanceType]*catalogTestTarget{
		protocol.ProvenanceTypeDirectKeyV1: exactTarget,
	}), limitsWith(verificationLimits{maxCandidates: 1}))
	if err != nil {
		t.Fatalf("newEvidenceCatalog at candidate limit: %v", err)
	}
	if candidates, err := exactCatalog.supportingCandidates("wanted/v1"); err != nil || len(candidates) != 1 {
		t.Fatalf("exact-limit candidates = %v, %v; want one", candidates, err)
	}

	first := catalogItem("first", "")
	second := catalogItem("second", "")
	target := &catalogTestTarget{provenanceType: protocol.ProvenanceTypeDirectKeyV1}
	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("root", ""), Supporting: []protocol.Item{first, second}}, catalogLookup(map[protocol.ProvenanceType]*catalogTestTarget{
		protocol.ProvenanceTypeDirectKeyV1: target,
	}), limitsWith(verificationLimits{maxCandidates: 1}))
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	if _, err := catalog.supportingCandidates("wanted/v1"); !errors.Is(err, errVerificationWorkLimit) {
		t.Fatalf("supportingCandidates error = %v, want candidate work limit", err)
	}
	if target.parseCalls != 0 {
		t.Fatalf("ParseHints calls = %d, want no partial candidate scan", target.parseCalls)
	}
}

func TestEvidenceCatalogCachesLookupAndParseErrors(t *testing.T) {
	item := catalogItem("support", "")
	var lookups int
	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("root", ""), Supporting: []protocol.Item{item}}, func(protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		lookups++
		return nil, false
	}, defaultVerificationLimits())
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := catalog.supportingCandidates("wanted/v1"); !errors.Is(err, protocol.ErrUnknownProvenanceType) {
			t.Fatalf("supportingCandidates error = %v, want unknown provenance type", err)
		}
	}
	if lookups != 1 {
		t.Fatalf("target lookup calls = %d, want cached single lookup", lookups)
	}
}

// This pins phase 6's temporary fail-closed lookup/ParseHints error handling.
// Phase 7 must skip a failed hint item and continue the bounded candidate scan;
// this test does not establish a package requirement that all couriered items
// have usable hints.
func TestEvidenceCatalogCachesVerifierMismatchAndParseErrors(t *testing.T) {
	parseFailure := errors.New("test ParseHints failure")
	tests := []struct {
		name       string
		target     *catalogTestTarget
		wantParse  int
		wantLookup int
	}{
		{
			name:       "mismatched provenance type",
			target:     &catalogTestTarget{provenanceType: "other/v1"},
			wantParse:  0,
			wantLookup: 1,
		},
		{
			name:       "cached ParseHints error",
			target:     &catalogTestTarget{provenanceType: protocol.ProvenanceTypeDirectKeyV1, parseErr: parseFailure},
			wantParse:  1,
			wantLookup: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var lookups int
			catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("root", ""), Supporting: []protocol.Item{catalogItem("support", "")}}, func(protocol.ProvenanceType) (protocol.TargetAPI, bool) {
				lookups++
				return test.target, true
			}, defaultVerificationLimits())
			if err != nil {
				t.Fatalf("newEvidenceCatalog: %v", err)
			}
			for i := 0; i < 2; i++ {
				_, err := catalog.supportingCandidates("wanted/v1")
				if err == nil {
					t.Fatal("supportingCandidates unexpectedly succeeded")
				}
				if test.name == "cached ParseHints error" && !errors.Is(err, parseFailure) {
					t.Fatalf("supportingCandidates error = %v, want cached ParseHints error", err)
				}
				if test.name == "mismatched provenance type" && !errors.Is(err, protocol.ErrUnknownProvenanceType) {
					t.Fatalf("supportingCandidates error = %v, want provenance mismatch", err)
				}
			}
			if lookups != test.wantLookup || test.target.parseCalls != test.wantParse {
				t.Fatalf("lookup calls = %d, ParseHints calls = %d; want %d and %d", lookups, test.target.parseCalls, test.wantLookup, test.wantParse)
			}
		})
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

func catalogLookup(targets map[protocol.ProvenanceType]*catalogTestTarget) protocol.TargetLookup {
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
	if overrides.maxCandidates != 0 {
		limits.maxCandidates = overrides.maxCandidates
	}
	if overrides.maxDepth != 0 {
		limits.maxDepth = overrides.maxDepth
	}
	if overrides.maxEdges != 0 {
		limits.maxEdges = overrides.maxEdges
	}
	return limits
}

type catalogTestTarget struct {
	provenanceType  protocol.ProvenanceType
	predicateByBody map[string]protocol.PredicateType
	mutateInput     bool
	parseErr        error
	parseCalls      int
}

func (t *catalogTestTarget) ProvenanceType() protocol.ProvenanceType { return t.provenanceType }
func (t *catalogTestTarget) RequiresEvidenceLog() bool               { return false }
func (t *catalogTestTarget) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	t.parseCalls++
	if t.parseErr != nil {
		return protocol.TentativeHints{}, t.parseErr
	}
	if t.mutateInput && len(evidence.Bytes) > 0 {
		evidence.Bytes[0] = 'X'
	}
	predicate, ok := t.predicateByBody[string(evidence.Bytes)]
	if !ok && t.mutateInput && len(evidence.Bytes) > 0 {
		predicate, ok = t.predicateByBody["first"]
	}
	if !ok {
		return protocol.TentativeHints{}, errors.New("no test hint")
	}
	return protocol.TentativeHints{PredicateType: predicate}, nil
}
func (t *catalogTestTarget) BeginVerification(context.Context, protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	return nil, errors.New("not used by catalog test")
}
func (t *catalogTestTarget) Owns(protocol.PredicateType) bool { return false }
func (t *catalogTestTarget) Apply(context.Context, protocol.ApplyRequest) error {
	return errors.New("not used by catalog test")
}
