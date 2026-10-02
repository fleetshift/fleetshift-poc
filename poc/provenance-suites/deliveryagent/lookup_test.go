package deliveryagent

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

func TestRelationLookupStopsCachesAndResumes(t *testing.T) {
	signer := testProducer(t, "addon")
	target := &countedTarget{delegate: directkey.NewTarget()}
	items := []protocol.Item{
		lookupRelationItem(t, signer, "one.example/Cluster", "application/one"),
		lookupRelationItem(t, signer, "two.example/Cluster", "application/two"),
		lookupRelationItem(t, signer, "one.example/Cluster", "application/later"),
		lookupRelationItem(t, signer, "three.example/Cluster", "application/three"),
		catalogItem("unused malformed native evidence", ""),
	}
	catalog := lookupTestCatalog(t, items, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.ProvenanceType(): target}))
	if target.parseCalls != 0 {
		t.Fatal("catalog eagerly parsed support")
	}
	for _, step := range []struct {
		key          string
		index, calls int
	}{
		{"two.example/Cluster", 1, 2},
		{"one.example/Cluster", 0, 2},
		{"three.example/Cluster", 3, 4},
		{"one.example/Cluster", 0, 4},
	} {
		got, err := catalog.supportingRelation(resourceTypeForTest(t, step.key))
		if err != nil || got != catalog.supporting[step.index] {
			t.Fatalf("lookup %s = %s, %v; want item %d", step.key, got, err, step.index)
		}
		if target.parseCalls != step.calls {
			t.Fatalf("lookup %s parsed %d items, want %d", step.key, target.parseCalls, step.calls)
		}
	}
	for range 2 {
		if _, err := catalog.supportingRelation(resourceTypeForTest(t, "missing.example/Cluster")); !errors.Is(err, ErrFulfillmentRelationRequired) {
			t.Fatalf("missing relation: %v", err)
		}
	}
	if target.parseCalls != 5 || target.beginCalls != 0 {
		t.Fatalf("calls after exhaustion: parse %d, verify %d", target.parseCalls, target.beginCalls)
	}
}

func TestRelationLookupSkipsUnusableCourieredItems(t *testing.T) {
	signer := testProducer(t, "addon")
	unknown := catalogItem("unknown", "")
	unknown.Evidence.ProvenanceType = "uninstalled/v1"
	malformedNative := catalogItem("malformed envelope", "")
	malformedNative.Evidence.MediaType = directkey.MediaTypeSignature
	items := []protocol.Item{
		unknown, malformedNative,
		lookupAssertionItem(t, signer, protocol.TypedAssertion{PredicateType: protocol.PredicateTypeFulfillmentRelationV1, Bytes: []byte("not JSON")}),
		lookupAssertionItem(t, signer, rawRelationAssertion(t, "local-kind", "application/json")),
		lookupAssertionItem(t, signer, protocol.TypedAssertion{PredicateType: "suite/opaque", Bytes: []byte("opaque event")}),
		lookupRelationItem(t, signer, "wanted.example/Cluster", ""),
	}
	target := &countedTarget{delegate: directkey.NewTarget()}
	catalog := lookupTestCatalog(t, items, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.ProvenanceType(): target}))
	got, err := catalog.supportingRelation(resourceTypeForTest(t, "wanted.example/Cluster"))
	if err != nil || got != catalog.supporting[5] {
		t.Fatalf("lookup through unusable support: %s, %v", got, err)
	}
	// A missing media type still supplies a usable key; authentication and
	// authoritative semantics decide whether that selected relation is acceptable.
	if target.parseCalls != 5 || target.beginCalls != 0 {
		t.Fatalf("parse=%d verify=%d", target.parseCalls, target.beginCalls)
	}
	if _, err := catalog.supportingRelation(resourceTypeForTest(t, "missing.example/Cluster")); !errors.Is(err, ErrFulfillmentRelationRequired) {
		t.Fatal(err)
	}
	if target.parseCalls != 5 {
		t.Fatal("lookup reparsed discarded failures")
	}
}

func TestRelationLookupRejectsTrustedLookupDefects(t *testing.T) {
	signer := testProducer(t, "addon")
	wrong := &hintTarget{TargetAPI: directkey.NewTarget(), kind: "wrong/v1"}
	for _, tc := range []struct {
		name   string
		lookup protocol.TargetLookup
	}{
		{"missing lookup", nil},
		{"successful nil", func(protocol.ProvenanceType) (protocol.TargetAPI, bool) { return nil, true }},
		{"successful wrong type", func(protocol.ProvenanceType) (protocol.TargetAPI, bool) { return wrong, true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			lookup := tc.lookup
			if lookup != nil {
				lookup = func(kind protocol.ProvenanceType) (protocol.TargetAPI, bool) { calls++; return tc.lookup(kind) }
			}
			catalog := lookupTestCatalog(t, []protocol.Item{
				lookupRelationItem(t, signer, "other.example/Cluster", "application/json"),
				lookupRelationItem(t, signer, "wanted.example/Cluster", "application/json"),
			}, lookup)
			for range 2 {
				if _, err := catalog.supportingRelation(resourceTypeForTest(t, "wanted.example/Cluster")); !errors.Is(err, errInvalidTargetLookup) {
					t.Fatalf("trusted lookup defect = %v", err)
				}
			}
			if tc.lookup != nil && calls != 1 {
				t.Fatalf("lookup calls=%d; defect should stop and be retained", calls)
			}
			if wrong.calls != 0 {
				t.Fatal("invalid implementation parsed evidence")
			}
		})
	}
}

func TestRelationLookupUsesEachInstalledTypeAndOwnsKeys(t *testing.T) {
	signer := testProducer(t, "addon")
	first := &hintTarget{TargetAPI: directkey.NewTarget(), kind: protocol.ProvenanceTypeDirectKeyV1, mutate: true}
	second := &hintTarget{TargetAPI: directkey.NewTarget(), kind: "another-native/v1"}
	items := []protocol.Item{
		lookupRelationItem(t, signer, "one.example/Cluster", "application/json"),
		lookupRelationItem(t, signer, "two.example/Cluster", "application/json"),
	}
	items[1].Evidence.ProvenanceType = second.kind
	catalog := lookupTestCatalog(t, items, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{first.kind: first, second.kind: second}))
	items[0].Evidence.Bytes[0] = 'X'
	if got, err := catalog.supportingRelation(resourceTypeForTest(t, "two.example/Cluster")); err != nil || got != catalog.supporting[1] {
		t.Fatalf("mixed native lookup: %s, %v", got, err)
	}
	// Mutating buffers retained by the native parser cannot rewrite discovered keys.
	for i := range first.returned.Bytes {
		first.returned.Bytes[i] = 'X'
	}
	if got, err := catalog.supportingRelation(resourceTypeForTest(t, "one.example/Cluster")); err != nil || got != catalog.supporting[0] {
		t.Fatalf("owned lookup key: %s, %v", got, err)
	}
	if first.calls != 1 || second.calls != 1 || catalog.item(catalog.supporting[0]).Evidence.Bytes[0] == 'X' {
		t.Fatal("lookup reparsed hints or shared profile-owned input")
	}
}

func TestRelationLookupCanExhaustTheWholeBoundedPackage(t *testing.T) {
	signer := testProducer(t, "addon")
	target := &countedTarget{delegate: directkey.NewTarget()}
	items := make([]protocol.Item, maxPackageStatements-1)
	for i := range items {
		items[i] = lookupRelationItem(t, signer, fmt.Sprintf("types.example/Type%d", i), "application/json")
	}
	catalog := lookupTestCatalog(t, items, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.ProvenanceType(): target}))
	for _, key := range []string{"types.example/Type1", "types.example/Type254"} {
		if _, err := catalog.supportingRelation(resourceTypeForTest(t, key)); err != nil {
			t.Fatal(err)
		}
	}
	if target.parseCalls != 255 {
		t.Fatalf("parsed %d, want 255", target.parseCalls)
	}
	for range 2 {
		if _, err := catalog.supportingRelation(resourceTypeForTest(t, "missing.example/Cluster")); !errors.Is(err, ErrFulfillmentRelationRequired) {
			t.Fatal(err)
		}
	}
	if target.parseCalls != 255 {
		t.Fatal("exhaustion caused repeated parsing")
	}
	items = append(items, lookupRelationItem(t, signer, "types.example/Extra", "application/json"))
	if _, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("root", ""), Supporting: items}, catalog.lookup, defaultVerificationLimits()); !errors.Is(err, errVerificationWorkLimit) {
		t.Fatalf("oversized package: %v", err)
	}
	if target.parseCalls != 255 {
		t.Fatal("structural rejection parsed hints")
	}
}

func TestRelationLookupRejectsMissingCatalogIdentity(t *testing.T) {
	signer := testProducer(t, "addon")
	target := &countedTarget{delegate: directkey.NewTarget()}
	catalog := lookupTestCatalog(t, []protocol.Item{lookupRelationItem(t, signer, "wanted.example/Cluster", "application/json")}, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.ProvenanceType(): target}))
	delete(catalog.byID, catalog.supporting[0])
	if _, err := catalog.supportingRelation(resourceTypeForTest(t, "wanted.example/Cluster")); !errors.Is(err, errUnknownCatalogEvidence) {
		t.Fatalf("broken catalog invariant=%v", err)
	}
	if target.parseCalls != 0 {
		t.Fatal("broken catalog reached native parsing")
	}
}

func TestRelationLookupUsesTheCatalogSnapshot(t *testing.T) {
	signer := testProducer(t, "addon")
	target := &hintTarget{TargetAPI: directkey.NewTarget(), kind: protocol.ProvenanceTypeDirectKeyV1, mutate: true}
	item := lookupRelationItem(t, signer, "wanted.example/Cluster", "application/json")
	item.Support = protocol.SupportMaterial(protocol.Encoded{MediaType: "application/support", Bytes: []byte("original support")})
	proof := protocol.DigestBytes([]byte("inclusion"))
	item.EvidenceLog = &protocol.EvidenceLogInclusion{Index: 1, InclusionProof: []protocol.Digest{proof}}
	consistency := protocol.DigestBytes([]byte("consistency"))
	update := &protocol.EvidenceLogUpdate{ConsistencyProof: []protocol.Digest{consistency}}
	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("root", ""), Supporting: []protocol.Item{item}, EvidenceLog: update}, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.kind: target}), defaultVerificationLimits())
	if err != nil {
		t.Fatal(err)
	}
	item.Evidence.Bytes[0] = 'X'
	item.Support.Bytes[0] = 'X'
	item.EvidenceLog.Index = 99
	item.EvidenceLog.InclusionProof[0] = "mutated proof"
	update.ConsistencyProof[0] = "mutated consistency"
	identity, err := catalog.supportingRelation(resourceTypeForTest(t, "wanted.example/Cluster"))
	if err != nil {
		t.Fatal(err)
	}
	got := catalog.item(identity)
	if got.Evidence.Bytes[0] == 'X' || string(got.Support.Bytes) != "original support" || got.EvidenceLog.Index != 1 || got.EvidenceLog.InclusionProof[0] != proof || catalog.update.ConsistencyProof[0] != consistency {
		t.Fatal("caller or profile mutation changed catalog snapshots")
	}
	if got.EvidenceLog != catalog.byID[identity].EvidenceLog {
		t.Fatal("internal lookup copied immutable state")
	}
}

func TestRelationLookupSkipsMissingAssertionPurpose(t *testing.T) {
	signer := testProducer(t, "addon")
	target := &hintTarget{TargetAPI: directkey.NewTarget(), kind: protocol.ProvenanceTypeDirectKeyV1, edit: func(hints *protocol.TentativeHints) {
		hints.Assertion.PredicateType = ""
	}}
	catalog := lookupTestCatalog(t, []protocol.Item{lookupRelationItem(t, signer, "wanted.example/Cluster", "application/json")}, catalogLookup(map[protocol.ProvenanceType]protocol.TargetAPI{target.kind: target}))
	for range 2 {
		if _, err := catalog.supportingRelation(resourceTypeForTest(t, "wanted.example/Cluster")); !errors.Is(err, ErrFulfillmentRelationRequired) {
			t.Fatal(err)
		}
	}
	if target.calls != 1 {
		t.Fatal("lookup reparsed a discarded assertion")
	}
}

func lookupAssertionItem(t *testing.T, signer *directkey.Producer, assertion protocol.TypedAssertion) protocol.Item {
	t.Helper()
	evidence, err := signer.CreateEvidence(context.Background(), assertion)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: evidence}}
}

func lookupRelationItem(t *testing.T, signer *directkey.Producer, kind string, media protocol.MediaType) protocol.Item {
	t.Helper()
	assertion, err := (protocol.FulfillmentRelation{ResourceType: resourceTypeForTest(t, kind), MediaType: media}).Assertion()
	if err != nil {
		t.Fatal(err)
	}
	return lookupAssertionItem(t, signer, assertion)
}

func lookupTestCatalog(t *testing.T, items []protocol.Item, lookup protocol.TargetLookup) *evidenceCatalog {
	t.Helper()
	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: catalogItem("root", ""), Supporting: items}, lookup, defaultVerificationLimits())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

// hintTarget decorates real native parsing for ownership and mixed-type tests.
// The additional type uses the same signature encoding only as a test fixture.
type hintTarget struct {
	protocol.TargetAPI
	kind     protocol.ProvenanceType
	mutate   bool
	calls    int
	returned protocol.TypedAssertion
	edit     func(*protocol.TentativeHints)
}

func (t *hintTarget) ProvenanceType() protocol.ProvenanceType { return t.kind }
func (t *hintTarget) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	t.calls++
	evidence.ProvenanceType = t.TargetAPI.ProvenanceType()
	hints, err := t.TargetAPI.ParseHints(evidence)
	if err == nil && t.edit != nil {
		t.edit(&hints)
	}
	if t.mutate && len(evidence.Bytes) > 0 {
		evidence.Bytes[0] = 'X'
	}
	t.returned = hints.Assertion
	return hints, err
}

// rawRelationAssertion allows native-producer tests to sign wire values that
// cannot be constructed as ResourceType, exercising the target parsing boundary.
func rawRelationAssertion(t *testing.T, kind string, media protocol.MediaType) protocol.TypedAssertion {
	t.Helper()
	encoded, err := protocol.MarshalCanonical(struct {
		ResourceType string             `json:"resource_type"`
		MediaType    protocol.MediaType `json:"media_type"`
	}{kind, media})
	if err != nil {
		t.Fatal(err)
	}
	return protocol.TypedAssertion{PredicateType: protocol.PredicateTypeFulfillmentRelationV1, Bytes: encoded}
}

func resourceTypeForTest(t *testing.T, value string) protocol.ResourceType {
	t.Helper()
	parsed, err := protocol.ParseResourceType(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
