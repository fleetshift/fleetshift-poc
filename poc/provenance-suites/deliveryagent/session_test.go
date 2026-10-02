package deliveryagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/directkey"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/temporal"
)

func TestVerificationSessionMemoizesSupportingVerificationAsImmutableData(t *testing.T) {
	session, _, supportID, target, logVerifier := newVerificationFixture(t, nil, defaultVerificationLimits())

	first, err := session.verifyNode(context.Background(), supportID)
	if err != nil {
		t.Fatalf("first support verification: %v", err)
	}

	second, err := session.verifyNode(context.Background(), supportID)
	if err != nil {
		t.Fatalf("second support verification: %v", err)
	}
	if string(second.result.Assertion.Bytes) != `{"resource_type":"test.example/Cluster","media_type":"application/json"}` {
		t.Fatalf("cached assertion = %q, want original relation", second.result.Assertion.Bytes)
	}
	if second.result.Temporal.LogPosition == nil || second.result.Temporal.LogPosition.Index != 1 {
		t.Fatalf("cached log position = %+v, want support index 1", second.result.Temporal.LogPosition)
	}
	if second.result.Authority.CredentialMethods[0] != "test-credential" {
		t.Fatalf("cached authority credentials = %v, want original", second.result.Authority.CredentialMethods)
	}
	if &first.statement.Evidence.Bytes[0] != &session.catalog.byID[supportID].Evidence.Bytes[0] ||
		&first.statement.Evidence.Bytes[0] != &second.statement.Evidence.Bytes[0] {
		t.Fatal("verification copied immutable catalog evidence")
	}
	if &first.result.Assertion.Bytes[0] != &second.result.Assertion.Bytes[0] ||
		first.result.Temporal.LogPosition != second.result.Temporal.LogPosition ||
		&first.result.Authority.CredentialMethods[0] != &second.result.Authority.CredentialMethods[0] {
		t.Fatal("cache lookup copied immutable verification data")
	}
	if target.beginCalls != 1 || target.finishCalls != 1 {
		t.Fatalf("profile calls = Begin %d, Finish %d; want one each", target.beginCalls, target.finishCalls)
	}
	if logVerifier.calls[supportID] != 1 {
		t.Fatalf("support occurrence verifier calls = %d, want one", logVerifier.calls[supportID])
	}
	if session.edgeCount != 0 {
		t.Fatalf("verified but unselected support produced %d semantic edges", session.edgeCount)
	}
}

func TestVerificationSessionAuthenticatesPriorIntentAsDependency(t *testing.T) {
	user := testProducer(t, "alice")
	baseTarget := directkey.NewTarget()
	target := &countedTarget{delegate: baseTarget}
	enrollTestTarget(t, baseTarget, user)
	trust := sessionTestTrust()
	lookup := func(pt protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		return target, pt == target.ProvenanceType()
	}
	createIntent := func(generation uint64) protocol.TypedEvidence {
		t.Helper()
		assertion, err := (protocol.DeploymentAuthorization{
			DeliveryScope: protocol.DeliveryScope{
				Tenant:           sessionTenant(),
				TargetID:         "target-test",
				FullResourceName: "resources.example/test-resource",
				Generation:       generation,
				Action:           protocol.ActionPut,
			},
			Manifests: []protocol.TypedManifest{{MediaType: "application/json", Bytes: []byte(`{"cluster":"test"}`)}},
		}).Assertion()
		if err != nil {
			t.Fatalf("create intent assertion: %v", err)
		}
		evidence, err := user.CreateEvidence(context.Background(), assertion)
		if err != nil {
			t.Fatalf("create intent evidence: %v", err)
		}
		return evidence
	}
	prior, current := createIntent(1), createIntent(2)
	pkg := loggedTestPackage(t, prior, current)
	retained := temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}
	newSession := func(pkg resourcemanager.DeliveryPackage) *verificationSession {
		t.Helper()
		catalog, err := newEvidenceCatalog(pkg, lookup, defaultVerificationLimits())
		if err != nil {
			t.Fatalf("newEvidenceCatalog: %v", err)
		}
		root := catalog.item(catalog.rootID)
		prepared, err := temporal.Prepare(retained, catalog.update, root.Evidence, root.EvidenceLog)
		if err != nil {
			t.Fatalf("temporal.Prepare: %v", err)
		}
		retained = prepared.NextState
		return newVerificationSession(catalog, trust, protocol.TemporalVerificationServices{Log: prepared.Log})
	}
	first := newSession(pkg)
	var original verifiedNode
	if err := first.withNode(context.Background(), first.catalog.rootID, func(root verifiedNode) error {
		original = root
		return nil
	}); err != nil {
		t.Fatalf("authenticate original root intent: %v", err)
	}

	// A later delivery consumes the original intent as prior evidence. This
	// test-only common handler exercises the verification seam for updates;
	// it does not add an update predicate or transformation to the POC.
	pkg.Root, pkg.Supporting[0] = pkg.Supporting[0], pkg.Root
	pkg.EvidenceLog.From = retained.EvidenceLog
	second := newSession(pkg)
	priorID := first.catalog.rootID
	if err := second.withNode(context.Background(), second.catalog.rootID, func(root verifiedNode) error {
		for range 2 {
			if err := second.withNode(context.Background(), priorID, func(dependency verifiedNode) error {
				if dependency.result.Authenticated.PredicateType != protocol.PredicateTypeDeploymentV1 {
					return fmt.Errorf("unexpected prior predicate %s", dependency.result.Authenticated.PredicateType)
				}
				if !reflect.DeepEqual(dependency.result.Policy, original.result.Policy) {
					return errors.New("prior intent selected a different policy as a dependency")
				}
				if !bytes.Equal(dependency.result.Assertion.Bytes, original.result.Assertion.Bytes) {
					return errors.New("prior intent assertion changed when used as a dependency")
				}
				return second.recordDependency(root.identity, dependency.identity)
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("authenticate prior intent as a dependency: %v", err)
	}
	if target.beginCalls != 3 || target.finishCalls != 3 {
		t.Fatalf("profile calls = Begin %d, Finish %d; want original delivery plus two unique nodes in the later delivery", target.beginCalls, target.finishCalls)
	}
}

func TestVerificationSessionCachesAuthenticationAcrossSemanticTenantChecks(t *testing.T) {
	session, _, supportID, target, _ := newVerificationFixture(t, nil, defaultVerificationLimits())
	evaluateForTenant := func(tenant protocol.Tenant) func(verifiedNode) error {
		return func(node verifiedNode) error {
			if node.result.Authenticated.Principal.Tenant() != tenant {
				return protocol.ErrTenantMismatch
			}
			return nil
		}
	}
	if err := session.withNode(context.Background(), supportID, evaluateForTenant(sessionTenant())); err != nil {
		t.Fatal(err)
	}
	evaluated := false
	err := session.withNode(context.Background(), supportID, func(node verifiedNode) error {
		evaluated = true
		return evaluateForTenant(otherSessionTenant())(node)
	})
	if !errors.Is(err, protocol.ErrTenantMismatch) || !evaluated {
		t.Fatalf("semantic tenant check = %v, evaluated = %v", err, evaluated)
	}
	if err := session.withNode(context.Background(), supportID, evaluateForTenant(sessionTenant())); err != nil {
		t.Fatal(err)
	}
	if target.beginCalls != 1 || target.finishCalls != 1 {
		t.Fatalf("profile calls = Begin %d, Finish %d; want one cached authentication", target.beginCalls, target.finishCalls)
	}
	if session.edgeCount != 0 {
		t.Fatal("semantic tenant checks selected an authorization dependency")
	}
}

func TestVerificationSessionFailedVerificationCanRetryAndMissingIdentityFailsClosed(t *testing.T) {
	rogue := testProducer(t, "rogue")
	session, targetKey, supportID, target, _ := newVerificationFixture(t, rogue, defaultVerificationLimits())

	missing := protocol.DigestBytes([]byte("not in this package"))
	if _, err := session.verifyNode(context.Background(), missing); !errors.Is(err, errUnknownCatalogEvidence) {
		t.Fatalf("missing identity error = %v, want fail-closed catalog lookup", err)
	}
	if _, err := session.verifyNode(context.Background(), supportID); !errors.Is(err, protocol.ErrVerificationFailed) {
		t.Fatalf("unenrolled signature error = %v, want verification failure", err)
	}
	if _, cached := session.memo[supportID]; cached {
		t.Fatal("failed selection left a successful or in-progress memo entry")
	}
	enrollTestTarget(t, targetKey, rogue)
	if _, err := session.verifyNode(context.Background(), supportID); err != nil {
		t.Fatalf("retry after adding the retained signing key: %v", err)
	}
	if target.beginCalls != 2 || target.finishCalls != 2 {
		t.Fatalf("profile retry calls = Begin %d, Finish %d; want two attempts", target.beginCalls, target.finishCalls)
	}
}

func TestVerificationSessionRelationLookupDoesNotCreateDependency(t *testing.T) {
	session, _, supportID, target, logVerifier := newVerificationFixture(t, nil, defaultVerificationLimits())
	identity, err := session.supportingRelation(resourceTypeForTest(t, "test.example/Cluster"))
	if err != nil {
		t.Fatalf("supportingRelation: %v", err)
	}
	if identity != supportID {
		t.Fatalf("relation identity = %s, want %s", identity, supportID)
	}
	if target.beginCalls != 0 || logVerifier.calls[supportID] != 0 || session.edgeCount != 0 {
		t.Fatalf("tentative relation lookup performed work: Begin=%d log=%d edges=%d", target.beginCalls, logVerifier.calls[supportID], session.edgeCount)
	}
	if _, err := session.verifyNode(context.Background(), supportID); err != nil {
		t.Fatalf("candidate verification: %v", err)
	}
	if session.edgeCount != 0 {
		t.Fatal("verification alone created an authorization edge")
	}
	if session.basisContributors != nil || session.actionBasis != nil {
		t.Fatal("lookup or independent authentication initialized basis accounting")
	}
}

func TestVerificationSessionRejectsActiveIdentityCycles(t *testing.T) {
	tests := []struct {
		name string
		run  func(*verificationSession, protocol.Digest, protocol.Digest) error
	}{
		{
			name: "root self cycle",
			run: func(session *verificationSession, root, _ protocol.Digest) error {
				return session.withNode(context.Background(), root, func(verifiedNode) error {
					return session.withNode(context.Background(), root, nil)
				})
			},
		},
		{
			name: "nested support path back to root",
			run: func(session *verificationSession, root, support protocol.Digest) error {
				return session.withNode(context.Background(), root, func(verifiedNode) error {
					return session.withNode(context.Background(), support, func(verifiedNode) error {
						return session.withNode(context.Background(), root, nil)
					})
				})
			},
		},
		{
			name: "support self cycle",
			run: func(session *verificationSession, _, support protocol.Digest) error {
				return session.withNode(context.Background(), support, func(verifiedNode) error {
					return session.withNode(context.Background(), support, nil)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session, root, support := newBareSession(t, defaultVerificationLimits(), 2)
			memoizeBareNode(session, root)
			memoizeBareNode(session, support)
			if err := test.run(session, root, support); !errors.Is(err, errVerificationCycle) {
				t.Fatalf("cycle error = %v, want identity cycle", err)
			}
			if len(session.active) != 0 || len(session.activeSet) != 0 {
				t.Fatalf("active path leaked after cycle: path=%v active=%v", session.active, session.activeSet)
			}
		})
	}
}

func TestVerificationSessionChecksDependencyEndpointsAndBoundsEdgesAndDepth(t *testing.T) {
	session, a, b := newBareSession(t, limitsWith(verificationLimits{maxEdges: 2, maxDepth: 2}), 3)
	c := session.catalog.supporting[1]
	memoizeBareNode(session, a)
	memoizeBareNode(session, b)
	memoizeBareNode(session, c)

	if err := session.recordDependency(a, a); !errors.Is(err, errVerificationCycle) {
		t.Fatalf("self-dependency error = %v, want cycle", err)
	}
	if err := session.recordDependency(a, b); err != nil {
		t.Fatalf("first edge: %v", err)
	}
	if err := session.recordDependency(a, b); err != nil {
		t.Fatalf("duplicate edge should be idempotent: %v", err)
	}
	if err := session.recordDependency(a, "not-verified"); !errors.Is(err, protocol.ErrPolicyReevaluation) {
		t.Fatalf("unverified endpoint: %v", err)
	}
	if err := session.recordDependency(b, c); err != nil {
		t.Fatalf("exact edge limit rejected: %v", err)
	}
	if err := session.recordDependency(a, c); !errors.Is(err, errVerificationWorkLimit) {
		t.Fatalf("edge-limit error = %v, want work limit", err)
	}

	depthSession, root, child := newBareSession(t, limitsWith(verificationLimits{maxDepth: 2}), 2)
	grandchild := depthSession.catalog.supporting[1]
	memoizeBareNode(depthSession, root)
	memoizeBareNode(depthSession, child)
	memoizeBareNode(depthSession, grandchild)
	if err := depthSession.withNode(context.Background(), root, func(verifiedNode) error {
		return depthSession.withNode(context.Background(), child, nil)
	}); err != nil {
		t.Fatalf("exact active depth limit rejected: %v", err)
	}
	if err := depthSession.withNode(context.Background(), root, func(verifiedNode) error {
		return depthSession.withNode(context.Background(), child, func(verifiedNode) error {
			return depthSession.withNode(context.Background(), grandchild, nil)
		})
	}); !errors.Is(err, errVerificationWorkLimit) {
		t.Fatalf("depth-limit error = %v, want work limit", err)
	}
}

func TestVerificationSessionBorrowsImmutableTrust(t *testing.T) {
	trust := sessionTestTrust()
	session := newVerificationSession(nil, trust, protocol.TemporalVerificationServices{})
	if &session.trust.AuthorityRegistry[0] != &trust.AuthorityRegistry[0] {
		t.Fatal("session copied its immutable trust configuration")
	}
}

func TestProfileOutputIsDetachedBeforeCommonCaching(t *testing.T) {
	session, _, supportID, target, _ := newVerificationFixture(t, nil, defaultVerificationLimits())
	first, err := session.verifyNode(context.Background(), supportID)
	if err != nil {
		t.Fatal(err)
	}
	// The decorator retains the real profile's returned buffers. A later
	// profile-side write must not alter common immutable verification data.
	target.lastResult.Assertion.Bytes[0] = 'X'
	second, err := session.verifyNode(context.Background(), supportID)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.result.Assertion.Bytes) != `{"resource_type":"test.example/Cluster","media_type":"application/json"}` ||
		string(second.result.Assertion.Bytes) != `{"resource_type":"test.example/Cluster","media_type":"application/json"}` {
		t.Fatal("profile-owned output changed the cached authentication")
	}
	if target.finishCalls != 1 {
		t.Fatalf("Finish calls = %d, want one", target.finishCalls)
	}
}

func TestSuiteApplyReceivesDetachedVerificationData(t *testing.T) {
	mutator := &mutatingApplyTarget{}
	item := catalogItem("apply-evidence", "apply-support")
	lookup := func(protocol.ProvenanceType) (protocol.TargetAPI, bool) { return mutator, true }
	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: item}, lookup, defaultVerificationLimits())
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	agent := &Agent{}
	session := newVerificationSession(catalog, protocol.TrustConfiguration{}, protocol.TemporalVerificationServices{})
	memoizeBareNode(session, catalog.rootID)
	root := verifiedNode{
		identity:  catalog.rootID,
		statement: cloneSignedStatement(item.SignedStatement),
		result: protocol.VerificationResult{
			ProvenanceAuthenticationResult: protocol.ProvenanceAuthenticationResult{
				Authenticated: protocol.AuthenticatedEvidence{
					PredicateType:        directkey.PredicateTypeEnrollmentV1,
					ProvenanceType:       protocol.ProvenanceTypeDirectKeyV1,
					SatisfiedConstraints: []protocol.ConstraintOutcome{{Name: "original"}},
				},
				Assertion: protocol.TypedAssertion{PredicateType: directkey.PredicateTypeEnrollmentV1, Bytes: []byte("assertion")},
			},
			Temporal: protocol.VerifiedSubjectTemporalInfo{
				LogPosition: &protocol.LogPosition{Domain: protocol.LogDomainTenantEvidenceV1, Index: 7},
				Times:       []protocol.VerifiedSubjectTime{{Authority: "tsa-original"}},
			},
		},
	}
	if err := agent.dispatchApplyLocked(session, root); err != nil {
		t.Fatalf("dispatchApplyLocked: %v", err)
	}
	if root.result.Authenticated.SatisfiedConstraints[0].Name != "original" || string(root.result.Assertion.Bytes) != "assertion" {
		t.Fatal("suite Apply mutated the verification result passed to the dispatcher")
	}
	if root.result.Temporal.LogPosition.Index != 7 || root.result.Temporal.Times[0].Authority != "tsa-original" {
		t.Fatal("suite Apply mutated the temporal facts passed to the dispatcher")
	}
	if string(root.statement.Evidence.Bytes) != "apply-evidence" || string(root.statement.Support.Bytes) != "apply-support" {
		t.Fatal("suite Apply mutated the root statement passed to the dispatcher")
	}
}

func newVerificationFixture(t *testing.T, supportSigner *directkey.Producer, limits verificationLimits) (*verificationSession, *directkey.Target, protocol.Digest, *countedTarget, *countedLogVerifier) {
	t.Helper()
	user := testProducer(t, "alice")
	if supportSigner == nil {
		supportSigner = user
	}
	baseTarget := directkey.NewTarget()
	enrollTestTarget(t, baseTarget, user)
	wrappedTarget := &countedTarget{delegate: baseTarget}
	trust := sessionTestTrust()
	lookup := func(provenanceType protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		if provenanceType != protocol.ProvenanceTypeDirectKeyV1 {
			return nil, false
		}
		return wrappedTarget, true
	}

	rootEvidence, err := user.CreateEvidence(context.Background(), protocol.TypedAssertion{PredicateType: protocol.PredicateTypeDeploymentV1, Bytes: []byte(`{"root":true}`)})
	if err != nil {
		t.Fatalf("create root evidence: %v", err)
	}
	supportEvidence, err := supportSigner.CreateEvidence(context.Background(), protocol.TypedAssertion{PredicateType: protocol.PredicateTypeFulfillmentRelationV1, Bytes: []byte(`{"resource_type":"test.example/Cluster","media_type":"application/json"}`)})
	if err != nil {
		t.Fatalf("create support evidence: %v", err)
	}
	pkg := loggedTestPackage(t, rootEvidence, supportEvidence)
	catalog, err := newEvidenceCatalog(pkg, lookup, limits)
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	prepared, err := temporal.Prepare(temporal.RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}, catalog.update, catalog.item(catalog.rootID).Evidence, catalog.item(catalog.rootID).EvidenceLog)
	if err != nil {
		t.Fatalf("temporal.Prepare: %v", err)
	}
	logVerifier := &countedLogVerifier{inner: prepared.Log, calls: make(map[protocol.Digest]int)}
	session := newVerificationSession(catalog, trust, protocol.TemporalVerificationServices{Log: logVerifier})
	return session, baseTarget, catalog.supporting[0], wrappedTarget, logVerifier
}

func newBareSession(t *testing.T, limits verificationLimits, supportingCount int) (*verificationSession, protocol.Digest, protocol.Digest) {
	t.Helper()
	root := catalogItem("bare-root", "")
	supporting := make([]protocol.Item, supportingCount)
	for i := range supporting {
		supporting[i] = catalogItem(fmt.Sprintf("bare-support-%d", i), "")
	}
	catalog, err := newEvidenceCatalog(resourcemanager.DeliveryPackage{Root: root, Supporting: supporting}, catalogLookup(nil), limits)
	if err != nil {
		t.Fatalf("newEvidenceCatalog: %v", err)
	}
	return newVerificationSession(catalog, protocol.TrustConfiguration{}, protocol.TemporalVerificationServices{}), catalog.rootID, catalog.supporting[0]
}

func memoizeBareNode(session *verificationSession, identity protocol.Digest) {
	session.memo[identity] = nodeState{
		phase:    nodeVerified,
		verified: verifiedNode{identity: identity},
	}
}

func loggedTestPackage(t *testing.T, evidence ...protocol.TypedEvidence) resourcemanager.DeliveryPackage {
	t.Helper()
	if len(evidence) == 0 {
		t.Fatal("loggedTestPackage requires evidence")
	}
	tree := merklelog.New()
	items := make([]protocol.Item, len(evidence))
	for i, typed := range evidence {
		identity, err := typed.Identity()
		if err != nil {
			t.Fatalf("identity: %v", err)
		}
		leaf, err := protocol.DecodeDigest(identity)
		if err != nil {
			t.Fatalf("decode identity: %v", err)
		}
		if _, _, err := tree.Append(leaf); err != nil {
			t.Fatalf("append evidence log leaf: %v", err)
		}
		items[i] = protocol.Item{SignedStatement: protocol.SignedStatement{Evidence: typed}}
	}
	root, err := tree.Root()
	if err != nil {
		t.Fatalf("evidence-log root: %v", err)
	}
	checkpoint, err := protocol.NewCheckpoint(tree.Size(), root)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	for i := range items {
		proof, err := tree.InclusionProof(uint64(i), tree.Size())
		if err != nil {
			t.Fatalf("inclusion proof %d: %v", i, err)
		}
		items[i].EvidenceLog = &protocol.EvidenceLogInclusion{Index: uint64(i), InclusionProof: protocol.EncodeProof(proof)}
	}
	return resourcemanager.DeliveryPackage{
		Root:       items[0],
		Supporting: items[1:],
		EvidenceLog: &protocol.EvidenceLogUpdate{
			From:       protocol.EmptyCheckpoint(),
			Checkpoint: checkpoint,
		},
	}
}

func sessionTestTrust() protocol.TrustConfiguration {
	profile := protocol.ProfileConfig{ProvenanceType: protocol.ProvenanceTypeDirectKeyV1}
	authority := protocol.AuthorityConfig{
		PrincipalAuthority: protocol.PrincipalAuthority{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: "https://issuer.example.test",
		},

		CredentialMethods:  []string{"test-credential"},
		ProvenanceProfiles: []protocol.ProfileConfig{profile},
		DeliveryPolicies: []protocol.DeliveryPolicy{
			sessionPolicy(protocol.PredicateTypeDeploymentV1, profile),
			sessionPolicy(protocol.PredicateTypeFulfillmentRelationV1, profile),
		},
	}
	return protocol.TrustConfiguration{AuthorityRegistry: []protocol.AuthorityConfig{authority}}
}

func sessionPolicy(predicate protocol.PredicateType, profile protocol.ProfileConfig) protocol.DeliveryPolicy {
	return protocol.DeliveryPolicy{
		Match:              protocol.PolicyMatch{PredicateType: predicate},
		LiveCredential:     protocol.RequirementNone,
		Provenance:         protocol.RequirementRequired,
		RequireEvidenceLog: true,
		Profiles:           []protocol.Digest{profileDigest(profile)},
	}
}

func testProducer(t *testing.T, subject string) *directkey.Producer {
	t.Helper()
	producer, err := directkey.NewProducer(protocol.Principal{
		Scheme:    protocol.IdentitySchemeOIDCSubV1,
		Authority: "https://issuer.example.test",
		Subject:   protocol.Subject(subject),
	})
	if err != nil {
		t.Fatalf("directkey.NewProducer: %v", err)
	}
	return producer
}

func enrollTestTarget(t *testing.T, target *directkey.Target, producer *directkey.Producer) {
	t.Helper()
	evidence, err := producer.CreateEnrollment()
	if err != nil {
		t.Fatalf("create enrollment: %v", err)
	}
	var body directkey.EnrollmentBody
	if err := json.Unmarshal(evidence.Bytes, &body); err != nil {
		t.Fatalf("decode enrollment body: %v", err)
	}
	assertionBytes, err := protocol.MarshalCanonical(directkey.EnrollmentAssertion{Principal: body.Principal, PublicKey: body.PublicKey})
	if err != nil {
		t.Fatalf("encode enrollment assertion: %v", err)
	}
	if err := target.Apply(context.Background(), protocol.ApplyRequest{
		Authenticated: protocol.AuthenticatedEvidence{Principal: body.Principal, PredicateType: directkey.PredicateTypeEnrollmentV1},
		Assertion:     protocol.TypedAssertion{PredicateType: directkey.PredicateTypeEnrollmentV1, Bytes: assertionBytes},
		Statement:     protocol.SignedStatement{Evidence: evidence},
	}); err != nil {
		t.Fatalf("apply enrollment: %v", err)
	}
}

type countedTarget struct {
	delegate    protocol.TargetAPI
	beginCalls  int
	finishCalls int
	parseCalls  int
	applyCalls  int
	requests    []protocol.VerifyRequest
	lastResult  protocol.ProvenanceAuthenticationResult
}

type mutatingApplyTarget struct{}

func (mutatingApplyTarget) ProvenanceType() protocol.ProvenanceType {
	return protocol.ProvenanceTypeDirectKeyV1
}
func (mutatingApplyTarget) ParseHints(protocol.TypedEvidence) (protocol.TentativeHints, error) {
	return protocol.TentativeHints{}, errors.New("not used")
}
func (mutatingApplyTarget) RequiresEvidenceLog() bool { return false }
func (mutatingApplyTarget) BeginVerification(context.Context, protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	return nil, errors.New("not used")
}
func (mutatingApplyTarget) Owns(predicate protocol.PredicateType) bool {
	return predicate == directkey.PredicateTypeEnrollmentV1
}
func (mutatingApplyTarget) Apply(_ context.Context, request protocol.ApplyRequest) error {
	request.Authenticated.SatisfiedConstraints[0].Name = "mutated"
	request.Assertion.Bytes[0] = 'X'
	request.Statement.Evidence.Bytes[0] = 'X'
	request.Statement.Support.Bytes[0] = 'X'
	request.Temporal.LogPosition.Index = 99
	request.Temporal.Times[0].Authority = "tsa-mutated"
	return nil
}

func (t *countedTarget) ProvenanceType() protocol.ProvenanceType { return t.delegate.ProvenanceType() }
func (t *countedTarget) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	t.parseCalls++
	return t.delegate.ParseHints(evidence)
}
func (t *countedTarget) RequiresEvidenceLog() bool { return t.delegate.RequiresEvidenceLog() }
func (t *countedTarget) BeginVerification(ctx context.Context, request protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	t.beginCalls++
	t.requests = append(t.requests, request)
	wrapped, err := t.delegate.BeginVerification(ctx, request)
	if err != nil {
		return nil, err
	}
	return &countedProfileSession{target: t, delegate: wrapped}, nil
}
func (t *countedTarget) Owns(predicate protocol.PredicateType) bool {
	return t.delegate.Owns(predicate)
}
func (t *countedTarget) Apply(ctx context.Context, request protocol.ApplyRequest) error {
	t.applyCalls++
	return t.delegate.Apply(ctx, request)
}

type countedProfileSession struct {
	target   *countedTarget
	delegate protocol.ProvenanceVerificationSession
}

func (s *countedProfileSession) Prepare(ctx context.Context) (protocol.TemporalPreparation, error) {
	return s.delegate.Prepare(ctx)
}
func (s *countedProfileSession) Finish(ctx context.Context, inputs protocol.VerifiedProvenanceTemporalInputs) (protocol.ProvenanceAuthenticationResult, error) {
	s.target.finishCalls++
	result, err := s.delegate.Finish(ctx, inputs)
	s.target.lastResult = result
	return result, err
}

type countedLogVerifier struct {
	inner protocol.OrderedLogEvidenceVerifier
	calls map[protocol.Digest]int
}

func (v *countedLogVerifier) VerifyOccurrence(ctx context.Context, evidence protocol.TypedEvidence, inclusion protocol.EvidenceLogInclusion) (protocol.VerifiedEvidenceLogBinding, error) {
	identity, err := evidence.Identity()
	if err == nil {
		v.calls[identity]++
	}
	return v.inner.VerifyOccurrence(ctx, evidence, inclusion)
}

func sessionTenant() protocol.Tenant {
	return protocol.Tenant{PrincipalAuthority: protocol.PrincipalAuthority{Scheme: protocol.IdentitySchemeOIDCSubV1, Authority: "https://issuer.example.test"}}
}
func otherSessionTenant() protocol.Tenant {
	tenant := sessionTenant()
	tenant.Partition = "other"
	return tenant
}

func profileDigest(profile protocol.ProfileConfig) protocol.Digest {
	reference, err := profile.Digest()
	if err != nil {
		panic(err)
	}
	return reference
}
