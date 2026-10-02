package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/internal/merklelog"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/resourcemanager"
)

const (
	continuityTestProvenanceType protocol.ProvenanceType = "continuity-test/v1"
	continuityTestPredicateType  protocol.PredicateType  = "continuity-test/v1"
	continuityTestMediaType      protocol.MediaType      = "application/continuity-test+json"
	continuityTestAuthority      protocol.Authority      = "https://continuity.example.test"
)

func TestContinuityProfileUsesAcceptedBoundaryPositionsThroughSelection(t *testing.T) {
	tree := merklelog.New()
	evidence := make(map[string]protocol.TypedEvidence, 4)
	var checkpoint protocol.Checkpoint = protocol.EmptyCheckpoint()
	for _, name := range []string{"E", "X", "R", "X-late"} {
		item := continuityTestEvidence(name)
		update, _ := mustAppendLog(t, tree, checkpoint, item)
		checkpoint = update.Checkpoint
		evidence[name] = item
	}

	inclusions := make(map[string]protocol.EvidenceLogInclusion, len(evidence))
	for index, name := range []string{"E", "X", "R", "X-late"} {
		inclusions[name] = mustEvidenceLogInclusion(t, tree, uint64(index))
	}

	target := &continuityTestTarget{}
	trust := continuityTestTrust()
	retained := RetainedState{EvidenceLog: protocol.EmptyCheckpoint()}
	fullHead := mustCheckpoint(t, tree, tree.Size())

	// E and R are accepted and applied as ordinary deliveries. Their packages
	// each carry only the current root inclusion, and Apply commits the checked
	// position into deterministic profile-owned state.
	for _, name := range []string{"E", "R"} {
		pkg := continuityTestPackage(t, tree, retained.EvidenceLog, evidence[name], inclusions[name])
		result, err := deliverContinuityTestPackage(t, target, trust, &retained, pkg)
		if err != nil {
			t.Fatalf("deliver %s: %v", name, err)
		}
		if result.Temporal.LogPosition == nil || result.Temporal.LogPosition.Index != inclusions[name].Index {
			t.Fatalf("%s temporal position = %+v, want index %d", name, result.Temporal.LogPosition, inclusions[name].Index)
		}
		if retained.EvidenceLog != fullHead {
			t.Fatalf("retained checkpoint after %s = %+v, want prebuilt head %+v", name, retained.EvidenceLog, fullHead)
		}
		boundary := target.established
		if name == "R" {
			boundary = target.retired
		}
		if got := boundary; got == nil || got.position.Index != inclusions[name].Index {
			t.Fatalf("profile did not retain accepted %s position: %+v", name, got)
		}
		wantTrace := []string{"prepare:" + name, "log:" + name, "finish:" + name, "apply:" + name}
		if !slices.Equal(target.trace, wantTrace) {
			t.Fatalf("%s pipeline trace = %v, want %v", name, target.trace, wantTrace)
		}
		target.trace = nil
	}

	// The X package reuses the already verified head and contains only X's
	// current inclusion. The accepted E and R positions come from profile state.
	xPackage := continuityTestPackage(t, tree, retained.EvidenceLog, evidence["X"], inclusions["X"])
	if len(xPackage.Supporting) != 0 {
		t.Fatalf("X package has %d supporting items, want no fresh E/R items", len(xPackage.Supporting))
	}
	if xPackage.Root.EvidenceLog == nil || xPackage.Root.EvidenceLog.Index != inclusions["X"].Index {
		t.Fatalf("X package root inclusion = %+v, want X at index %d", xPackage.Root.EvidenceLog, inclusions["X"].Index)
	}
	rootIdentity, err := xPackage.Root.Evidence.Identity()
	if err != nil {
		t.Fatalf("X root identity: %v", err)
	}
	wantXIdentity, err := evidence["X"].Identity()
	if err != nil {
		t.Fatalf("X evidence identity: %v", err)
	}
	if rootIdentity != wantXIdentity {
		t.Fatal("X package root is not the X evidence item")
	}

	xResult, err := deliverContinuityTestPackage(t, target, trust, &retained, xPackage)
	if err != nil {
		t.Fatalf("deliver X between accepted E and R: %v", err)
	}
	if got := xResult.Temporal.LogPosition; got == nil || got.Index != inclusions["X"].Index {
		t.Fatalf("X subject position = %+v, want current X index %d", got, inclusions["X"].Index)
	}
	if got := xResult.Validity.Window.EstablishedBy; got == nil || got.Log == nil || got.Log.Position.Index != inclusions["E"].Index || got.Log.Inclusive {
		t.Fatalf("X established bound = %+v, want exclusive accepted E index %d", got, inclusions["E"].Index)
	}
	if got := xResult.Validity.Window.RetiredBy; got == nil || got.Log == nil || got.Log.Position.Index != inclusions["R"].Index || got.Log.Inclusive {
		t.Fatalf("X retired bound = %+v, want exclusive accepted R index %d", got, inclusions["R"].Index)
	}
	wantBasis := []protocol.Digest{mustIdentity(t, evidence["E"]), mustIdentity(t, evidence["R"])}
	gotBasis := xResult.Validity.Basis
	if len(gotBasis) != len(wantBasis) || !slices.Contains(gotBasis, wantBasis[0]) || !slices.Contains(gotBasis, wantBasis[1]) {
		t.Fatalf("X validity basis = %v, want accepted E/R identities %v", gotBasis, wantBasis)
	}
	wantXTrace := []string{"prepare:X", "log:X", "finish:X", "apply:X"}
	if !slices.Equal(target.trace, wantXTrace) {
		t.Fatalf("X pipeline trace = %v, want %v", target.trace, wantXTrace)
	}
	if !slices.Equal(target.applied, []string{"E", "R", "X"}) {
		t.Fatalf("applied continuity items = %v, want [E R X]", target.applied)
	}

	// X-late has a valid current inclusion but falls after R's retained cutoff.
	// It reaches Finish and normalization, then the common final validity check
	// rejects it before the profile's Apply method can run.
	latePackage := continuityTestPackage(t, tree, retained.EvidenceLog, evidence["X-late"], inclusions["X-late"])
	if len(latePackage.Supporting) != 0 {
		t.Fatalf("X-late package has %d supporting items, want no fresh E/R items", len(latePackage.Supporting))
	}
	if !slices.Equal(target.applied, []string{"E", "R", "X"}) {
		t.Fatalf("precondition applied items = %v, want [E R X]", target.applied)
	}
	target.trace = nil
	_, err = deliverContinuityTestPackage(t, target, trust, &retained, latePackage)
	if !errors.Is(err, protocol.ErrTemporalValidity) || !errors.Is(err, protocol.ErrNoSuccessfulProfile) {
		t.Fatalf("deliver X-late after R error = %v, want temporal validity rejection", err)
	}
	wantLateTrace := []string{"prepare:X-late", "log:X-late", "finish:X-late"}
	if !slices.Equal(target.trace, wantLateTrace) {
		t.Fatalf("X-late pipeline trace = %v, want %v", target.trace, wantLateTrace)
	}
	if !slices.Equal(target.applied, []string{"E", "R", "X"}) {
		t.Fatalf("applied continuity items after X-late = %v, want [E R X]", target.applied)
	}
}

type continuityTestBoundary struct {
	position protocol.LogPosition
	evidence protocol.Digest
}

type continuityTestTarget struct {
	established *continuityTestBoundary
	retired     *continuityTestBoundary
	trace       []string
	applied     []string
}

func (t *continuityTestTarget) ProvenanceType() protocol.ProvenanceType {
	return continuityTestProvenanceType
}

func (t *continuityTestTarget) ParseHints(evidence protocol.TypedEvidence) (protocol.TentativeHints, error) {
	if evidence.ProvenanceType != continuityTestProvenanceType || evidence.MediaType != continuityTestMediaType {
		return protocol.TentativeHints{}, fmt.Errorf("unexpected continuity test evidence envelope")
	}
	return continuityTestHints(), nil
}

func (t *continuityTestTarget) RequiresEvidenceLog() bool { return true }

func (t *continuityTestTarget) BeginVerification(_ context.Context, request protocol.VerifyRequest) (protocol.ProvenanceVerificationSession, error) {
	if request.Statement.Evidence.ProvenanceType != continuityTestProvenanceType {
		return nil, protocol.ErrUnknownProvenanceType
	}
	kind := string(request.Statement.Evidence.Bytes)
	if kind != "E" && kind != "X" && kind != "R" && kind != "X-late" {
		return nil, fmt.Errorf("unknown continuity test item %q", kind)
	}
	established, retired := cloneContinuityBoundary(t.established), cloneContinuityBoundary(t.retired)
	return &continuityTestSession{
		target:      t,
		request:     request,
		kind:        kind,
		established: established,
		retired:     retired,
	}, nil
}

func (t *continuityTestTarget) Owns(predicate protocol.PredicateType) bool {
	return predicate == continuityTestPredicateType
}

func (t *continuityTestTarget) Apply(_ context.Context, request protocol.ApplyRequest) error {
	kind := string(request.Statement.Evidence.Bytes)
	if request.Authenticated.PredicateType != continuityTestPredicateType || request.Assertion.PredicateType != continuityTestPredicateType {
		return protocol.ErrPolicyReevaluation
	}
	if kind == "E" || kind == "R" {
		if request.Temporal.LogPosition == nil {
			return fmt.Errorf("accepted %s has no common-verified log position", kind)
		}
		identity, err := request.Statement.Evidence.Identity()
		if err != nil {
			return err
		}
		boundary := &continuityTestBoundary{position: *request.Temporal.LogPosition, evidence: identity}
		if kind == "E" {
			t.established = boundary
		} else {
			t.retired = boundary
		}
	}
	t.applied = append(t.applied, kind)
	t.trace = append(t.trace, "apply:"+kind)
	return nil
}

type continuityTestSession struct {
	target      *continuityTestTarget
	request     protocol.VerifyRequest
	kind        string
	established *continuityTestBoundary
	retired     *continuityTestBoundary
	prepared    bool
	finished    bool
}

func (s *continuityTestSession) Prepare(context.Context) (protocol.TemporalPreparation, error) {
	if s.prepared || s.finished {
		return protocol.TemporalPreparation{}, protocol.ErrVerificationFailed
	}
	s.prepared = true
	s.target.trace = append(s.target.trace, "prepare:"+s.kind)
	return protocol.TemporalPreparation{}, nil
}

func (s *continuityTestSession) Finish(_ context.Context, inputs protocol.VerifiedProvenanceTemporalInputs) (protocol.ProvenanceAuthenticationResult, error) {
	if !s.prepared || s.finished {
		return protocol.ProvenanceAuthenticationResult{}, protocol.ErrVerificationFailed
	}
	s.finished = true
	s.target.trace = append(s.target.trace, "finish:"+s.kind)
	if len(inputs.Timestamps) != 0 {
		return protocol.ProvenanceAuthenticationResult{}, fmt.Errorf("continuity test profile does not accept timestamp observations")
	}

	assertion := protocol.TypedAssertion{
		PredicateType: continuityTestPredicateType,
		Bytes:         append([]byte(nil), s.request.Statement.Evidence.Bytes...),
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		return protocol.ProvenanceAuthenticationResult{}, err
	}
	authorityDigest, err := s.request.AuthorityConfig.Digest()
	if err != nil {
		return protocol.ProvenanceAuthenticationResult{}, err
	}
	profileDigest, err := s.request.ProfileConfig.Digest()
	if err != nil {
		return protocol.ProvenanceAuthenticationResult{}, err
	}
	principal := protocol.Principal{
		Scheme:    protocol.IdentitySchemeOIDCSubV1,
		Authority: continuityTestAuthority,
		Subject:   "alice",
	}
	authenticated := protocol.AuthenticatedEvidence{
		Principal: principal,

		PredicateType:         continuityTestPredicateType,
		ContentDigest:         contentDigest,
		ProvenanceType:        continuityTestProvenanceType,
		AuthorityConfigDigest: authorityDigest,
		ProfileConfigDigest:   profileDigest,
	}
	result := protocol.ProvenanceAuthenticationResult{
		Authenticated: authenticated,
		Assertion:     assertion,
	}
	if s.kind != "X" && s.kind != "X-late" {
		return result, nil
	}
	if s.established == nil || s.retired == nil {
		return protocol.ProvenanceAuthenticationResult{}, fmt.Errorf("continuity bounds have not both been accepted")
	}
	result.Established = []protocol.AuthenticatedTemporalConstraint{continuityTestConstraint(*s.established)}
	result.Retired = []protocol.AuthenticatedTemporalConstraint{continuityTestConstraint(*s.retired)}
	return result, nil
}

type continuityTraceLog struct {
	inner  protocol.OrderedLogEvidenceVerifier
	target *continuityTestTarget
}

func (l continuityTraceLog) VerifyOccurrence(ctx context.Context, evidence protocol.TypedEvidence, inclusion protocol.EvidenceLogInclusion) (protocol.VerifiedEvidenceLogBinding, error) {
	l.target.trace = append(l.target.trace, "log:"+string(evidence.Bytes))
	return l.inner.VerifyOccurrence(ctx, evidence, inclusion)
}

func deliverContinuityTestPackage(t *testing.T, target *continuityTestTarget, trust protocol.TrustConfiguration, retained *RetainedState, pkg resourcemanager.DeliveryPackage) (protocol.VerificationResult, error) {
	t.Helper()
	prepared, err := Prepare(*retained, pkg.EvidenceLog, pkg.Root.Evidence, pkg.Root.EvidenceLog)
	if err != nil {
		return protocol.VerificationResult{}, err
	}
	// Like Agent.Deliver, retain the independently verified prefix before
	// provenance, temporal-window, and apply decisions.
	*retained = prepared.NextState
	lookup := func(kind protocol.ProvenanceType) (protocol.TargetAPI, bool) {
		if kind != target.ProvenanceType() {
			return nil, false
		}
		return target, true
	}
	result, err := protocol.SelectAndVerify(context.Background(), pkg.Root, trust, lookup, protocol.TemporalVerificationServices{
		Log: continuityTraceLog{inner: prepared.Log, target: target},
	})
	if err != nil {
		return protocol.VerificationResult{}, err
	}
	if !target.Owns(result.Authenticated.PredicateType) {
		return protocol.VerificationResult{}, protocol.ErrUnknownPredicateType
	}
	err = target.Apply(context.Background(), protocol.ApplyRequest{
		Authenticated: result.Authenticated,
		Assertion:     result.Assertion,
		Statement:     pkg.Root.SignedStatement,
		Temporal:      result.Temporal,
	})
	if err != nil {
		return protocol.VerificationResult{}, err
	}
	return result, nil
}

func continuityTestPackage(t *testing.T, tree *merklelog.Tree, retained protocol.Checkpoint, evidence protocol.TypedEvidence, inclusion protocol.EvidenceLogInclusion) resourcemanager.DeliveryPackage {
	t.Helper()
	update := mustEvidenceLogUpdate(t, tree, retained)
	return resourcemanager.DeliveryPackage{
		Root: protocol.Item{
			SignedStatement: protocol.SignedStatement{Evidence: evidence},
			EvidenceLog:     &inclusion,
		},
		EvidenceLog: &update,
	}
}

func continuityTestEvidence(kind string) protocol.TypedEvidence {
	return protocol.TypedEvidence{
		ProvenanceType: continuityTestProvenanceType,
		Encoded: protocol.Encoded{
			MediaType: continuityTestMediaType,
			Bytes:     []byte(kind),
		},
	}
}

func continuityTestTrust() protocol.TrustConfiguration {
	profile := protocol.ProfileConfig{ProvenanceType: continuityTestProvenanceType}
	authority := protocol.AuthorityConfig{
		PrincipalAuthority: protocol.PrincipalAuthority{
			Scheme:    protocol.IdentitySchemeOIDCSubV1,
			Authority: continuityTestAuthority,
		},

		ProvenanceProfiles: []protocol.ProfileConfig{profile},
		DeliveryPolicies: []protocol.DeliveryPolicy{{
			Match: protocol.PolicyMatch{
				PredicateType: continuityTestPredicateType,
			},
			LiveCredential:     protocol.RequirementNone,
			Provenance:         protocol.RequirementRequired,
			RequireEvidenceLog: true,
			Profiles:           []protocol.Digest{profileDigest(profile)},
		}},
	}
	return protocol.TrustConfiguration{AuthorityRegistry: []protocol.AuthorityConfig{authority}}
}

func continuityTestHints() protocol.TentativeHints {
	return protocol.TentativeHints{
		Scheme:    protocol.IdentitySchemeOIDCSubV1,
		Authority: continuityTestAuthority,
		Subject:   "alice",
		Assertion: protocol.TypedAssertion{PredicateType: continuityTestPredicateType},
	}
}

func continuityTestConstraint(boundary continuityTestBoundary) protocol.AuthenticatedTemporalConstraint {
	return protocol.AuthenticatedTemporalConstraint{
		Boundary: protocol.AuthenticatedTemporalBoundary{
			Log: &protocol.AuthenticatedLogBoundary{
				Position:  boundary.position,
				Inclusive: false,
			},
		},
		Basis: []protocol.Digest{boundary.evidence},
	}
}

func cloneContinuityBoundary(in *continuityTestBoundary) *continuityTestBoundary {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
