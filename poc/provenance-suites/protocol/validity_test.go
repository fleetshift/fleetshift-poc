package protocol

import (
	"errors"
	"sort"
	"testing"
	"time"
)

func TestNormalizeValidityWindowEmptyIsUnbounded(t *testing.T) {
	got, err := NormalizeValidityWindow(nil, nil)
	if err != nil {
		t.Fatalf("NormalizeValidityWindow: %v", err)
	}
	if got.Window.EstablishedBy != nil || got.Window.RetiredBy != nil {
		t.Fatalf("empty input produced a bounded window: %+v", got.Window)
	}
	if len(got.Basis) != 0 {
		t.Fatalf("empty input Basis = %v, want empty", got.Basis)
	}
}

func TestNormalizeValidityWindowSelectsGreatestAndSmallestLogIndexes(t *testing.T) {
	got, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{
			logConstraint(3, true, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			logConstraint(7, true, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			logConstraint(5, true, "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
		},
		[]AuthenticatedTemporalConstraint{
			logConstraint(20, true, "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
			logConstraint(12, true, "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
			logConstraint(18, true, "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
		},
	)
	if err != nil {
		t.Fatalf("NormalizeValidityWindow: %v", err)
	}
	if got.Window.EstablishedBy == nil || got.Window.EstablishedBy.Log == nil {
		t.Fatal("missing established log bound")
	}
	if got.Window.EstablishedBy.Log.Position.Index != 7 || !got.Window.EstablishedBy.Log.Inclusive {
		t.Fatalf("established log = %+v, want index 7 inclusive", got.Window.EstablishedBy.Log)
	}
	if got.Window.RetiredBy == nil || got.Window.RetiredBy.Log == nil {
		t.Fatal("missing retired log bound")
	}
	if got.Window.RetiredBy.Log.Position.Index != 12 || !got.Window.RetiredBy.Log.Inclusive {
		t.Fatalf("retired log = %+v, want index 12 inclusive", got.Window.RetiredBy.Log)
	}
}

func TestNormalizeValidityWindowSelectsGreatestLatestAndSmallestEarliestTime(t *testing.T) {
	t0 := instant(0)
	got, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{
			timeConstraint(t0.Add(-time.Hour), t0, true),
			timeConstraint(t0.Add(-2*time.Hour), t0.Add(time.Hour), true),
		},
		[]AuthenticatedTemporalConstraint{
			timeConstraint(t0.Add(3*time.Hour), t0.Add(4*time.Hour), true),
			timeConstraint(t0.Add(2*time.Hour), t0.Add(5*time.Hour), true),
		},
	)
	if err != nil {
		t.Fatalf("NormalizeValidityWindow: %v", err)
	}
	if got.Window.EstablishedBy == nil || got.Window.EstablishedBy.Time == nil {
		t.Fatal("missing established time bound")
	}
	if !got.Window.EstablishedBy.Time.Latest.Equal(t0.Add(time.Hour)) {
		t.Fatalf("established Latest = %v, want %v", got.Window.EstablishedBy.Time.Latest, t0.Add(time.Hour))
	}
	if got.Window.RetiredBy == nil || got.Window.RetiredBy.Time == nil {
		t.Fatal("missing retired time bound")
	}
	if !got.Window.RetiredBy.Time.Earliest.Equal(t0.Add(2 * time.Hour)) {
		t.Fatalf("retired Earliest = %v, want %v", got.Window.RetiredBy.Time.Earliest, t0.Add(2*time.Hour))
	}
}

func TestNormalizeValidityWindowExclusiveWinsTies(t *testing.T) {
	got, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{
			logConstraint(4, true),
			logConstraint(4, false),
		},
		[]AuthenticatedTemporalConstraint{
			logConstraint(9, true),
			logConstraint(9, false),
		},
	)
	if err != nil {
		t.Fatalf("NormalizeValidityWindow: %v", err)
	}
	if got.Window.EstablishedBy.Log.Inclusive {
		t.Fatal("established tie kept the inclusive bound; exclusive is stricter")
	}
	if got.Window.RetiredBy.Log.Inclusive {
		t.Fatal("retired tie kept the inclusive bound; exclusive is stricter")
	}

	t0 := instant(0)
	timed, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{
			timeConstraint(t0, t0, true),
			timeConstraint(t0, t0, false),
		},
		[]AuthenticatedTemporalConstraint{
			timeConstraint(t0.Add(time.Hour), t0.Add(time.Hour), true),
			timeConstraint(t0.Add(time.Hour), t0.Add(time.Hour), false),
		},
	)
	if err != nil {
		t.Fatalf("time NormalizeValidityWindow: %v", err)
	}
	if timed.Window.EstablishedBy.Time.Inclusive {
		t.Fatal("established time tie kept the inclusive bound")
	}
	if timed.Window.RetiredBy.Time.Inclusive {
		t.Fatal("retired time tie kept the inclusive bound")
	}
}

func TestNormalizeValidityWindowBasisIsDeduplicatedUnionIncludingDominated(t *testing.T) {
	dup := Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	dominated := Digest("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	winner := Digest("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	got, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{
			logConstraint(1, true, dup, dominated),
			logConstraint(3, true, winner, dup),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("NormalizeValidityWindow: %v", err)
	}
	if got.Window.EstablishedBy.Log.Position.Index != 3 {
		t.Fatalf("established index = %d, want 3", got.Window.EstablishedBy.Log.Position.Index)
	}
	want := []Digest{dominated, winner, dup}
	sortDigests(want)
	if len(got.Basis) != 3 {
		t.Fatalf("Basis = %v, want 3 unique digests including dominated", got.Basis)
	}
	for i, d := range want {
		if got.Basis[i] != d {
			t.Fatalf("Basis[%d] = %q, want %q", i, got.Basis[i], d)
		}
	}
}

func TestNormalizeValidityWindowRejectsCrossDomainAndInverted(t *testing.T) {
	_, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(1, true)},
		[]AuthenticatedTemporalConstraint{{
			Boundary: AuthenticatedTemporalBoundary{
				Log: &AuthenticatedLogBoundary{
					Position:  LogPosition{Domain: "other-log/v1", Index: 8},
					Inclusive: true,
				},
			},
		}},
	)
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("cross-domain error = %v, want ErrTemporalValidity", err)
	}

	_, err = NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(10, true)},
		[]AuthenticatedTemporalConstraint{logConstraint(5, true)},
	)
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("inverted log error = %v, want ErrTemporalValidity", err)
	}

	_, err = NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(5, false)},
		[]AuthenticatedTemporalConstraint{logConstraint(6, false)},
	)
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("unsatisfiable exclusive log window error = %v, want ErrTemporalValidity", err)
	}

	t0 := instant(0)
	_, err = NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{timeConstraint(t0, t0.Add(2*time.Hour), true)},
		[]AuthenticatedTemporalConstraint{timeConstraint(t0, t0.Add(time.Hour), true)},
	)
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("inverted time error = %v, want ErrTemporalValidity", err)
	}
}

func TestNormalizeValidityWindowKeepsIndependentAttemptsSeparate(t *testing.T) {
	first, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(5, false)},
		[]AuthenticatedTemporalConstraint{logConstraint(15, false)},
	)
	if err != nil {
		t.Fatalf("first attempt: %v", err)
	}
	second, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(20, false)},
		[]AuthenticatedTemporalConstraint{logConstraint(30, false)},
	)
	if err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	if first.Window.EstablishedBy.Log.Position.Index != 5 || first.Window.RetiredBy.Log.Position.Index != 15 {
		t.Fatalf("first window = %+v", first.Window)
	}
	if second.Window.EstablishedBy.Log.Position.Index != 20 || second.Window.RetiredBy.Log.Position.Index != 30 {
		t.Fatalf("second window = %+v", second.Window)
	}

	_, err = NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(5, false), logConstraint(20, false)},
		[]AuthenticatedTemporalConstraint{logConstraint(15, false), logConstraint(30, false)},
	)
	if !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("merged attempts error = %v, want inverted ErrTemporalValidity", err)
	}
}

func TestCheckTemporalValidityLogInclusiveAndExclusive(t *testing.T) {
	inclusive, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(10, true)},
		[]AuthenticatedTemporalConstraint{logConstraint(20, true)},
	)
	if err != nil {
		t.Fatalf("inclusive window: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(10), inclusive.Window); err != nil {
		t.Fatalf("index 10 at inclusive established: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(20), inclusive.Window); err != nil {
		t.Fatalf("index 20 at inclusive retired: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(9), inclusive.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("index 9 error = %v, want ErrTemporalValidity", err)
	}
	if err := CheckTemporalValidity(subjectLog(21), inclusive.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("index 21 error = %v, want ErrTemporalValidity", err)
	}

	exclusive, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(10, false)},
		[]AuthenticatedTemporalConstraint{logConstraint(20, false)},
	)
	if err != nil {
		t.Fatalf("exclusive window: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(11), exclusive.Window); err != nil {
		t.Fatalf("index 11 inside exclusive window: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(10), exclusive.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("index 10 at exclusive established error = %v, want ErrTemporalValidity", err)
	}
	if err := CheckTemporalValidity(subjectLog(20), exclusive.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("index 20 at exclusive retired error = %v, want ErrTemporalValidity", err)
	}
}

func TestCheckTemporalValidityTimeInsideOutsideAndOverlap(t *testing.T) {
	t0 := instant(0)
	window, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{timeConstraint(t0, t0.Add(time.Hour), false)},
		[]AuthenticatedTemporalConstraint{timeConstraint(t0.Add(3*time.Hour), t0.Add(4*time.Hour), false)},
	)
	if err != nil {
		t.Fatalf("time window: %v", err)
	}

	inside := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-test",
			Earliest:  t0.Add(2 * time.Hour),
			Latest:    t0.Add(2 * time.Hour),
		}},
	}
	if err := CheckTemporalValidity(inside, window.Window); err != nil {
		t.Fatalf("point inside time window: %v", err)
	}

	before := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-test",
			Earliest:  t0.Add(30 * time.Minute),
			Latest:    t0.Add(90 * time.Minute),
		}},
	}
	if err := CheckTemporalValidity(before, window.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("overlapping establishment error = %v, want ErrTemporalValidity", err)
	}

	atBound := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-test",
			Earliest:  t0.Add(time.Hour),
			Latest:    t0.Add(time.Hour),
		}},
	}
	if err := CheckTemporalValidity(atBound, window.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("exclusive established equality error = %v, want ErrTemporalValidity", err)
	}

	inclusive, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{timeConstraint(t0, t0.Add(time.Hour), true)},
		[]AuthenticatedTemporalConstraint{timeConstraint(t0.Add(3*time.Hour), t0.Add(4*time.Hour), true)},
	)
	if err != nil {
		t.Fatalf("inclusive time window: %v", err)
	}
	if err := CheckTemporalValidity(atBound, inclusive.Window); err != nil {
		t.Fatalf("inclusive established equality: %v", err)
	}
}

func TestCheckTemporalValidityObservedByIsBoundaryMetadata(t *testing.T) {
	t0 := instant(0)
	establishedByTSA, declaredLater := []AuthenticatedTemporalConstraint{
		timeConstraintObserved(t0, t0.Add(time.Hour), true, "tsa-a"),
	}, []AuthenticatedTemporalConstraint{
		timeConstraint(t0.Add(time.Hour), t0.Add(2*time.Hour), true),
	}

	fromOtherTSA := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-b",
			Earliest:  t0.Add(3 * time.Hour),
			Latest:    t0.Add(3 * time.Hour),
		}},
	}

	observedOnly, err := NormalizeValidityWindow(establishedByTSA, nil)
	if err != nil {
		t.Fatalf("observed-only window: %v", err)
	}
	if err := CheckTemporalValidity(fromOtherTSA, observedOnly.Window); err != nil {
		t.Fatalf("approved observation from another TSA against observed boundary: %v", err)
	}

	combined, err := NormalizeValidityWindow(append(establishedByTSA, declaredLater...), nil)
	if err != nil {
		t.Fatalf("combined window: %v", err)
	}
	if err := CheckTemporalValidity(fromOtherTSA, combined.Window); err != nil {
		t.Fatalf("approved observation from another TSA against stronger declared boundary: %v", err)
	}

	permuted, err := NormalizeValidityWindow(append(declaredLater, establishedByTSA...), nil)
	if err != nil {
		t.Fatalf("permuted combined window: %v", err)
	}
	if err := CheckTemporalValidity(fromOtherTSA, permuted.Window); err != nil {
		t.Fatalf("permuted constraints changed acceptance: %v", err)
	}

	betweenBounds := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-b",
			Earliest:  t0.Add(90 * time.Minute),
			Latest:    t0.Add(90 * time.Minute),
		}},
	}
	if err := CheckTemporalValidity(betweenBounds, observedOnly.Window); err != nil {
		t.Fatalf("observation after original boundary: %v", err)
	}
	if err := CheckTemporalValidity(betweenBounds, combined.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("stronger declared boundary error = %v, want ErrTemporalValidity", err)
	}
}

func TestCheckTemporalValidityObservedByTieOrderDoesNotChangeAcceptance(t *testing.T) {
	t0 := instant(0)
	fromTSAB := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-b",
			Earliest:  t0.Add(2 * time.Hour),
			Latest:    t0.Add(2 * time.Hour),
		}},
	}

	constraintA := timeConstraintObserved(t0, t0.Add(time.Hour), true, "tsa-a")
	constraintB := timeConstraintObserved(t0, t0.Add(time.Hour), true, "tsa-b")
	for name, constraints := range map[string][]AuthenticatedTemporalConstraint{
		"tsa-a first": {constraintA, constraintB},
		"tsa-b first": {constraintB, constraintA},
	} {
		t.Run(name, func(t *testing.T) {
			validity, err := NormalizeValidityWindow(constraints, nil)
			if err != nil {
				t.Fatalf("NormalizeValidityWindow: %v", err)
			}
			if err := CheckTemporalValidity(fromTSAB, validity.Window); err != nil {
				t.Fatalf("tie order changed acceptance: %v", err)
			}
		})
	}
}

func TestCheckTemporalValidityOneObservationMustSatisfyBothTimeBounds(t *testing.T) {
	t0 := instant(0)
	window, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{timeConstraintObserved(t0, t0.Add(time.Hour), true, "tsa-a")},
		[]AuthenticatedTemporalConstraint{timeConstraintObserved(t0.Add(3*time.Hour), t0.Add(4*time.Hour), true, "tsa-b")},
	)
	if err != nil {
		t.Fatalf("time window: %v", err)
	}

	separateObservations := VerifiedSubjectTemporalInfo{
		Times: []VerifiedSubjectTime{
			{
				Authority: "tsa-b",
				Earliest:  t0.Add(30 * time.Minute),
				Latest:    t0.Add(30 * time.Minute),
			},
			{
				Authority: "tsa-a",
				Earliest:  t0.Add(5 * time.Hour),
				Latest:    t0.Add(5 * time.Hour),
			},
		},
	}
	if err := CheckTemporalValidity(separateObservations, window.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("separate observations satisfying opposite sides error = %v, want ErrTemporalValidity", err)
	}
}

func TestCheckTemporalValidityCoordinatePresenceAndIgnoreExtras(t *testing.T) {
	logOnly, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(4, true)},
		[]AuthenticatedTemporalConstraint{logConstraint(8, true)},
	)
	if err != nil {
		t.Fatalf("log-only window: %v", err)
	}
	t0 := instant(0)
	withExtraTime := VerifiedSubjectTemporalInfo{
		LogPosition: &LogPosition{Domain: LogDomainTenantEvidenceV1, Index: 6},
		Times: []VerifiedSubjectTime{{
			Authority: "unused",
			Earliest:  t0.Add(-time.Hour),
			Latest:    t0.Add(-time.Hour),
		}},
	}
	if err := CheckTemporalValidity(withExtraTime, logOnly.Window); err != nil {
		t.Fatalf("log-only window should ignore extra times: %v", err)
	}
	if err := CheckTemporalValidity(VerifiedSubjectTemporalInfo{Times: withExtraTime.Times}, logOnly.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("log window without subject log error = %v, want ErrTemporalValidity", err)
	}

	timeOnly, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{timeConstraint(t0, t0, true)},
		[]AuthenticatedTemporalConstraint{timeConstraint(t0.Add(2*time.Hour), t0.Add(2*time.Hour), true)},
	)
	if err != nil {
		t.Fatalf("time-only window: %v", err)
	}
	withExtraLog := VerifiedSubjectTemporalInfo{
		LogPosition: &LogPosition{Domain: LogDomainTenantEvidenceV1, Index: 99},
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-test",
			Earliest:  t0.Add(time.Hour),
			Latest:    t0.Add(time.Hour),
		}},
	}
	if err := CheckTemporalValidity(withExtraLog, timeOnly.Window); err != nil {
		t.Fatalf("time-only window should ignore extra log position: %v", err)
	}
	if err := CheckTemporalValidity(VerifiedSubjectTemporalInfo{LogPosition: withExtraLog.LogPosition}, timeOnly.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("time window without subject time error = %v, want ErrTemporalValidity", err)
	}

	both, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{
			logConstraint(4, true),
			timeConstraint(t0, t0, true),
		},
		[]AuthenticatedTemporalConstraint{
			logConstraint(8, true),
			timeConstraint(t0.Add(2*time.Hour), t0.Add(2*time.Hour), true),
		},
	)
	if err != nil {
		t.Fatalf("both-coordinate window: %v", err)
	}
	if err := CheckTemporalValidity(withExtraTime, both.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("both-coordinate window with time outside error = %v, want ErrTemporalValidity", err)
	}
	okBoth := VerifiedSubjectTemporalInfo{
		LogPosition: &LogPosition{Domain: LogDomainTenantEvidenceV1, Index: 6},
		Times: []VerifiedSubjectTime{{
			Authority: "tsa-test",
			Earliest:  t0.Add(time.Hour),
			Latest:    t0.Add(time.Hour),
		}},
	}
	if err := CheckTemporalValidity(okBoth, both.Window); err != nil {
		t.Fatalf("subject matching both coordinates: %v", err)
	}

	if err := CheckTemporalValidity(okBoth, AuthenticatedValidityWindow{}); err != nil {
		t.Fatalf("omitted sides are unbounded: %v", err)
	}

	wrongDomain := VerifiedSubjectTemporalInfo{
		LogPosition: &LogPosition{Domain: "other-log/v1", Index: 6},
	}
	if err := CheckTemporalValidity(wrongDomain, logOnly.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("mismatched log domain error = %v, want ErrTemporalValidity", err)
	}
}

func TestCheckTemporalValidityContinuityShapedLogWindow(t *testing.T) {
	validity, err := NormalizeValidityWindow(
		[]AuthenticatedTemporalConstraint{logConstraint(10, false)},
		[]AuthenticatedTemporalConstraint{logConstraint(20, false)},
	)
	if err != nil {
		t.Fatalf("E/R window: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(15), validity.Window); err != nil {
		t.Fatalf("E < X < R: %v", err)
	}
	if err := CheckTemporalValidity(subjectLog(21), validity.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("X after R error = %v, want ErrTemporalValidity", err)
	}
	if err := CheckTemporalValidity(subjectLog(10), validity.Window); !errors.Is(err, ErrTemporalValidity) {
		t.Fatalf("X at exclusive E error = %v, want ErrTemporalValidity", err)
	}
}

func logConstraint(index uint64, inclusive bool, basis ...Digest) AuthenticatedTemporalConstraint {
	return AuthenticatedTemporalConstraint{
		Boundary: AuthenticatedTemporalBoundary{
			Log: &AuthenticatedLogBoundary{
				Position:  LogPosition{Domain: LogDomainTenantEvidenceV1, Index: index},
				Inclusive: inclusive,
			},
		},
		Basis: basis,
	}
}

func timeConstraint(earliest, latest time.Time, inclusive bool) AuthenticatedTemporalConstraint {
	return AuthenticatedTemporalConstraint{
		Boundary: AuthenticatedTemporalBoundary{
			Time: &AuthenticatedTimeBoundary{
				Earliest:  earliest,
				Latest:    latest,
				Inclusive: inclusive,
			},
		},
	}
}

func timeConstraintObserved(earliest, latest time.Time, inclusive bool, observed TimestampAuthorityID) AuthenticatedTemporalConstraint {
	c := timeConstraint(earliest, latest, inclusive)
	id := observed
	c.Boundary.Time.ObservedBy = &id
	return c
}

func subjectLog(index uint64) VerifiedSubjectTemporalInfo {
	pos := LogPosition{Domain: LogDomainTenantEvidenceV1, Index: index}
	return VerifiedSubjectTemporalInfo{LogPosition: &pos}
}

func instant(offset time.Duration) time.Time {
	return time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC).Add(offset)
}

func sortDigests(digests []Digest) {
	sort.Slice(digests, func(i, j int) bool { return digests[i] < digests[j] })
}
