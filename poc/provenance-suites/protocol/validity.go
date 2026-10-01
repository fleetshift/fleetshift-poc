package protocol

import (
	"fmt"
	"sort"
	"time"
)

// AuthenticatedLogBoundary is a provenance-authenticated log coordinate
// used as a validity bound. Inclusive is coordinate-specific.
type AuthenticatedLogBoundary struct {
	Position  LogPosition
	Inclusive bool
}

// AuthenticatedTimeBoundary is a provenance-authenticated wall-time
// coordinate used as a validity bound. ObservedBy records the TSA source
// when the bound came from an accepted observation; it is provenance
// metadata and does not constrain the source of a subject observation. Nil
// denotes a declared wall-time field. Inclusive is coordinate-specific.
type AuthenticatedTimeBoundary struct {
	Earliest   time.Time
	Latest     time.Time
	ObservedBy *TimestampAuthorityID
	Inclusive  bool
}

// AuthenticatedTemporalBoundary is one side of a validity window. Log and
// time never substitute for each other; either or both may be present.
type AuthenticatedTemporalBoundary struct {
	Log  *AuthenticatedLogBoundary
	Time *AuthenticatedTimeBoundary
}

// AuthenticatedValidityWindow is the normalized coordinate window
// consumed by CheckTemporalValidity. An omitted side is unbounded.
type AuthenticatedValidityWindow struct {
	EstablishedBy *AuthenticatedTemporalBoundary
	RetiredBy     *AuthenticatedTemporalBoundary
}

// AuthenticatedValidity is the normalized window plus the audit basis of
// the constraints that produced it. Basis is not part of the window
// passed to the checker.
type AuthenticatedValidity struct {
	Window AuthenticatedValidityWindow
	Basis  []Digest
}

// AuthenticatedTemporalConstraint is one established or retired cutoff
// returned by Finish, together with the evidence authenticating it.
// Common code treats Basis and nested boundary pointers as immutable.
type AuthenticatedTemporalConstraint struct {
	Boundary AuthenticatedTemporalBoundary
	Basis    []Digest
}

// NormalizeValidityWindow reduces one successful profile attempt's
// established and retired constraints to a single coordinate window.
// Empty input is an unbounded window. Cross-domain log positions, inverted
// windows, and unsatisfiable discrete or inclusive bounds fail with
// ErrTemporalValidity. Basis is the deterministic deduplicated union of
// every constraint basis, including dominated constraints.
// Inputs are immutable; the normalized window shares its selected boundaries.
func NormalizeValidityWindow(established, retired []AuthenticatedTemporalConstraint) (AuthenticatedValidity, error) {
	if err := validateConstraintGroups(established, retired); err != nil {
		return AuthenticatedValidity{}, err
	}
	if _, err := collectLogDomain(established, retired); err != nil {
		return AuthenticatedValidity{}, err
	}

	establishedBy, err := normalizeSide(established, true)
	if err != nil {
		return AuthenticatedValidity{}, err
	}
	retiredBy, err := normalizeSide(retired, false)
	if err != nil {
		return AuthenticatedValidity{}, err
	}
	if err := checkWindowSatisfiable(establishedBy, retiredBy); err != nil {
		return AuthenticatedValidity{}, err
	}
	return AuthenticatedValidity{
		Window: AuthenticatedValidityWindow{
			EstablishedBy: establishedBy,
			RetiredBy:     retiredBy,
		},
		Basis: unionBasis(established, retired),
	}, nil
}

func validateConstraintGroups(groups ...[]AuthenticatedTemporalConstraint) error {
	for _, group := range groups {
		for _, c := range group {
			if c.Boundary.Log == nil && c.Boundary.Time == nil {
				return fmt.Errorf("%w: temporal constraint has no log or time coordinate", ErrTemporalValidity)
			}
			if c.Boundary.Time != nil && c.Boundary.Time.Earliest.After(c.Boundary.Time.Latest) {
				return fmt.Errorf("%w: time boundary earliest is after latest", ErrTemporalValidity)
			}
		}
	}
	return nil
}

func collectLogDomain(groups ...[]AuthenticatedTemporalConstraint) (LogDomainID, error) {
	var domain LogDomainID
	seen := false
	for _, group := range groups {
		for _, c := range group {
			if c.Boundary.Log == nil {
				continue
			}
			if !seen {
				domain = c.Boundary.Log.Position.Domain
				seen = true
				continue
			}
			if c.Boundary.Log.Position.Domain != domain {
				return "", fmt.Errorf("%w: log domains %q and %q are not comparable", ErrTemporalValidity, domain, c.Boundary.Log.Position.Domain)
			}
		}
	}
	return domain, nil
}

func normalizeSide(constraints []AuthenticatedTemporalConstraint, established bool) (*AuthenticatedTemporalBoundary, error) {
	logBound := pickLogBound(constraints, established)
	timeBound := pickTimeBound(constraints, established)
	if logBound == nil && timeBound == nil {
		return nil, nil
	}
	return &AuthenticatedTemporalBoundary{Log: logBound, Time: timeBound}, nil
}

func pickLogBound(constraints []AuthenticatedTemporalConstraint, established bool) *AuthenticatedLogBoundary {
	var best *AuthenticatedLogBoundary
	for i := range constraints {
		log := constraints[i].Boundary.Log
		if log == nil {
			continue
		}
		if best == nil {
			best = log
			continue
		}
		if established {
			if log.Position.Index > best.Position.Index || (log.Position.Index == best.Position.Index && !log.Inclusive && best.Inclusive) {
				best = log
			}
			continue
		}
		if log.Position.Index < best.Position.Index || (log.Position.Index == best.Position.Index && !log.Inclusive && best.Inclusive) {
			best = log
		}
	}
	return best
}

func pickTimeBound(constraints []AuthenticatedTemporalConstraint, established bool) *AuthenticatedTimeBoundary {
	var best *AuthenticatedTimeBoundary
	for i := range constraints {
		tb := constraints[i].Boundary.Time
		if tb == nil {
			continue
		}
		if best == nil {
			best = tb
			continue
		}
		if established {
			if tb.Latest.After(best.Latest) || (tb.Latest.Equal(best.Latest) && !tb.Inclusive && best.Inclusive) {
				best = tb
			}
			continue
		}
		if tb.Earliest.Before(best.Earliest) || (tb.Earliest.Equal(best.Earliest) && !tb.Inclusive && best.Inclusive) {
			best = tb
		}
	}
	return best
}

func cloneTimeBoundary(in AuthenticatedTimeBoundary) AuthenticatedTimeBoundary {
	out := in
	if in.ObservedBy != nil {
		id := *in.ObservedBy
		out.ObservedBy = &id
	}
	return out
}

func checkWindowSatisfiable(established, retired *AuthenticatedTemporalBoundary) error {
	if established == nil || retired == nil {
		return nil
	}
	if established.Log != nil && retired.Log != nil && !logWindowSatisfiable(*established.Log, *retired.Log) {
		return fmt.Errorf("%w: unsatisfiable log window", ErrTemporalValidity)
	}
	if established.Time != nil && retired.Time != nil && !timeWindowSatisfiable(*established.Time, *retired.Time) {
		return fmt.Errorf("%w: unsatisfiable time window", ErrTemporalValidity)
	}
	return nil
}

func logWindowSatisfiable(est, ret AuthenticatedLogBoundary) bool {
	min := est.Position.Index
	if !est.Inclusive {
		if est.Position.Index == ^uint64(0) {
			return false
		}
		min++
	}
	max := ret.Position.Index
	if !ret.Inclusive {
		if ret.Position.Index == 0 {
			return false
		}
		max--
	}
	return min <= max
}

func timeWindowSatisfiable(est, ret AuthenticatedTimeBoundary) bool {
	min := est.Latest
	if !est.Inclusive {
		next := est.Latest.Add(time.Nanosecond)
		if !next.After(est.Latest) {
			return false
		}
		min = next
	}
	max := ret.Earliest
	if !ret.Inclusive {
		prev := ret.Earliest.Add(-time.Nanosecond)
		if !prev.Before(ret.Earliest) {
			return false
		}
		max = prev
	}
	return !min.After(max)
}

func unionBasis(established, retired []AuthenticatedTemporalConstraint) []Digest {
	seen := make(map[Digest]struct{})
	var out []Digest
	for _, group := range [][]AuthenticatedTemporalConstraint{established, retired} {
		for _, c := range group {
			for _, d := range c.Basis {
				if d == "" {
					continue
				}
				if _, ok := seen[d]; ok {
					continue
				}
				seen[d] = struct{}{}
				out = append(out, d)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// CheckTemporalValidity compares digest-free subject facts to a normalized
// window. Log compares only to log, time only to time. A present coordinate
// with no comparable subject fact fails. An omitted side is unbounded.
// Subject times must already have been admitted by the active trusted-time
// source policy and endorsed as applicable to this subject by provenance
// verification. This helper compares coordinates only: ObservedBy describes
// the source of a boundary and does not constrain subject observations. The
// initial evaluator admits one tenant evidence-log domain.
func CheckTemporalValidity(subject VerifiedSubjectTemporalInfo, window AuthenticatedValidityWindow) error {
	if err := checkLogValidity(subject, window); err != nil {
		return err
	}
	return checkTimeValidity(subject, window)
}

func checkLogValidity(subject VerifiedSubjectTemporalInfo, window AuthenticatedValidityWindow) error {
	est := windowLog(window.EstablishedBy)
	ret := windowLog(window.RetiredBy)
	if est == nil && ret == nil {
		return nil
	}
	if err := requireTenantLog(est, ret, subject.LogPosition); err != nil {
		return err
	}
	if subject.LogPosition == nil {
		return fmt.Errorf("%w: window has a log coordinate but the subject has no log position", ErrTemporalValidity)
	}
	if est != nil && !logAfter(*subject.LogPosition, *est) {
		return fmt.Errorf("%w: subject log index %d is not after established index %d", ErrTemporalValidity, subject.LogPosition.Index, est.Position.Index)
	}
	if ret != nil && !logBefore(*subject.LogPosition, *ret) {
		return fmt.Errorf("%w: subject log index %d is not before retired index %d", ErrTemporalValidity, subject.LogPosition.Index, ret.Position.Index)
	}
	return nil
}

func checkTimeValidity(subject VerifiedSubjectTemporalInfo, window AuthenticatedValidityWindow) error {
	est := windowTime(window.EstablishedBy)
	ret := windowTime(window.RetiredBy)
	if est == nil && ret == nil {
		return nil
	}
	var comparable []VerifiedSubjectTime
	for _, obs := range subject.Times {
		if obs.Earliest.After(obs.Latest) {
			return fmt.Errorf("%w: subject time earliest is after latest", ErrTemporalValidity)
		}
		comparable = append(comparable, obs)
	}
	if len(comparable) == 0 {
		return fmt.Errorf("%w: window has a time coordinate but the subject has no time observation", ErrTemporalValidity)
	}
	for _, obs := range comparable {
		if est != nil && !timeAfter(obs, *est) {
			continue
		}
		if ret != nil && !timeBefore(obs, *ret) {
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: no subject time observation satisfies the validity window", ErrTemporalValidity)
}

func requireTenantLog(est, ret *AuthenticatedLogBoundary, subject *LogPosition) error {
	for _, bound := range []*AuthenticatedLogBoundary{est, ret} {
		if bound == nil {
			continue
		}
		if bound.Position.Domain != LogDomainTenantEvidenceV1 {
			return fmt.Errorf("%w: window log domain %q is not %s", ErrTemporalValidity, bound.Position.Domain, LogDomainTenantEvidenceV1)
		}
	}
	if subject != nil && subject.Domain != LogDomainTenantEvidenceV1 {
		return fmt.Errorf("%w: subject log domain %q is not %s", ErrTemporalValidity, subject.Domain, LogDomainTenantEvidenceV1)
	}
	return nil
}

func windowLog(side *AuthenticatedTemporalBoundary) *AuthenticatedLogBoundary {
	if side == nil {
		return nil
	}
	return side.Log
}

func windowTime(side *AuthenticatedTemporalBoundary) *AuthenticatedTimeBoundary {
	if side == nil {
		return nil
	}
	return side.Time
}

func logAfter(subject LogPosition, bound AuthenticatedLogBoundary) bool {
	if subject.Domain != bound.Position.Domain {
		return false
	}
	if bound.Inclusive {
		return subject.Index >= bound.Position.Index
	}
	return subject.Index > bound.Position.Index
}

func logBefore(subject LogPosition, bound AuthenticatedLogBoundary) bool {
	if subject.Domain != bound.Position.Domain {
		return false
	}
	if bound.Inclusive {
		return subject.Index <= bound.Position.Index
	}
	return subject.Index < bound.Position.Index
}

func timeAfter(obs VerifiedSubjectTime, bound AuthenticatedTimeBoundary) bool {
	if bound.Inclusive {
		return !obs.Earliest.Before(bound.Latest)
	}
	return obs.Earliest.After(bound.Latest)
}

func timeBefore(obs VerifiedSubjectTime, bound AuthenticatedTimeBoundary) bool {
	if bound.Inclusive {
		return !obs.Latest.After(bound.Earliest)
	}
	return obs.Latest.Before(bound.Earliest)
}
