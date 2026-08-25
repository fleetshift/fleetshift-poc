// Package temporal verifies append-only evidence-log updates and binds
// reached TypedEvidence identities to verified log positions.
package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

var (
	// ErrMissingUpdate is returned when Prepare is given a nil log update.
	ErrMissingUpdate = errors.New("missing evidence-log update")

	// ErrMissingRootInclusion is returned when Prepare is given a nil
	// root inclusion proof.
	ErrMissingRootInclusion = errors.New("missing root evidence-log inclusion")

	// ErrSecondOccurrence is returned when VerifyOccurrence is asked to bind
	// an already observed evidence identity to a different inclusion.
	ErrSecondOccurrence = errors.New("evidence identity already bound to a different log occurrence")

	// ErrCheckpointStale is returned when a log update was constructed from
	// an older checkpoint than the verifier currently retains.
	ErrCheckpointStale = errors.New("manager used a stale agent checkpoint")
)

// CheckpointStaleError tells a caller that proof construction started from
// an older checkpoint than this verifier currently retains.
type CheckpointStaleError struct {
	checkpoint protocol.Checkpoint
	cause      error
}

func (e *CheckpointStaleError) Error() string {
	return fmt.Sprintf("%v: retained checkpoint size %d: %v", ErrCheckpointStale, e.checkpoint.Size, e.cause)
}

func (e *CheckpointStaleError) Unwrap() error {
	return ErrCheckpointStale
}

// LatestCheckpoint is the retained checkpoint the caller should reconstruct
// proofs from.
func (e *CheckpointStaleError) LatestCheckpoint() protocol.Checkpoint {
	return e.checkpoint
}

// RetainedState is the verifier's accepted append-only evidence-log position.
type RetainedState struct {
	EvidenceLog protocol.Checkpoint
}

// PreparedUpdate is a verified successor prefix together with a log verifier
// bound to that checkpoint. NextState is the candidate retained checkpoint
// to install after Prepare succeeds.
type PreparedUpdate struct {
	Log       protocol.OrderedLogEvidenceVerifier
	NextState RetainedState
}

// Prepare verifies that update extends retained and that rootEvidence is
// included under the successor checkpoint. From must equal the retained
// checkpoint exactly. An older From is stale regardless of whether the
// successor matches, lags, or forks the retained head, including when
// RFC 6962 equal-size consistency against that head would succeed. A newer
// From, same-size different From root, rollback from an exact From, or
// invalid consistency proof is an invalid log update. A missing or
// mismatched root inclusion fails after a valid checkpoint transition and
// returns no candidate state.
func Prepare(
	retained RetainedState,
	update *protocol.EvidenceLogUpdate,
	rootEvidence protocol.TypedEvidence,
	rootInclusion *protocol.EvidenceLogInclusion,
) (PreparedUpdate, error) {
	if update == nil {
		return PreparedUpdate{}, ErrMissingUpdate
	}
	if update.From != retained.EvidenceLog {
		if update.From.Size < retained.EvidenceLog.Size {
			return PreparedUpdate{}, &CheckpointStaleError{
				checkpoint: retained.EvidenceLog,
				cause:      fmt.Errorf("constructed from checkpoint size %d", update.From.Size),
			}
		}
		return PreparedUpdate{}, fmt.Errorf("%w: from checkpoint does not match retained", protocol.ErrInvalidLogUpdate)
	}
	if err := protocol.VerifyEvidenceLogUpdate(retained.EvidenceLog, *update); err != nil {
		return PreparedUpdate{}, err
	}
	if rootInclusion == nil {
		return PreparedUpdate{}, ErrMissingRootInclusion
	}
	log := newIdentityMemoLog(update.Checkpoint)
	if _, err := log.VerifyOccurrence(context.Background(), rootEvidence, *rootInclusion); err != nil {
		return PreparedUpdate{}, err
	}
	return PreparedUpdate{
		Log:       log,
		NextState: RetainedState{EvidenceLog: update.Checkpoint},
	}, nil
}

type memo struct {
	inclusion protocol.EvidenceLogInclusion
	binding   protocol.VerifiedEvidenceLogBinding
}

type identityMemoLog struct {
	checkpoint protocol.Checkpoint
	seen       map[protocol.Digest]memo
}

func newIdentityMemoLog(checkpoint protocol.Checkpoint) *identityMemoLog {
	return &identityMemoLog{
		checkpoint: checkpoint,
		seen:       make(map[protocol.Digest]memo),
	}
}

func (v *identityMemoLog) VerifyOccurrence(_ context.Context, evidence protocol.TypedEvidence, inclusion protocol.EvidenceLogInclusion) (protocol.VerifiedEvidenceLogBinding, error) {
	identity, err := evidence.Identity()
	if err != nil {
		return protocol.VerifiedEvidenceLogBinding{}, fmt.Errorf("%w: evidence identity: %v", protocol.ErrInvalidLogInclusion, err)
	}
	if memo, ok := v.seen[identity]; ok {
		if sameInclusion(memo.inclusion, inclusion) {
			return memo.binding, nil
		}
		return protocol.VerifiedEvidenceLogBinding{}, fmt.Errorf("%w: identity %s", ErrSecondOccurrence, identity)
	}
	if err := protocol.VerifyEvidenceLogInclusion(v.checkpoint, evidence, inclusion); err != nil {
		return protocol.VerifiedEvidenceLogBinding{}, err
	}
	binding := protocol.VerifiedEvidenceLogBinding{
		Position: protocol.LogPosition{
			Domain: protocol.LogDomainTenantEvidenceV1,
			Index:  inclusion.Index,
		},
		Evidence: identity,
	}
	v.seen[identity] = memo{
		inclusion: cloneInclusion(inclusion),
		binding:   binding,
	}
	return binding, nil
}

func sameInclusion(a, b protocol.EvidenceLogInclusion) bool {
	if a.Index != b.Index || len(a.InclusionProof) != len(b.InclusionProof) {
		return false
	}
	for i := range a.InclusionProof {
		if a.InclusionProof[i] != b.InclusionProof[i] {
			return false
		}
	}
	return true
}

func cloneInclusion(in protocol.EvidenceLogInclusion) protocol.EvidenceLogInclusion {
	return protocol.EvidenceLogInclusion{
		Index:          in.Index,
		InclusionProof: append([]protocol.Digest(nil), in.InclusionProof...),
	}
}
