package kubernetes

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	// LocalEnsureRetryDeadline bounds producer EnsureIndexer retry attempts.
	LocalEnsureRetryDeadline = time.Minute
	// ReportResultRetryDeadline bounds short retries of terminal ReportResult.
	ReportResultRetryDeadline = 30 * time.Second

	// localEnsureBackoffStart is the initial RetryLocalEnvelope backoff.
	localEnsureBackoffStart = time.Second
	// localEnsureBackoffCap caps RetryLocalEnvelope backoff growth.
	localEnsureBackoffCap = 15 * time.Second
	// localEnsureBackoffFactor multiplies backoff after each failed attempt.
	localEnsureBackoffFactor = 2.0
)

// DefaultIndexConfig returns the IndexConfig used for producer and
// startup-replay EnsureIndexer calls.
func DefaultIndexConfig() IndexConfig {
	return IndexConfig{Schema: DefaultKubernetesSchema()}
}

// IsPermanentEnsureError reports whether err should fail immediately
// instead of retrying under [RetryLocalEnvelope].
func IsPermanentEnsureError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, domain.ErrInvalidArgument) {
		return true
	}
	if errors.Is(err, ErrIndexerAllowListEmpty) {
		return true
	}
	if errors.Is(err, ErrStaleIndexerGeneration) {
		return true
	}
	return false
}

// RetryLocalEnvelope runs fn under a wall-clock deadline with exponential
// backoff and full jitter. [IsPermanentEnsureError] results return
// immediately. If the deadline expires after a failed attempt, the last
// error is returned; if the context is cancelled before any attempt
// produces an error, the context error is returned.
func RetryLocalEnvelope(ctx context.Context, deadline time.Duration, fn func(context.Context) error) error {
	if deadline <= 0 {
		deadline = LocalEnsureRetryDeadline
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	backoff := localEnsureBackoffStart
	var lastErr error
	for {
		lastErr = fn(deadlineCtx)
		if lastErr == nil {
			return nil
		}
		if IsPermanentEnsureError(lastErr) {
			return lastErr
		}
		if err := deadlineCtx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		sleep := min(fullJitter(backoff), localEnsureBackoffCap)
		timer := time.NewTimer(sleep)
		select {
		case <-deadlineCtx.Done():
			timer.Stop()
			if lastErr != nil {
				return lastErr
			}
			return deadlineCtx.Err()
		case <-timer.C:
		}
		next := min(time.Duration(float64(backoff)*localEnsureBackoffFactor), localEnsureBackoffCap)
		backoff = next
	}
}

// fullJitter returns a random duration in [0, max]. Non-positive max yields 0.
func fullJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max) + 1))
}
