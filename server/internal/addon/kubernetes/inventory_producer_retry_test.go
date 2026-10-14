package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestIsPermanentEnsureError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "transient", err: errors.New("dial timeout"), want: false},
		{name: "invalid argument", err: domain.ErrInvalidArgument, want: true},
		{name: "wrapped invalid argument", err: fmt.Errorf("x: %w", domain.ErrInvalidArgument), want: true},
		{name: "allow-list empty", err: ErrIndexerAllowListEmpty, want: true},
		{name: "stale generation", err: ErrStaleIndexerGeneration, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanentEnsureError(tt.err); got != tt.want {
				t.Fatalf("IsPermanentEnsureError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRetryLocalEnvelope_SuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	err := RetryLocalEnvelope(context.Background(), time.Second, func(context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("RetryLocalEnvelope: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRetryLocalEnvelope_PermanentErrorFailsFast(t *testing.T) {
	calls := 0
	err := RetryLocalEnvelope(context.Background(), time.Minute, func(context.Context) error {
		calls++
		return ErrStaleIndexerGeneration
	})
	if !errors.Is(err, ErrStaleIndexerGeneration) {
		t.Fatalf("err = %v, want ErrStaleIndexerGeneration", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (no retry on permanent)", calls)
	}
}

func TestRetryLocalEnvelope_DeadlineReturnsLastError(t *testing.T) {
	transient := errors.New("temporary")
	calls := 0
	err := RetryLocalEnvelope(context.Background(), 20*time.Millisecond, func(context.Context) error {
		calls++
		return transient
	})
	if !errors.Is(err, transient) {
		t.Fatalf("err = %v, want last transient error", err)
	}
	if calls < 1 {
		t.Fatal("expected at least one attempt")
	}
}

func TestRetryLocalEnvelope_CancelledContextReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := RetryLocalEnvelope(ctx, time.Second, func(attemptCtx context.Context) error {
		if attemptCtx.Err() == nil {
			t.Fatal("expected attempt context to be cancelled")
		}
		return errors.New("transient")
	})
	if err == nil {
		t.Fatal("expected error when parent context is cancelled")
	}
}

func TestRetryLocalEnvelope_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := RetryLocalEnvelope(context.Background(), time.Second, func(context.Context) error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetryLocalEnvelope: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestFullJitter(t *testing.T) {
	if got := fullJitter(0); got != 0 {
		t.Fatalf("fullJitter(0) = %v, want 0", got)
	}
	if got := fullJitter(-1); got != 0 {
		t.Fatalf("fullJitter(-1) = %v, want 0", got)
	}
	got := fullJitter(10 * time.Millisecond)
	if got < 0 || got > 10*time.Millisecond {
		t.Fatalf("fullJitter(10ms) = %v, want in [0,10ms]", got)
	}
}
