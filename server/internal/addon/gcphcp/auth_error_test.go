package gcphcp

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsAuthExpiredError_MatchesWrappedError(t *testing.T) {
	base := fmt.Errorf("STS returned status 400: invalid_grant")
	wrapped := newAuthExpiredError(base)

	if !IsAuthExpiredError(wrapped) {
		t.Fatal("expected IsAuthExpiredError to return true")
	}
}

func TestIsAuthExpiredError_RejectsNonAuthError(t *testing.T) {
	base := fmt.Errorf("network timeout")

	if IsAuthExpiredError(base) {
		t.Fatal("expected IsAuthExpiredError to return false for non-auth error")
	}
}

func TestIsAuthExpiredError_RejectsNil(t *testing.T) {
	if IsAuthExpiredError(nil) {
		t.Fatal("expected IsAuthExpiredError to return false for nil")
	}
}

func TestNewAuthExpiredError_PreservesOriginalError(t *testing.T) {
	base := fmt.Errorf("IAM returned status 401: unauthorized")
	wrapped := newAuthExpiredError(base)

	if wrapped.Error() != base.Error() {
		t.Fatalf("error message = %q, want %q", wrapped.Error(), base.Error())
	}
	if !errors.Is(wrapped, base) {
		t.Fatal("expected wrapped error to unwrap to base")
	}
}

func TestNewAuthExpiredError_DoubleWrapIsIdempotent(t *testing.T) {
	base := fmt.Errorf("token expired")
	first := newAuthExpiredError(base)
	second := newAuthExpiredError(first)

	if first != second {
		t.Fatal("expected double-wrap to return the same error")
	}
}

func TestNewAuthExpiredError_NilReturnsNil(t *testing.T) {
	if newAuthExpiredError(nil) != nil {
		t.Fatal("expected nil input to return nil")
	}
}

func TestIsAuthExpiredError_MatchesDeeplyWrapped(t *testing.T) {
	base := fmt.Errorf("unauthorized")
	authErr := newAuthExpiredError(base)
	wrapped := fmt.Errorf("broker auth exchange: %w", authErr)

	if !IsAuthExpiredError(wrapped) {
		t.Fatal("expected IsAuthExpiredError to match through fmt.Errorf wrapping")
	}
}
