package domain_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestTerminalError(t *testing.T) {
	t.Run("wraps error as terminal", func(t *testing.T) {
		cause := errors.New("something broke")
		err := domain.TerminalError(cause)
		if err == nil {
			t.Fatal("expected non-nil error")
		}
		if !domain.IsTerminal(err) {
			t.Fatal("expected IsTerminal to return true")
		}
	})

	t.Run("unwraps to original cause", func(t *testing.T) {
		cause := errors.New("something broke")
		err := domain.TerminalError(cause)
		if !errors.Is(err, cause) {
			t.Fatal("expected errors.Is to find the original cause")
		}
	})

	t.Run("preserves error message", func(t *testing.T) {
		cause := errors.New("something broke")
		err := domain.TerminalError(cause)
		want := "terminal: something broke"
		if err.Error() != want {
			t.Fatalf("got %q, want %q", err.Error(), want)
		}
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		if err := domain.TerminalError(nil); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("unclassified error is not terminal", func(t *testing.T) {
		err := errors.New("transient blip")
		if domain.IsTerminal(err) {
			t.Fatal("plain error should not be terminal")
		}
	})

	t.Run("nil is not terminal", func(t *testing.T) {
		if domain.IsTerminal(nil) {
			t.Fatal("nil should not be terminal")
		}
	})

	t.Run("detectable through wrapping", func(t *testing.T) {
		cause := errors.New("something broke")
		terminal := domain.TerminalError(cause)
		wrapped := fmt.Errorf("outer context: %w", terminal)
		if !domain.IsTerminal(wrapped) {
			t.Fatal("IsTerminal should see through fmt.Errorf wrapping")
		}
		if !errors.Is(wrapped, cause) {
			t.Fatal("original cause should be reachable through wrapping chain")
		}
	})

	t.Run("detectable after serialization", func(t *testing.T) {
		cause := errors.New("something broke")
		terminal := domain.TerminalError(cause)
		serialized := errors.New(terminal.Error())
		if !domain.IsTerminal(serialized) {
			t.Fatal("IsTerminal should detect terminal marker in serialized error string")
		}
	})

	t.Run("detectable after serialization with wrapping", func(t *testing.T) {
		cause := errors.New("something broke")
		terminal := domain.TerminalError(cause)
		serialized := fmt.Errorf("activity failed: %s", terminal.Error())
		if !domain.IsTerminal(serialized) {
			t.Fatal("IsTerminal should detect terminal marker in wrapped serialized error string")
		}
	})
}
