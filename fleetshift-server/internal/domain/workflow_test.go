package domain_test

import (
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestContinueAsNewError(t *testing.T) {
	t.Run("is detectable via errors.As", func(t *testing.T) {
		err := domain.ContinueAsNew("some-input")
		var target *domain.ContinueAsNewError
		if !errors.As(err, &target) {
			t.Fatal("expected errors.As to find ContinueAsNewError")
		}
		if target.Input != "some-input" {
			t.Fatalf("got input %v, want %q", target.Input, "some-input")
		}
	})

	t.Run("has a descriptive message", func(t *testing.T) {
		err := domain.ContinueAsNew("dep-123")
		if err.Error() != "continue as new" {
			t.Fatalf("got %q, want %q", err.Error(), "continue as new")
		}
	})
}
