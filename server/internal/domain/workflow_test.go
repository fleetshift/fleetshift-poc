package domain_test

import (
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestCreateManagedResourceWorkflowID_UsesFullResourceName(t *testing.T) {
	a := domain.CreateManagedResourceWorkflowID("//test.fleetshift.io/clusters/prod")
	b := domain.CreateManagedResourceWorkflowID("//test.fleetshift.io/databases/prod")

	if a == b {
		t.Fatalf("workflow IDs must differ for different full resource names: %q", a)
	}

	// Same full name should produce the same workflow ID.
	c := domain.CreateManagedResourceWorkflowID("//test.fleetshift.io/clusters/prod")
	if a != c {
		t.Fatalf("same full name should produce same workflow ID: got %q and %q", a, c)
	}
}

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
