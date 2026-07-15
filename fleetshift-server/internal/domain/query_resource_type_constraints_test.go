package domain_test

import (
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestResourceTypeConstraints(t *testing.T) {
	constrained, types, err := domain.ResourceTypeConstraints("")
	if err != nil || constrained || len(types) != 0 {
		t.Fatalf("empty: constrained=%v types=%v err=%v", constrained, types, err)
	}

	constrained, types, err = domain.ResourceTypeConstraints(`name == "x"`)
	if err != nil || constrained || len(types) != 0 {
		t.Fatalf("no type: constrained=%v types=%v err=%v", constrained, types, err)
	}

	constrained, types, err = domain.ResourceTypeConstraints(`resourceType == "a/T"`)
	if err != nil || !constrained || len(types) != 1 || types[0] != "a/T" {
		t.Fatalf("eq: constrained=%v types=%v err=%v", constrained, types, err)
	}

	constrained, types, err = domain.ResourceTypeConstraints(`resourceType in ["a/T", "b/U"]`)
	if err != nil || !constrained || len(types) != 2 {
		t.Fatalf("in: constrained=%v types=%v err=%v", constrained, types, err)
	}

	constrained, types, err = domain.ResourceTypeConstraints(`resourceType == "a/T" && name == "x"`)
	if err != nil || !constrained || len(types) != 1 || types[0] != "a/T" {
		t.Fatalf("and: constrained=%v types=%v err=%v", constrained, types, err)
	}

	constrained, types, err = domain.ResourceTypeConstraints(`(resourceType == "a/T") || name == "x"`)
	if err != nil || constrained || len(types) != 0 {
		t.Fatalf("or: constrained=%v types=%v err=%v (guard under || must not count)", constrained, types, err)
	}

	constrained, types, err = domain.ResourceTypeConstraints(`resourceType in []`)
	if err != nil || !constrained || len(types) != 0 {
		t.Fatalf("empty in: constrained=%v types=%v err=%v", constrained, types, err)
	}

	_, _, err = domain.ResourceTypeConstraints(`name ==`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("invalid CEL: err = %v, want ErrInvalidArgument", err)
	}
}
