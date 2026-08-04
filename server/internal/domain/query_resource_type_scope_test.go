package domain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type staticQuerySchemas map[domain.ResourceType]domain.ResourceQuerySchema

func (s staticQuerySchemas) GetResourceQuerySchema(_ context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	schema, ok := s[rt]
	return schema, ok, nil
}

func (s staticQuerySchemas) ListResourceQuerySchemas(_ context.Context) ([]domain.ResourceQuerySchema, error) {
	out := make([]domain.ResourceQuerySchema, 0, len(s))
	for _, schema := range s {
		out = append(out, schema)
	}
	return out, nil
}

func TestResolveQueryResourceTypeScope_NilProvider(t *testing.T) {
	scope, err := domain.ResolveQueryResourceTypeScope(context.Background(), nil, "")
	if err != nil || scope.Empty || scope.Types != nil {
		t.Fatalf("nil provider: scope=%+v err=%v", scope, err)
	}

	scope, err = domain.ResolveQueryResourceTypeScope(context.Background(), nil, `resourceType == "a/T"`)
	if err != nil || scope.Empty || scope.Types != nil {
		t.Fatalf("nil provider with type filter: scope=%+v err=%v", scope, err)
	}
}

func TestResolveQueryResourceTypeScope_EmptyActivated(t *testing.T) {
	scope, err := domain.ResolveQueryResourceTypeScope(context.Background(), staticQuerySchemas{}, "")
	if err != nil || !scope.Empty {
		t.Fatalf("empty provider: scope=%+v err=%v", scope, err)
	}

	_, err = domain.ResolveQueryResourceTypeScope(
		context.Background(), staticQuerySchemas{}, `resourceType == "a/T"`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty activated + named type: err=%v, want ErrInvalidArgument", err)
	}
}

func TestResolveQueryResourceTypeScope_DefaultsToActivated(t *testing.T) {
	provider := staticQuerySchemas{
		"b/U": {ResourceType: "b/U"},
		"a/T": {ResourceType: "a/T"},
	}
	scope, err := domain.ResolveQueryResourceTypeScope(context.Background(), provider, "")
	if err != nil || scope.Empty {
		t.Fatalf("scope=%+v err=%v", scope, err)
	}
	if len(scope.Types) != 2 || scope.Types[0] != "a/T" || scope.Types[1] != "b/U" {
		t.Fatalf("Types = %v, want sorted [a/T b/U]", scope.Types)
	}
}

func TestResolveQueryResourceTypeScope_RejectsInactive(t *testing.T) {
	provider := staticQuerySchemas{"a/T": {ResourceType: "a/T"}}
	_, err := domain.ResolveQueryResourceTypeScope(
		context.Background(), provider, `resourceType == "b/U"`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("err=%v, want ErrInvalidArgument", err)
	}

	_, err = domain.ResolveQueryResourceTypeScope(
		context.Background(), provider, `resourceType in ["a/T", "b/U"]`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("in with inactive: err=%v, want ErrInvalidArgument", err)
	}
}

func TestResolveQueryResourceTypeScope_EmptyInList(t *testing.T) {
	provider := staticQuerySchemas{"a/T": {ResourceType: "a/T"}}
	scope, err := domain.ResolveQueryResourceTypeScope(
		context.Background(), provider, `resourceType in []`)
	if err != nil || !scope.Empty {
		t.Fatalf("scope=%+v err=%v", scope, err)
	}
}

func TestResolveQueryResourceTypeScope_AcceptsActivatedNamed(t *testing.T) {
	provider := staticQuerySchemas{
		"a/T": {ResourceType: "a/T"},
		"b/U": {ResourceType: "b/U"},
	}
	scope, err := domain.ResolveQueryResourceTypeScope(
		context.Background(), provider, `resourceType == "a/T"`)
	if err != nil || scope.Empty {
		t.Fatalf("scope=%+v err=%v", scope, err)
	}
	// Named constraints tighten the IN list to the caller's types so
	// unrelated activations do not invalidate page tokens.
	if len(scope.Types) != 1 || scope.Types[0] != "a/T" {
		t.Fatalf("Types = %v, want [a/T]", scope.Types)
	}

	scope, err = domain.ResolveQueryResourceTypeScope(
		context.Background(), provider, `resourceType in ["b/U", "a/T"]`)
	if err != nil || scope.Empty {
		t.Fatalf("in scope=%+v err=%v", scope, err)
	}
	if len(scope.Types) != 2 || scope.Types[0] != "a/T" || scope.Types[1] != "b/U" {
		t.Fatalf("Types = %v, want sorted named set", scope.Types)
	}
}
