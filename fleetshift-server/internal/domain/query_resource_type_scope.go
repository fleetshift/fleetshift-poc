package domain

import (
	"context"
	"fmt"
	"slices"
)

// QueryResourceTypeScope is the activation-derived type constraint
// [QueryRepository] applies for one QueryResources call.
type QueryResourceTypeScope struct {
	// Types, when non-nil, must be ANDed as an IN constraint on
	// extension (service_name, type_name). Nil means no activation
	// constraint (no [QuerySchemaProvider]).
	Types []ResourceType

	// Empty is true when the caller should return an empty page
	// without querying: the provider is present but lists no types,
	// or the filter names an empty resource_type set (e.g.
	// `resource_type in []`). Callers must still compile the filter
	// for validation before honoring Empty.
	Empty bool
}

// ResolveQueryResourceTypeScope derives the type scope for
// QueryResources from provider and filter.
//
// A nil provider is permissive: no activation IN constraint and no
// inactive-type rejection (contract tests and structural-only
// compiles). A non-nil provider scopes results to activated types from
// [QuerySchemaProvider.ListResourceQuerySchemas]. When the filter names
// top-level resource_type == / in constraints, the IN list is those
// named types (sorted) after validating each is activated — not the
// full activated set — so unrelated activations do not invalidate page
// tokens. An empty activated set with no named types yields Empty;
// named types against an empty activated set are [ErrInvalidArgument].
//
// TODO: Consider collapsing this into repositories' existing query
// parsing and type awareness.
func ResolveQueryResourceTypeScope(
	ctx context.Context,
	provider QuerySchemaProvider,
	filter string,
) (QueryResourceTypeScope, error) {
	if provider == nil {
		return QueryResourceTypeScope{}, nil
	}

	schemas, err := provider.ListResourceQuerySchemas(ctx)
	if err != nil {
		return QueryResourceTypeScope{}, fmt.Errorf("list activated resource query schemas: %w", err)
	}
	activatedSet := make(map[ResourceType]struct{}, len(schemas))
	for _, schema := range schemas {
		rt := schema.ResourceType
		if rt == "" {
			continue
		}
		activatedSet[rt] = struct{}{}
	}

	constrained, named, err := ResourceTypeConstraints(filter)
	if err != nil {
		return QueryResourceTypeScope{}, err
	}

	if len(activatedSet) == 0 {
		if constrained {
			if len(named) == 0 {
				return QueryResourceTypeScope{Empty: true}, nil
			}
			return QueryResourceTypeScope{}, fmt.Errorf(
				"%w: resource_type %q is not an activated extension resource type",
				ErrInvalidArgument, named[0])
		}
		return QueryResourceTypeScope{Empty: true}, nil
	}

	if constrained {
		if len(named) == 0 {
			return QueryResourceTypeScope{Empty: true}, nil
		}
		for _, rt := range named {
			if _, ok := activatedSet[rt]; !ok {
				return QueryResourceTypeScope{}, fmt.Errorf(
					"%w: resource_type %q is not an activated extension resource type",
					ErrInvalidArgument, rt)
			}
		}
		slices.Sort(named)
		return QueryResourceTypeScope{Types: named}, nil
	}

	activated := make([]ResourceType, 0, len(activatedSet))
	for rt := range activatedSet {
		activated = append(activated, rt)
	}
	slices.Sort(activated)
	return QueryResourceTypeScope{Types: activated}, nil
}
