package querysql

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// resolveSchema is the per-compilation schema wiring shared by every
// ResolveContext copy produced during one CompileFilter call. Lazy
// GetResourceQuerySchema results are stored here so Validate*/Classify
// reuse one lookup; a new compilation allocates a new resolveSchema.
type resolveSchema struct {
	provider domain.QuerySchemaProvider

	fetched bool
	schema  domain.ResourceQuerySchema
	ok      bool
	err     error
}

// newResolveContext builds a ResolveContext for schema validation /
// classification unit tests. Bind is intentionally unset: these tests
// never render SQL. CompileFilter always supplies Bind via state.
func newResolveContext(ctx context.Context, guard *domain.ResourceType, schemas domain.QuerySchemaProvider) ResolveContext {
	var rs *resolveSchema
	if schemas != nil {
		rs = &resolveSchema{provider: schemas}
	}
	return ResolveContext{
		Context:             ctx,
		GuardedResourceType: guard,
		schema:              rs,
	}
}

// ValidateSpecPath checks names — the parsed resource.spec.<path>
// segments — against the compiler's schema provider when a top-level
// resourceType == guard is present and a SpecDescriptor is registered
// for that type. Without a guard, provider, registered schema, or
// descriptor, names are returned unchanged for structural JSON
// extraction.
//
// When a descriptor is present, segments are matched against canonical
// JSON field names only (see [ValidateDescriptorPath]).
func (ctx ResolveContext) ValidateSpecPath(names []string) ([]string, error) {
	schema, ok, err := ctx.lookupSchema()
	if err != nil {
		return nil, err
	}
	if !ok || schema.SpecDescriptor == nil {
		return names, nil
	}
	return ValidateDescriptorPath(schema.SpecDescriptor, "resource.spec", names)
}

// ValidateObservationPath is ValidateSpecPath's counterpart for
// resource.observation.<path>, using InventoryObservationDescriptor.
func (ctx ResolveContext) ValidateObservationPath(names []string) ([]string, error) {
	schema, ok, err := ctx.lookupSchema()
	if err != nil {
		return nil, err
	}
	if !ok || schema.InventoryObservationDescriptor == nil {
		return names, nil
	}
	return ValidateDescriptorPath(schema.InventoryObservationDescriptor, "resource.observation", names)
}

// ClassifyResourceContainer classifies resource.<segs> for container
// membership. Without a guard+descriptor the path is accepted
// structurally and Kind stays Unknown for open JSON under
// spec/observation.
func (ctx ResolveContext) ClassifyResourceContainer(segs []string) (ResourceContainerClass, error) {
	return classifyResourceContainer(segs, ctx)
}

func (ctx ResolveContext) lookupSchema() (domain.ResourceQuerySchema, bool, error) {
	if ctx.GuardedResourceType == nil || ctx.schema == nil || ctx.schema.provider == nil {
		return domain.ResourceQuerySchema{}, false, nil
	}
	if ctx.schema.fetched {
		return ctx.schema.schema, ctx.schema.ok, ctx.schema.err
	}
	rt := *ctx.GuardedResourceType
	schema, ok, err := ctx.schema.provider.GetResourceQuerySchema(ctx.Context, rt)
	if err != nil {
		err = fmt.Errorf("filter: look up query schema for %q: %w", rt, err)
	}
	ctx.schema.fetched = true
	ctx.schema.schema = schema
	ctx.schema.ok = ok
	ctx.schema.err = err
	return schema, ok, err
}
