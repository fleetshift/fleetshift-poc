package application

import (
	"context"
	"sync"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// QuerySchemaCatalog is an in-memory, thread-safe
// [domain.QuerySchemaProvider]. It is the plan's "in-memory schema
// catalog that records active schemas at activation time" -- see the
// QueryRepository POC plan's "Schema Provider" section.
//
// Per this repo's testing conventions (prefer a real, no-I/O
// implementation over a mock), this same type also serves as its own
// test double: tests construct one directly and call Register instead
// of hand-rolling a separate stub provider.
//
// It is intentionally independent of dynamicapi.DynamicFileRegistry:
// that registry serves gRPC reflection over compiled *file*
// descriptors, while this catalog serves query-time field validation
// over compiled *message* descriptors, keyed by [domain.ResourceType]
// rather than gRPC service name.
type QuerySchemaCatalog struct {
	mu      sync.RWMutex
	schemas map[domain.ResourceType]domain.ResourceQuerySchema
}

var _ domain.QuerySchemaProvider = (*QuerySchemaCatalog)(nil)

// NewQuerySchemaCatalog returns an empty catalog.
func NewQuerySchemaCatalog() *QuerySchemaCatalog {
	return &QuerySchemaCatalog{schemas: make(map[domain.ResourceType]domain.ResourceQuerySchema)}
}

// Register records (or replaces) schema under schema.ResourceType.
// Intended to be called at schema activation time -- see
// managedresource.DynamicSchemaActivator's wiring in cli/serve.go --
// and directly by tests.
func (c *QuerySchemaCatalog) Register(schema domain.ResourceQuerySchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[schema.ResourceType] = schema
}

// Unregister removes rt's schema, if any. Intended to be called when
// a schema is deactivated (see DynamicSchemaActivator.Deactivate).
func (c *QuerySchemaCatalog) Unregister(rt domain.ResourceType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, rt)
}

// GetResourceQuerySchema implements [domain.QuerySchemaProvider].
func (c *QuerySchemaCatalog) GetResourceQuerySchema(_ context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.schemas[rt]
	return s, ok, nil
}

// ListResourceQuerySchemas implements [domain.QuerySchemaProvider].
func (c *QuerySchemaCatalog) ListResourceQuerySchemas(_ context.Context) ([]domain.ResourceQuerySchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]domain.ResourceQuerySchema, 0, len(c.schemas))
	for _, s := range c.schemas {
		out = append(out, s)
	}
	return out, nil
}
