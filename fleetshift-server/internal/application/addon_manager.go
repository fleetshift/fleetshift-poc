package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SchemaActivationID is an opaque token returned by
// [SchemaActivator.Activate] that identifies an active transport
// registration. The application layer stores it and passes it back to
// Deactivate; it must not interpret or parse the value.
type SchemaActivationID string

// SchemaActivator compiles and registers the transport-layer API
// surface for an extension resource schema's management section. The
// application layer calls this without knowing about proto compilation,
// gRPC service descriptors, or HTTP muxes — the implementation lives
// in the transport layer.
type SchemaActivator interface {
	Activate(ctx context.Context, schema domain.ExtensionResourceSchema) (SchemaActivationID, error)
	Deactivate(id SchemaActivationID)
}

// DeliveryAgentRegistry manages the mapping from [domain.TargetType] to
// [domain.DeliveryAgent]. The addon manager uses this to register and
// deregister agents during addon connect/disconnect without coupling to
// the concrete routing implementation.
type DeliveryAgentRegistry interface {
	Register(targetType domain.TargetType, agent domain.DeliveryAgent)
	Deregister(targetType domain.TargetType)
}

// AddonManagerDeps holds the injected dependencies for [AddonManager].
type AddonManagerDeps struct {
	Router    DeliveryAgentRegistry
	TypeSvc   *ExtensionResourceTypeService
	Activator SchemaActivator
}

// AddonManager orchestrates the addon lifecycle: enable, connect,
// disconnect, disable. It holds in-memory addon state and coordinates
// schema activation (via [SchemaActivator]), delivery agent routing,
// and managed resource type definitions.
type AddonManager struct {
	mu     sync.RWMutex
	addons map[domain.AddonID]*addonRecord
	now    func() time.Time

	router    DeliveryAgentRegistry
	typeSvc   *ExtensionResourceTypeService
	activator SchemaActivator
}

// AddonManagerOption configures an [AddonManager].
type AddonManagerOption func(*AddonManager)

// WithAddonManagerClock overrides the wall-clock used for addon
// lifecycle timestamps (e.g. EnabledAt, ConnectedAt). Defaults to
// [time.Now].
func WithAddonManagerClock(fn func() time.Time) AddonManagerOption {
	return func(m *AddonManager) { m.now = fn }
}

// addonRecord is the in-memory state for an addon within the manager.
type addonRecord struct {
	addon domain.Addon
	agent domain.DeliveryAgent
	// Keyed by resource type so connectSchemas can reconcile the new
	// input against existing state and tear down stale schemas.
	// Content-change detection is handled by the SchemaActivator itself.
	registeredSchemas map[domain.ResourceType]registeredSchema
}

// registeredSchema tracks a schema that has been registered (type def
// created) and optionally activated in the transport layer.
type registeredSchema struct {
	// activation is non-nil when the schema has a live transport
	// registration (managed schemas). Nil for inventory-only schemas.
	activation *SchemaActivationID
}

// NewAddonManager creates a new manager with the given dependencies
// and options.
func NewAddonManager(deps AddonManagerDeps, opts ...AddonManagerOption) *AddonManager {
	m := &AddonManager{
		addons:    make(map[domain.AddonID]*addonRecord),
		now:       time.Now,
		router:    deps.Router,
		typeSvc:   deps.TypeSvc,
		activator: deps.Activator,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Enable authorizes and records an addon's declared capabilities.
// The addon transitions to [domain.AddonStateEnabled]. No schemas are
// compiled and no gRPC surface is created — that happens at Connect.
//
// If the addon was previously disabled (state [domain.AddonStateDefined]),
// Enable re-enables it by updating the record in place.
func (m *AddonManager) Enable(_ context.Context, desc domain.AddonDescriptor) error {
	for _, cap := range desc.Capabilities {
		if rt, ok := capabilityResourceType(cap); ok {
			if err := validateResourceTypeOwnership(desc.ID, rt); err != nil {
				return err
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if rec, exists := m.addons[desc.ID]; exists {
		if rec.addon.State != domain.AddonStateDefined {
			return fmt.Errorf("%w: addon %q is already enabled", domain.ErrAlreadyExists, desc.ID)
		}
		rec.addon.Name = desc.Name
		rec.addon.State = domain.AddonStateEnabled
		rec.addon.Capabilities = desc.Capabilities
		rec.addon.EnabledAt = m.now()
		return nil
	}

	now := m.now()
	m.addons[desc.ID] = &addonRecord{
		addon: domain.Addon{
			ID:           desc.ID,
			Name:         desc.Name,
			State:        domain.AddonStateEnabled,
			Capabilities: desc.Capabilities,
			EnabledAt:    now,
		},
	}
	return nil
}

// ConnectInput carries the runtime assets an addon provides at connect
// time. Each capability type contributes its own field; absent fields
// are simply not processed. This keeps the [AddonManager.Connect]
// signature stable as new capability types are introduced.
type ConnectInput struct {
	// Agent is the delivery agent for addons that declare a
	// [domain.DeliveryCapability]. Nil for managed-resource-only addons.
	Agent domain.DeliveryAgent

	// Targets are the delivery targets this addon serves. Registered
	// atomically with the agent so the routing table and target store
	// are consistent. Existing targets are silently skipped.
	Targets []domain.TargetInfo

	// Schemas are the extension resource schemas for addons that declare
	// a [domain.ManagedResourceCapability] and/or
	// [domain.InventoryResourceCapability]. Nil for delivery-only addons.
	Schemas []domain.ExtensionResourceSchema
}

// Connect activates an addon's runtime capabilities. The [ConnectInput]
// represents the addon's current truth — schemas, agents, and targets
// it now provides. On reconnection (after a previous disconnect),
// Connect reconciles: schemas that were active from the previous
// connection but are absent from the new input are deactivated, and
// schemas that are unchanged are left in place.
//
// The addon must be in [domain.AddonStateEnabled] (or re-connecting
// after a disconnect). Each schema section is validated against the
// addon's declared capabilities: Management requires a
// [domain.ManagedResourceCapability], Inventory requires a
// [domain.InventoryResourceCapability].
func (m *AddonManager) Connect(ctx context.Context, addonID domain.AddonID, in ConnectInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.addons[addonID]
	if !ok {
		return fmt.Errorf("%w: addon %q not found (not enabled)", domain.ErrNotFound, addonID)
	}
	if rec.addon.State != domain.AddonStateEnabled {
		return fmt.Errorf("%w: addon %q is in state %d, expected enabled", domain.ErrInvalidArgument, addonID, rec.addon.State)
	}

	// TODO: Connect is not transactional — partial failures leave
	// inconsistent state (e.g. schemas activated but agent not
	// registered). Add compensation/rollback so a failed step
	// undoes earlier side effects.
	if err := m.connectSchemas(ctx, rec, in.Schemas); err != nil {
		return err
	}

	if err := m.connectDeliveryAgent(rec, in.Agent); err != nil {
		return err
	}

	if err := m.connectTargets(ctx, rec, in.Targets); err != nil {
		return err
	}

	now := m.now()
	rec.addon.State = domain.AddonStateConnected
	rec.addon.ConnectedAt = &now
	return nil
}

// connectSchemas reconciles the addon's registered schemas against the
// new input:
//  1. Tears down schemas that are no longer provided (stale).
//  2. Validates each schema section against the addon's declared
//     capabilities.
//  3. Registers the schema (creates the type definition).
//  4. For schemas with a Management section, calls
//     [SchemaActivator.Activate] to compile the transport API surface.
//     Inventory-only schemas skip activation (no dynamic API yet).
func (m *AddonManager) connectSchemas(ctx context.Context, rec *addonRecord, schemas []domain.ExtensionResourceSchema) error {
	newTypes := make(map[domain.ResourceType]struct{}, len(schemas))
	for _, s := range schemas {
		newTypes[s.ResourceType] = struct{}{}
	}

	for rt := range rec.registeredSchemas {
		if _, stillPresent := newTypes[rt]; !stillPresent {
			m.teardownSchema(ctx, rec, rt)
		}
	}

	for _, schema := range schemas {
		if err := validateResourceTypeOwnership(rec.addon.ID, schema.ResourceType); err != nil {
			return err
		}
		if err := validateSchemaCapabilities(rec, schema); err != nil {
			return err
		}
		if err := m.registerSchema(ctx, rec, schema); err != nil {
			return fmt.Errorf("register schema for %q: %w", schema.ResourceType, err)
		}
		// TODO: no inventory schema yet
		if schema.Management != nil {
			if err := m.activateSchema(ctx, rec, schema); err != nil {
				return fmt.Errorf("activate schema for %q: %w", schema.ResourceType, err)
			}
		}
	}
	return nil
}

// teardownSchema deactivates the transport surface (if active) and
// deletes the type definition from the store.
func (m *AddonManager) teardownSchema(ctx context.Context, rec *addonRecord, rt domain.ResourceType) {
	if reg, ok := rec.registeredSchemas[rt]; ok {
		if reg.activation != nil {
			m.activator.Deactivate(*reg.activation)
		}
		_ = m.typeSvc.Delete(ctx, rt)
		delete(rec.registeredSchemas, rt)
	}
}

func (m *AddonManager) connectDeliveryAgent(rec *addonRecord, agent domain.DeliveryAgent) error {
	if agent == nil {
		return nil
	}
	for _, cap := range rec.addon.Capabilities {
		if dc, ok := cap.(domain.DeliveryCapability); ok {
			m.router.Register(dc.TargetType, agent)
			rec.agent = agent
		}
	}
	return nil
}

func (m *AddonManager) connectTargets(ctx context.Context, rec *addonRecord, targets []domain.TargetInfo) error {
	targetSvc := &TargetService{Store: m.typeSvc.Store()}
	for _, t := range targets {
		if err := targetSvc.Register(ctx, t); err != nil {
			if errors.Is(err, domain.ErrAlreadyExists) {
				continue
			}
			return fmt.Errorf("register target %q: %w", t.ID(), err)
		}
	}
	return nil
}

// Disconnect deactivates an addon's runtime capabilities. The delivery
// agent is deregistered, but the API surface remains live so users can
// still CRUD managed resources. The addon transitions back to
// [domain.AddonStateEnabled].
func (m *AddonManager) Disconnect(_ context.Context, addonID domain.AddonID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.addons[addonID]
	if !ok {
		return fmt.Errorf("%w: addon %q not found", domain.ErrNotFound, addonID)
	}

	if rec.agent != nil {
		for _, cap := range rec.addon.Capabilities {
			if dc, ok := cap.(domain.DeliveryCapability); ok {
				m.router.Deregister(dc.TargetType)
			}
		}
		rec.agent = nil
	}

	rec.addon.State = domain.AddonStateEnabled
	rec.addon.ConnectedAt = nil
	return nil
}

// Disable fully removes an addon's API surface and type definitions.
// Schema activations are torn down, delivery agents are removed, and
// extension resource type defs are deleted. The addon transitions to
// [domain.AddonStateDefined].
func (m *AddonManager) Disable(ctx context.Context, addonID domain.AddonID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.addons[addonID]
	if !ok {
		return fmt.Errorf("%w: addon %q not found", domain.ErrNotFound, addonID)
	}

	if rec.agent != nil {
		for _, cap := range rec.addon.Capabilities {
			if dc, ok := cap.(domain.DeliveryCapability); ok {
				m.router.Deregister(dc.TargetType)
			}
		}
		rec.agent = nil
	}

	// Tear down all registered schemas (both activated and
	// inventory-only).
	for rt := range rec.registeredSchemas {
		m.teardownSchema(ctx, rec, rt)
	}

	rec.addon.State = domain.AddonStateDefined
	rec.addon.ConnectedAt = nil
	return nil
}

// Get returns the current state of an addon.
func (m *AddonManager) Get(addonID domain.AddonID) (domain.Addon, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.addons[addonID]
	if !ok {
		return domain.Addon{}, fmt.Errorf("%w: addon %q not found", domain.ErrNotFound, addonID)
	}
	return rec.addon, nil
}

// validateResourceTypeOwnership checks that the resource type's
// service name matches the addon's ID. This enforces that an addon
// can only register extension resource types under its own service
// namespace.
func validateResourceTypeOwnership(addonID domain.AddonID, rt domain.ResourceType) error {
	expected := domain.ServiceName(addonID)
	actual := rt.ServiceName()
	if expected != actual {
		return fmt.Errorf(
			"%w: addon %q cannot register resource type %q (service name %q does not match addon ID)",
			domain.ErrInvalidArgument, addonID, rt, actual,
		)
	}
	return nil
}

// capabilityResourceType extracts the [domain.ResourceType] from a
// capability, if the capability type carries one. Returns false for
// capability types like [domain.DeliveryCapability] that don't own
// extension resource types.
func capabilityResourceType(cap domain.Capability) (domain.ResourceType, bool) {
	switch c := cap.(type) {
	case domain.ManagedResourceCapability:
		return c.ResourceType, true
	case domain.InventoryResourceCapability:
		return c.ResourceType, true
	default:
		return "", false
	}
}

// validateSchemaCapabilities checks that each non-nil section of the
// schema is backed by a matching capability declaration on the addon.
func validateSchemaCapabilities(rec *addonRecord, schema domain.ExtensionResourceSchema) error {
	if schema.Management != nil {
		if !hasCapabilityFor[domain.ManagedResourceCapability](rec, schema.ResourceType) {
			return fmt.Errorf("%w: addon %q has no ManagedResourceCapability for resource type %q",
				domain.ErrInvalidArgument, rec.addon.ID, schema.ResourceType)
		}
	}
	if schema.Inventory != nil {
		if !hasCapabilityFor[domain.InventoryResourceCapability](rec, schema.ResourceType) {
			return fmt.Errorf("%w: addon %q has no InventoryResourceCapability for resource type %q",
				domain.ErrInvalidArgument, rec.addon.ID, schema.ResourceType)
		}
	}
	return nil
}

// resourceCapability is the constraint for capability types that carry
// a ResourceType field.
type resourceCapability interface {
	domain.ManagedResourceCapability | domain.InventoryResourceCapability
}

// hasCapabilityFor returns true if the addon has a capability of type C
// matching the given resource type.
func hasCapabilityFor[C resourceCapability](rec *addonRecord, rt domain.ResourceType) bool {
	for _, cap := range rec.addon.Capabilities {
		if c, ok := cap.(C); ok {
			switch v := any(c).(type) {
			case domain.ManagedResourceCapability:
				if v.ResourceType == rt {
					return true
				}
			case domain.InventoryResourceCapability:
				if v.ResourceType == rt {
					return true
				}
			}
		}
	}
	return false
}

// registerSchema ensures the extension resource type definition exists
// in the store. It is called for every schema (managed, inventory, or
// both) before any transport activation.
func (m *AddonManager) registerSchema(ctx context.Context, rec *addonRecord, schema domain.ExtensionResourceSchema) error {
	if _, ok := rec.registeredSchemas[schema.ResourceType]; ok {
		return nil
	}

	newVer := domain.APIVersion(schema.Version)
	newCol := domain.CollectionID(schema.CollectionID)

	input := CreateExtensionTypeInput{
		ResourceType: schema.ResourceType,
		APIVersion:   newVer,
		CollectionID: newCol,
	}
	if schema.Management != nil {
		input.Management = &CreateExtensionTypeManagementInput{
			Relation: schema.Management.Relation,
			// TODO: support relation signatures and validation through attestation evidence
			Signature: domain.Signature{},
		}
	}
	if schema.Inventory != nil {
		// TODO: schema support for inventory
		input.Inventory = &CreateExtensionTypeInventoryInput{}
	}

	_, err := m.typeSvc.Create(ctx, input)
	if err != nil {
		if !errors.Is(err, domain.ErrAlreadyExists) {
			return fmt.Errorf("create type def: %w", err)
		}
		if err := m.detectAPIMetadataDrift(ctx, schema.ResourceType, newVer, newCol); err != nil {
			return err
		}
	}
	if rec.registeredSchemas == nil {
		rec.registeredSchemas = make(map[domain.ResourceType]registeredSchema)
	}
	rec.registeredSchemas[schema.ResourceType] = registeredSchema{}
	return nil
}

// activateSchema delegates to the SchemaActivator and records the
// resulting activation ID. Only called for schemas with a Management
// section — the schema has already been registered by [registerSchema].
func (m *AddonManager) activateSchema(ctx context.Context, rec *addonRecord, schema domain.ExtensionResourceSchema) error {
	id, err := m.activator.Activate(ctx, schema)
	if err != nil {
		return err
	}

	// If the activation ID changed (e.g. the gRPC service name
	// changed due to a package rename), deactivate the old one so
	// its gRPC/HTTP routes don't leak.
	if reg, ok := rec.registeredSchemas[schema.ResourceType]; ok && reg.activation != nil && *reg.activation != id {
		m.activator.Deactivate(*reg.activation)
	}
	rec.registeredSchemas[schema.ResourceType] = registeredSchema{activation: &id}

	return nil
}

// detectAPIMetadataDrift loads the existing type def and rejects
// reconnection attempts that change the API identity fields. The
// service name is not checked because it is derived from the
// [domain.ResourceType], which is already the lookup key.
func (m *AddonManager) detectAPIMetadataDrift(ctx context.Context, rt domain.ResourceType, newVer domain.APIVersion, newCol domain.CollectionID) error {
	existing, err := m.typeSvc.Get(ctx, rt)
	if err != nil {
		return fmt.Errorf("load existing type def for drift detection: %w", err)
	}
	if existing.APIVersion() != newVer {
		return fmt.Errorf("%w: API version drift: existing %q, new %q", domain.ErrInvalidArgument, existing.APIVersion(), newVer)
	}
	if existing.CollectionID() != newCol {
		return fmt.Errorf("%w: collection ID drift: existing %q, new %q", domain.ErrInvalidArgument, existing.CollectionID(), newCol)
	}
	return nil
}
