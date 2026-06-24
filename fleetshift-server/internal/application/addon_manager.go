package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SchemaRegistrationID is an opaque token returned by
// [SchemaActivator.Activate] that identifies a schema registration.
// The application layer stores it and passes it back to Deactivate;
// it must not interpret or parse the value.
type SchemaRegistrationID string

// SchemaActivator compiles and registers the transport-layer API
// surface for a managed resource schema. The application layer calls
// this without knowing about proto compilation, gRPC service
// descriptors, or HTTP muxes — the implementation lives in the
// transport layer.
type SchemaActivator interface {
	Activate(ctx context.Context, schema domain.ManagedResourceSchema) (SchemaRegistrationID, error)
	Deactivate(id SchemaRegistrationID)
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
	TypeSvc   *ManagedResourceTypeService
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
	typeSvc   *ManagedResourceTypeService
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
	// input against existing state and deactivate stale schemas.
	// Content-change detection is handled by the SchemaActivator itself.
	schemaRegistrations map[domain.ResourceType]SchemaRegistrationID
	registeredTypeDefs  map[domain.ResourceType]struct{}
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

	// Schemas are the managed resource schemas for addons that declare
	// a [domain.ManagedResourceCapability]. Nil for delivery-only addons.
	Schemas []domain.ManagedResourceSchema
}

// Connect activates an addon's runtime capabilities. The [ConnectInput]
// represents the addon's current truth — schemas, agents, and targets
// it now provides. On reconnection (after a previous disconnect),
// Connect reconciles: schemas that were active from the previous
// connection but are absent from the new input are deactivated, and
// schemas that are unchanged are left in place.
//
// The addon must be in [domain.AddonStateEnabled] (or re-connecting
// after a disconnect). Schemas are validated against the addon's
// declared [domain.ManagedResourceCapability] entries.
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

// connectSchemas reconciles the addon's active schema registrations
// against the new input:
//  1. Deactivates schemas that are no longer provided (stale).
//  2. Calls Activate for every schema in the input — the
//     [SchemaActivator] handles content-change detection internally
//     (via hashing) and atomically swaps or no-ops as appropriate.
func (m *AddonManager) connectSchemas(ctx context.Context, rec *addonRecord, schemas []domain.ManagedResourceSchema) error {
	newTypes := make(map[domain.ResourceType]struct{}, len(schemas))
	for _, s := range schemas {
		newTypes[s.ResourceType] = struct{}{}
	}

	for rt := range rec.schemaRegistrations {
		if _, stillPresent := newTypes[rt]; !stillPresent {
			m.deactivateSchema(ctx, rec, rt)
		}
	}

	for _, schema := range schemas {
		if err := validateSchemaCapability(rec, schema); err != nil {
			return err
		}
		if err := m.activateSchema(ctx, rec, schema); err != nil {
			return fmt.Errorf("activate schema for %q: %w", schema.ResourceType, err)
		}
	}
	return nil
}

func (m *AddonManager) deactivateSchema(ctx context.Context, rec *addonRecord, rt domain.ResourceType) {
	if id, ok := rec.schemaRegistrations[rt]; ok {
		m.activator.Deactivate(id)
		delete(rec.schemaRegistrations, rt)
	}
	if _, ok := rec.registeredTypeDefs[rt]; ok {
		_ = m.typeSvc.Delete(ctx, rt)
		delete(rec.registeredTypeDefs, rt)
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
// managed resource type defs are deleted. The addon transitions to
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

	for rt := range rec.schemaRegistrations {
		m.deactivateSchema(ctx, rec, rt)
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

// validateSchemaCapability checks that the schema's ResourceType
// matches a declared ManagedResourceCapability on the addon.
func validateSchemaCapability(rec *addonRecord, schema domain.ManagedResourceSchema) error {
	for _, cap := range rec.addon.Capabilities {
		if mrc, ok := cap.(domain.ManagedResourceCapability); ok {
			if mrc.ResourceType == schema.ResourceType {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: addon %q has no ManagedResourceCapability for resource type %q",
		domain.ErrInvalidArgument, rec.addon.ID, schema.ResourceType)
}

// activateSchema delegates to the SchemaActivator and records the
// resulting registration ID and type def.
//
// Type metadata is validated before activation so that a CreateType or
// drift-detection failure never leaves routes live with no matching
// type definition (and never tears down the previous registration).
func (m *AddonManager) activateSchema(ctx context.Context, rec *addonRecord, schema domain.ManagedResourceSchema) error {
	// Validate / register the type definition first — this is
	// side-effect-free with respect to the transport layer, so
	// a failure here keeps the previous registration intact.
	if _, ok := rec.registeredTypeDefs[schema.ResourceType]; !ok {
		newSvc := domain.ServiceName(schema.APIServiceName)
		newVer := domain.APIVersion(schema.Version)
		newCol := domain.CollectionID(schema.CollectionID)

		_, err := m.typeSvc.Create(ctx, CreateTypeInput{
			ResourceType:   schema.ResourceType,
			Relation:       schema.Relation,
			Signature:      domain.Signature{},
			APIServiceName: newSvc,
			APIVersion:     newVer,
			CollectionID:   newCol,
		})
		if err != nil {
			if !errors.Is(err, domain.ErrAlreadyExists) {
				return fmt.Errorf("create type def: %w", err)
			}
			if err := m.detectAPIMetadataDrift(ctx, schema.ResourceType, newSvc, newVer, newCol); err != nil {
				return err
			}
		}
		if rec.registeredTypeDefs == nil {
			rec.registeredTypeDefs = make(map[domain.ResourceType]struct{})
		}
		rec.registeredTypeDefs[schema.ResourceType] = struct{}{}
	}

	id, err := m.activator.Activate(ctx, schema)
	if err != nil {
		return err
	}
	if rec.schemaRegistrations == nil {
		rec.schemaRegistrations = make(map[domain.ResourceType]SchemaRegistrationID)
	}

	// If the registration ID changed (e.g. the gRPC service name
	// changed due to a package rename), deactivate the old one so
	// its gRPC/HTTP routes don't leak.
	if prev, ok := rec.schemaRegistrations[schema.ResourceType]; ok && prev != id {
		m.activator.Deactivate(prev)
	}
	rec.schemaRegistrations[schema.ResourceType] = id

	return nil
}

// detectAPIMetadataDrift loads the existing type def and rejects
// reconnection attempts that change the API identity fields.
func (m *AddonManager) detectAPIMetadataDrift(ctx context.Context, rt domain.ResourceType, newSvc domain.ServiceName, newVer domain.APIVersion, newCol domain.CollectionID) error {
	existing, err := m.typeSvc.Get(ctx, rt)
	if err != nil {
		return fmt.Errorf("load existing type def for drift detection: %w", err)
	}
	if existing.APIServiceName != newSvc {
		return fmt.Errorf("%w: API service name drift: existing %q, new %q", domain.ErrInvalidArgument, existing.APIServiceName, newSvc)
	}
	if existing.APIVersion != newVer {
		return fmt.Errorf("%w: API version drift: existing %q, new %q", domain.ErrInvalidArgument, existing.APIVersion, newVer)
	}
	if existing.CollectionID != newCol {
		return fmt.Errorf("%w: collection ID drift: existing %q, new %q", domain.ErrInvalidArgument, existing.CollectionID, newCol)
	}
	return nil
}
