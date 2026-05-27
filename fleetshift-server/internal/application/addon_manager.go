package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// SchemaActivator compiles and registers the transport-layer API
// surface for a managed resource schema. The application layer calls
// this without knowing about proto compilation, gRPC service
// descriptors, or HTTP muxes — the implementation lives in the
// transport layer.
type SchemaActivator interface {
	Activate(ctx context.Context, schema domain.ManagedResourceSchema) (SchemaHandle, error)
	Deactivate(handle SchemaHandle)
}

// SchemaHandle is an opaque token returned by [SchemaActivator.Activate]
// that identifies the transport registrations so they can be torn down.
type SchemaHandle struct {
	ServiceName string
	Plural      string
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

	router    DeliveryAgentRegistry
	typeSvc   *ManagedResourceTypeService
	activator SchemaActivator
}

// addonRecord is the in-memory state for an addon within the manager.
type addonRecord struct {
	addon domain.Addon
	agent domain.DeliveryAgent
	// Keyed by resource type so connectSchemas can reconcile the new
	// input against existing state and deactivate stale schemas.
	// Content-change detection is handled by the SchemaActivator itself.
	schemaHandles      map[domain.ResourceType]SchemaHandle
	registeredTypeDefs map[domain.ResourceType]struct{}
}

// NewAddonManager creates a new manager with the given dependencies.
func NewAddonManager(deps AddonManagerDeps) *AddonManager {
	return &AddonManager{
		addons:    make(map[domain.AddonID]*addonRecord),
		router:    deps.Router,
		typeSvc:   deps.TypeSvc,
		activator: deps.Activator,
	}
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
		rec.addon.EnabledAt = time.Now().UTC()
		return nil
	}

	now := time.Now().UTC()
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

	now := time.Now().UTC()
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

	for rt := range rec.schemaHandles {
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
	if handle, ok := rec.schemaHandles[rt]; ok {
		m.activator.Deactivate(handle)
		delete(rec.schemaHandles, rt)
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
	targetSvc := &TargetService{Store: m.typeSvc.Store}
	for _, t := range targets {
		if err := targetSvc.Register(ctx, t); err != nil {
			if errors.Is(err, domain.ErrAlreadyExists) {
				continue
			}
			return fmt.Errorf("register target %q: %w", t.ID, err)
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

	for rt := range rec.schemaHandles {
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
// resulting handle and type def.
func (m *AddonManager) activateSchema(ctx context.Context, rec *addonRecord, schema domain.ManagedResourceSchema) error {
	handle, err := m.activator.Activate(ctx, schema)
	if err != nil {
		return err
	}
	if rec.schemaHandles == nil {
		rec.schemaHandles = make(map[domain.ResourceType]SchemaHandle)
	}
	rec.schemaHandles[schema.ResourceType] = handle

	if _, ok := rec.registeredTypeDefs[schema.ResourceType]; !ok {
		if _, err := m.typeSvc.Create(ctx, CreateTypeInput{
			ResourceType: schema.ResourceType,
			Relation:     schema.Relation,
			Signature:    domain.Signature{},
		}); err != nil && !errors.Is(err, domain.ErrAlreadyExists) {
			return fmt.Errorf("create type def: %w", err)
		}
		if rec.registeredTypeDefs == nil {
			rec.registeredTypeDefs = make(map[domain.ResourceType]struct{})
		}
		rec.registeredTypeDefs[schema.ResourceType] = struct{}{}
	}

	return nil
}
