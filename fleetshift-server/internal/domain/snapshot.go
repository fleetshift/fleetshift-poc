package domain

import (
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// Snapshot types
//
// Every aggregate that participates in a repository has a corresponding
// snapshot DTO: an all-exported, anemic struct used as the explicit
// serialization boundary between the domain and persistence layers.
//
// See docs/domain.md ("Snapshots and persistence") for the full pattern.
// ---------------------------------------------------------------------------

// FulfillmentSnapshot is the persistence DTO for [Fulfillment].
//
// It captures persisted state and pending strategy buffers. Internal
// baselines (loadedGeneration) are omitted -- [FulfillmentFromSnapshot]
// derives them from persisted state.
type FulfillmentSnapshot struct {
	ID                       FulfillmentID
	ManifestStrategy         ManifestStrategySpec
	ManifestStrategyVersion  StrategyVersion
	PlacementStrategy        PlacementStrategySpec
	PlacementStrategyVersion StrategyVersion
	RolloutStrategy          *RolloutStrategySpec
	RolloutStrategyVersion   StrategyVersion
	ResolvedTargets          []TargetID
	State                    FulfillmentState
	PauseReason              string
	StatusReason             string
	Auth                     DeliveryAuth
	Provenance               *Provenance
	AttestationRef           *AttestationRef
	Generation               Generation
	ObservedGeneration       Generation
	ActiveWorkflowGen        *Generation
	CreatedAt                time.Time
	UpdatedAt                time.Time

	// Pending strategy records collected by Advance* methods.
	// Populated on Snapshot() for write-path serialization;
	// empty when constructed by FulfillmentFromSnapshot (read path).
	PendingStrategyRecords PendingStrategyRecords
}

// DeploymentSnapshot is the persistence DTO for [Deployment].
type DeploymentSnapshot struct {
	Name          ResourceName
	UID           DeploymentUID
	FulfillmentID FulfillmentID
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// DeliverySnapshot is the persistence DTO for [Delivery].
type DeliverySnapshot struct {
	ID            DeliveryID
	FulfillmentID FulfillmentID
	TargetID      TargetID
	Manifests     []Manifest
	Generation    Generation
	State         DeliveryState
	Operation     DeliveryOperation
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// TargetInfoSnapshot is the persistence DTO for [TargetInfo].
type TargetInfoSnapshot struct {
	ID                    TargetID
	InventoryItemID       InventoryItemID
	Type                  TargetType
	Name                  string
	State                 TargetState
	Labels                map[string]string
	Properties            map[string]string
	AcceptedManifestTypes []ManifestType
}

// InventoryItemSnapshot is the persistence DTO for [InventoryItem].
type InventoryItemSnapshot struct {
	ID               InventoryItemID
	Type             InventoryItemType
	Name             string
	Properties       json.RawMessage
	Labels           map[string]string
	SourceDeliveryID *DeliveryID
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// AuthMethodSnapshot is the persistence DTO for [AuthMethod].
// The OIDC sub-object ([OIDCConfig]) is an immutable value object
// with all-exported fields, so it embeds directly.
type AuthMethodSnapshot struct {
	ID   AuthMethodID
	Type AuthMethodType
	OIDC *OIDCConfig
}

// SignerEnrollmentSnapshot is the persistence DTO for [SignerEnrollment].
type SignerEnrollmentSnapshot struct {
	ID SignerEnrollmentID
	FederatedIdentity
	IdentityToken   RawToken
	RegistrySubject RegistrySubject
	RegistryID      KeyRegistryID
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// PlatformResourceSnapshot is the persistence DTO for [PlatformResource].
// It captures the aggregate's full state including child entities.
//
// There is no UID field: [ResourceName] is the sole, permanent
// identifier for a platform resource (see resource_identity.go's
// [NewPlatformResource] doc for why).
type PlatformResourceSnapshot struct {
	Name      ResourceName
	Labels    map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time

	Representations []ResourceRepresentationSnapshot
	Aliases         []ResourceAliasSnapshot
	Relationships   []ResourceRelationshipSnapshot
}

// ResourceRepresentationSnapshot is the persistence DTO for
// [ResourceRepresentation]. It is never itself persisted -- the
// repository builds these on read by deriving representations from
// extension resources (see resource_identity_and_api.md).
type ResourceRepresentationSnapshot struct {
	ServiceName          ServiceName
	Version              APIVersion
	Name                 ResourceName
	ExtensionResourceUID ExtensionResourceUID
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// ResourceAliasSnapshot is the persistence DTO for an [Alias] bound
// to a platform resource.
type ResourceAliasSnapshot struct {
	Namespace AliasNamespace
	Key       AliasKey
	Value     AliasValue
	CreatedAt time.Time
}

// ResourceRelationshipSnapshot is the persistence DTO for
// [ResourceRelationship].
type ResourceRelationshipSnapshot struct {
	SourceName    ResourceName
	Type          RelationshipType
	TargetName    ResourceName
	SourceService ServiceName
	CreatedAt     time.Time
}

// ManagementTypeSnapshot is the persistence DTO for [ManagementType].
type ManagementTypeSnapshot struct {
	Relation  FulfillmentRelation
	Signature Signature
}

// InventoryTypeSnapshot is the persistence DTO for [InventoryType].
// An empty struct signals "inventory-capable"; nil means not.
type InventoryTypeSnapshot struct{}

// ConditionSnapshot is the persistence DTO for [Condition].
type ConditionSnapshot struct {
	Type               ConditionType
	Status             ConditionStatus
	Reason             string
	Message            string
	LastTransitionTime time.Time
}

// InventoryResourceSnapshot is the persistence DTO for [InventoryResource].
//
// Observation is nil when there is no latest observation, distinct
// from a present-but-empty JSON object; see [InventoryResource.Observation].
type InventoryResourceSnapshot struct {
	Labels      map[string]string
	Observation json.RawMessage
	Conditions  []ConditionSnapshot
	ObservedAt  time.Time
	UpdatedAt   time.Time
}

// ObservationSnapshot is the persistence DTO for [Observation].
type ObservationSnapshot struct {
	ID                   ObservationID
	ExtensionResourceUID ExtensionResourceUID
	Observation          json.RawMessage
	ObservedAt           time.Time
	CreatedAt            time.Time
}

// ConditionTransitionSnapshot is the persistence DTO for
// [ConditionTransition].
type ConditionTransitionSnapshot struct {
	ID                   ConditionTransitionID
	ExtensionResourceUID ExtensionResourceUID
	ConditionType        ConditionType
	Status               ConditionStatus
	Reason               string
	Message              string
	LastTransitionTime   time.Time
	ObservedAt           time.Time
	CreatedAt            time.Time
}

// ExtensionResourceTypeSnapshot is the persistence DTO for
// [ExtensionResourceType].
type ExtensionResourceTypeSnapshot struct {
	ResourceType ResourceType
	APIVersion   APIVersion
	CollectionID CollectionID
	Management   *ManagementTypeSnapshot
	Inventory    *InventoryTypeSnapshot
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ManagedStateSnapshot is the persistence DTO for [ManagedState].
type ManagedStateSnapshot struct {
	CurrentVersion IntentVersion
	FulfillmentID  FulfillmentID
}

// ExtensionResourceSnapshot is the persistence DTO for
// [ExtensionResource].
//
// It captures persisted state and pending intents. On the read path,
// [ExtensionResourceFromSnapshot] initializes pending intents as empty.
type ExtensionResourceSnapshot struct {
	UID          ExtensionResourceUID
	ResourceType ResourceType
	Name         ResourceName
	Labels       map[string]string
	Managed      *ManagedStateSnapshot
	Inventory    *InventoryResourceSnapshot
	CreatedAt    time.Time
	UpdatedAt    time.Time

	// ReportedAliases mirrors [ExtensionResource.ReportedAliases] --
	// this extension resource's own pending, unreconciled alias
	// assertions. It is empty (never nil) when this resource has never
	// reported any aliases.
	ReportedAliases []Alias

	// Pending intents collected by RecordIntent.
	// Populated on Snapshot() for write-path serialization;
	// empty when constructed by ExtensionResourceFromSnapshot (read path).
	PendingIntents []ResourceIntent
}

// ---------------------------------------------------------------------------
// Snapshot() methods -- extract current state into a snapshot DTO.
// ---------------------------------------------------------------------------

// Snapshot returns a [FulfillmentSnapshot] capturing all persisted state
// and pending strategy buffers. Internal baselines (loadedGeneration) are
// omitted.
func (f *Fulfillment) Snapshot() FulfillmentSnapshot {
	return FulfillmentSnapshot{
		ID:                       f.id,
		ManifestStrategy:         f.manifestStrategy,
		ManifestStrategyVersion:  f.manifestStrategyVersion,
		PlacementStrategy:        f.placementStrategy,
		PlacementStrategyVersion: f.placementStrategyVersion,
		RolloutStrategy:          f.rolloutStrategy,
		RolloutStrategyVersion:   f.rolloutStrategyVersion,
		ResolvedTargets:          f.resolvedTargets,
		State:                    f.state,
		PauseReason:              f.pauseReason,
		StatusReason:             f.statusReason,
		Auth:                     f.auth,
		Provenance:               f.provenance,
		AttestationRef:           f.attestationRef,
		Generation:               f.generation,
		ObservedGeneration:       f.observedGeneration,
		ActiveWorkflowGen:        f.activeWorkflowGen,
		CreatedAt:                f.createdAt,
		UpdatedAt:                f.updatedAt,
		PendingStrategyRecords: PendingStrategyRecords{
			Manifest:  f.pendingManifest,
			Placement: f.pendingPlacement,
			Rollout:   f.pendingRollout,
		},
	}
}

// Snapshot returns a [DeploymentSnapshot] capturing all persisted state.
func (d Deployment) Snapshot() DeploymentSnapshot {
	return DeploymentSnapshot{
		Name:          d.name,
		UID:           d.uid,
		FulfillmentID: d.fulfillmentID,
		CreatedAt:     d.createdAt,
		UpdatedAt:     d.updatedAt,
	}
}

// Snapshot returns a [DeliverySnapshot] capturing all persisted state.
func (d *Delivery) Snapshot() DeliverySnapshot {
	return DeliverySnapshot{
		ID:            d.id,
		FulfillmentID: d.fulfillmentID,
		TargetID:      d.targetID,
		Manifests:     d.manifests,
		Generation:    d.generation,
		State:         d.state,
		Operation:     d.operation,
		CreatedAt:     d.createdAt,
		UpdatedAt:     d.updatedAt,
	}
}

// Snapshot returns a [TargetInfoSnapshot] capturing all persisted state.
func (t TargetInfo) Snapshot() TargetInfoSnapshot {
	return TargetInfoSnapshot{
		ID:                    t.id,
		InventoryItemID:       t.inventoryItemID,
		Type:                  t.targetType,
		Name:                  t.name,
		State:                 t.state,
		Labels:                t.labels,
		Properties:            t.properties,
		AcceptedManifestTypes: t.acceptedManifestTypes,
	}
}

// Snapshot returns an [InventoryItemSnapshot] capturing all persisted state.
func (i InventoryItem) Snapshot() InventoryItemSnapshot {
	return InventoryItemSnapshot{
		ID:               i.id,
		Type:             i.inventoryType,
		Name:             i.name,
		Properties:       i.properties,
		Labels:           i.labels,
		SourceDeliveryID: i.sourceDeliveryID,
		CreatedAt:        i.createdAt,
		UpdatedAt:        i.updatedAt,
	}
}

// Snapshot returns an [AuthMethodSnapshot] capturing all persisted state.
func (m AuthMethod) Snapshot() AuthMethodSnapshot {
	return AuthMethodSnapshot{
		ID:   m.id,
		Type: m.authType,
		OIDC: m.oidcConfig,
	}
}

// Snapshot returns a [SignerEnrollmentSnapshot] capturing all persisted state.
func (e SignerEnrollment) Snapshot() SignerEnrollmentSnapshot {
	return SignerEnrollmentSnapshot{
		ID:                e.id,
		FederatedIdentity: e.federatedIdentity,
		IdentityToken:     e.identityToken,
		RegistrySubject:   e.registrySubject,
		RegistryID:        e.registryID,
		CreatedAt:         e.createdAt,
		ExpiresAt:         e.expiresAt,
	}
}

// Snapshot returns an [InventoryResourceSnapshot] capturing all state.
func (ir InventoryResource) Snapshot() InventoryResourceSnapshot {
	conds := make([]ConditionSnapshot, len(ir.conditions))
	for i, c := range ir.conditions {
		conds[i] = ConditionSnapshot{
			Type:               c.conditionType,
			Status:             c.status,
			Reason:             c.reason,
			Message:            c.message,
			LastTransitionTime: c.lastTransitionTime,
		}
	}
	labels := make(map[string]string, len(ir.labels))
	for k, v := range ir.labels {
		labels[k] = v
	}
	return InventoryResourceSnapshot{
		Labels:      labels,
		Observation: observationToSnapshot(ir.observation),
		Conditions:  conds,
		ObservedAt:  ir.observedAt,
		UpdatedAt:   ir.updatedAt,
	}
}

// observationToSnapshot converts a domain observation pointer to its
// snapshot representation, preserving nil ("no latest observation")
// faithfully rather than defaulting to an empty JSON object.
func observationToSnapshot(o *json.RawMessage) json.RawMessage {
	if o == nil {
		return nil
	}
	return *o
}

// observationFromSnapshot converts a snapshot observation back to the
// domain pointer representation, preserving nil faithfully.
func observationFromSnapshot(o json.RawMessage) *json.RawMessage {
	if o == nil {
		return nil
	}
	return &o
}

// Snapshot returns an [ExtensionResourceTypeSnapshot] capturing all
// persisted state.
func (t ExtensionResourceType) Snapshot() ExtensionResourceTypeSnapshot {
	var mgmt *ManagementTypeSnapshot
	if t.management != nil {
		mgmt = &ManagementTypeSnapshot{
			Relation:  t.management.relation,
			Signature: t.management.signature,
		}
	}
	var inv *InventoryTypeSnapshot
	if t.inventory != nil {
		inv = &InventoryTypeSnapshot{}
	}
	return ExtensionResourceTypeSnapshot{
		ResourceType: t.resourceType,
		APIVersion:   t.apiVersion,
		CollectionID: t.collectionID,
		Management:   mgmt,
		Inventory:    inv,
		CreatedAt:    t.createdAt,
		UpdatedAt:    t.updatedAt,
	}
}

// Snapshot returns an [ExtensionResourceSnapshot] capturing all
// persisted state and pending intents.
func (r *ExtensionResource) Snapshot() ExtensionResourceSnapshot {
	var managed *ManagedStateSnapshot
	if r.managed != nil {
		managed = &ManagedStateSnapshot{
			CurrentVersion: r.managed.currentVersion,
			FulfillmentID:  r.managed.fulfillmentID,
		}
	}
	labels := make(map[string]string, len(r.labels))
	for k, v := range r.labels {
		labels[k] = v
	}
	var inv *InventoryResourceSnapshot
	if r.inventory != nil {
		conds := make([]ConditionSnapshot, len(r.inventory.conditions))
		for i, c := range r.inventory.conditions {
			conds[i] = ConditionSnapshot{
				Type:               c.conditionType,
				Status:             c.status,
				Reason:             c.reason,
				Message:            c.message,
				LastTransitionTime: c.lastTransitionTime,
			}
		}
		invLabels := make(map[string]string, len(r.inventory.labels))
		for k, v := range r.inventory.labels {
			invLabels[k] = v
		}
		inv = &InventoryResourceSnapshot{
			Labels:      invLabels,
			Observation: observationToSnapshot(r.inventory.observation),
			Conditions:  conds,
			ObservedAt:  r.inventory.observedAt,
			UpdatedAt:   r.inventory.updatedAt,
		}
	}
	aliases := make([]Alias, len(r.reportedAliases))
	copy(aliases, r.reportedAliases)

	return ExtensionResourceSnapshot{
		UID:             r.uid,
		ResourceType:    r.resourceType,
		Name:            r.name,
		Labels:          labels,
		Managed:         managed,
		Inventory:       inv,
		CreatedAt:       r.createdAt,
		UpdatedAt:       r.updatedAt,
		ReportedAliases: aliases,
		PendingIntents:  r.pendingIntents,
	}
}

// ---------------------------------------------------------------------------
// FromSnapshot factories -- hydrate a domain object from a snapshot.
//
// Each factory produces an object in "freshly loaded from storage" state:
// persisted state hydrated, pending buffers empty, internal baselines
// derived from persisted state.
// ---------------------------------------------------------------------------

// FulfillmentFromSnapshot constructs a [Fulfillment] from a snapshot.
// The internal loadedGeneration baseline is set to s.Generation so that
// [advanceGeneration] enforces the single-bump invariant. Pending
// strategy buffers start nil regardless of what the snapshot contains.
func FulfillmentFromSnapshot(s FulfillmentSnapshot) *Fulfillment {
	return &Fulfillment{
		id:                       s.ID,
		manifestStrategy:         s.ManifestStrategy,
		manifestStrategyVersion:  s.ManifestStrategyVersion,
		placementStrategy:        s.PlacementStrategy,
		placementStrategyVersion: s.PlacementStrategyVersion,
		rolloutStrategy:          s.RolloutStrategy,
		rolloutStrategyVersion:   s.RolloutStrategyVersion,
		resolvedTargets:          s.ResolvedTargets,
		state:                    s.State,
		pauseReason:              s.PauseReason,
		statusReason:             s.StatusReason,
		auth:                     s.Auth,
		provenance:               s.Provenance,
		attestationRef:           s.AttestationRef,
		generation:               s.Generation,
		observedGeneration:       s.ObservedGeneration,
		activeWorkflowGen:        s.ActiveWorkflowGen,
		createdAt:                s.CreatedAt,
		updatedAt:                s.UpdatedAt,
		loadedGeneration:         s.Generation,
	}
}

// DeploymentFromSnapshot constructs a [Deployment] from a snapshot.
func DeploymentFromSnapshot(s DeploymentSnapshot) Deployment {
	return Deployment{
		name:          s.Name,
		uid:           s.UID,
		fulfillmentID: s.FulfillmentID,
		createdAt:     s.CreatedAt,
		updatedAt:     s.UpdatedAt,
	}
}

// DeliveryFromSnapshot constructs a [Delivery] from a snapshot.
func DeliveryFromSnapshot(s DeliverySnapshot) Delivery {
	return Delivery{
		id:            s.ID,
		fulfillmentID: s.FulfillmentID,
		targetID:      s.TargetID,
		manifests:     s.Manifests,
		generation:    s.Generation,
		state:         s.State,
		operation:     s.Operation,
		createdAt:     s.CreatedAt,
		updatedAt:     s.UpdatedAt,
	}
}

// TargetInfoFromSnapshot constructs a [TargetInfo] from a snapshot.
func TargetInfoFromSnapshot(s TargetInfoSnapshot) TargetInfo {
	return TargetInfo{
		id:                    s.ID,
		inventoryItemID:       s.InventoryItemID,
		targetType:            s.Type,
		name:                  s.Name,
		state:                 s.State,
		labels:                s.Labels,
		properties:            s.Properties,
		acceptedManifestTypes: s.AcceptedManifestTypes,
	}
}

// InventoryItemFromSnapshot constructs an [InventoryItem] from a snapshot.
func InventoryItemFromSnapshot(s InventoryItemSnapshot) InventoryItem {
	return InventoryItem{
		id:               s.ID,
		inventoryType:    s.Type,
		name:             s.Name,
		properties:       s.Properties,
		labels:           s.Labels,
		sourceDeliveryID: s.SourceDeliveryID,
		createdAt:        s.CreatedAt,
		updatedAt:        s.UpdatedAt,
	}
}

// AuthMethodFromSnapshot constructs an [AuthMethod] from a snapshot.
func AuthMethodFromSnapshot(s AuthMethodSnapshot) AuthMethod {
	return AuthMethod{
		id:         s.ID,
		authType:   s.Type,
		oidcConfig: s.OIDC,
	}
}

// SignerEnrollmentFromSnapshot constructs a [SignerEnrollment] from a snapshot.
func SignerEnrollmentFromSnapshot(s SignerEnrollmentSnapshot) SignerEnrollment {
	return SignerEnrollment{
		id:                s.ID,
		federatedIdentity: s.FederatedIdentity,
		identityToken:     s.IdentityToken,
		registrySubject:   s.RegistrySubject,
		registryID:        s.RegistryID,
		createdAt:         s.CreatedAt,
		expiresAt:         s.ExpiresAt,
	}
}

// PlatformResourceFromSnapshot constructs a [PlatformResource] from a
// snapshot. Labels are shallow-copied to avoid sharing the map with
// the caller. Child entities (representations, aliases,
// relationships) are reconstituted from their snapshot slices.
func PlatformResourceFromSnapshot(s PlatformResourceSnapshot) *PlatformResource {
	labels := make(map[string]string, len(s.Labels))
	for k, v := range s.Labels {
		labels[k] = v
	}

	reps := make([]ResourceRepresentation, len(s.Representations))
	for i, rs := range s.Representations {
		reps[i] = ResourceRepresentationFromSnapshot(rs)
	}

	aliases := make([]Alias, len(s.Aliases))
	for i, as := range s.Aliases {
		aliases[i] = Alias{Namespace: as.Namespace, Key: as.Key, Value: as.Value}
	}

	rels := make([]ResourceRelationship, len(s.Relationships))
	for i, rs := range s.Relationships {
		rels[i] = ResourceRelationshipFromSnapshot(rs)
	}

	return &PlatformResource{
		name:            s.Name,
		labels:          labels,
		createdAt:       s.CreatedAt,
		updatedAt:       s.UpdatedAt,
		representations: reps,
		aliases:         aliases,
		relationships:   rels,
	}
}

// ExtensionResourceTypeFromSnapshot constructs an
// [ExtensionResourceType] from a snapshot.
func ExtensionResourceTypeFromSnapshot(s ExtensionResourceTypeSnapshot) ExtensionResourceType {
	var mgmt *ManagementType
	if s.Management != nil {
		mgmt = &ManagementType{
			relation:  s.Management.Relation,
			signature: s.Management.Signature,
		}
	}
	var inv *InventoryType
	if s.Inventory != nil {
		it := InventoryType{}
		inv = &it
	}
	return ExtensionResourceType{
		resourceType: s.ResourceType,
		apiVersion:   s.APIVersion,
		collectionID: s.CollectionID,
		management:   mgmt,
		inventory:    inv,
		createdAt:    s.CreatedAt,
		updatedAt:    s.UpdatedAt,
	}
}

// ExtensionResourceFromSnapshot constructs an [ExtensionResource] from
// a snapshot. Pending intents start nil regardless of what the snapshot
// contains. Labels are shallow-copied to avoid sharing the map with
// the caller.
func ExtensionResourceFromSnapshot(s ExtensionResourceSnapshot) *ExtensionResource {
	labels := make(map[string]string, len(s.Labels))
	for k, v := range s.Labels {
		labels[k] = v
	}
	var managed *ManagedState
	if s.Managed != nil {
		managed = &ManagedState{
			currentVersion: s.Managed.CurrentVersion,
			fulfillmentID:  s.Managed.FulfillmentID,
		}
	}
	var inv *InventoryResource
	if s.Inventory != nil {
		conds := make([]Condition, len(s.Inventory.Conditions))
		for i, cs := range s.Inventory.Conditions {
			conds[i] = Condition{
				conditionType:      cs.Type,
				status:             cs.Status,
				reason:             cs.Reason,
				message:            cs.Message,
				lastTransitionTime: cs.LastTransitionTime,
			}
		}
		invLabels := make(map[string]string, len(s.Inventory.Labels))
		for k, v := range s.Inventory.Labels {
			invLabels[k] = v
		}
		inv = &InventoryResource{
			labels:      invLabels,
			observation: observationFromSnapshot(s.Inventory.Observation),
			conditions:  conds,
			observedAt:  s.Inventory.ObservedAt,
			updatedAt:   s.Inventory.UpdatedAt,
		}
	}
	aliases := make([]Alias, len(s.ReportedAliases))
	copy(aliases, s.ReportedAliases)

	return &ExtensionResource{
		uid:             s.UID,
		resourceType:    s.ResourceType,
		name:            s.Name,
		labels:          labels,
		managed:         managed,
		inventory:       inv,
		reportedAliases: aliases,
		createdAt:       s.CreatedAt,
		updatedAt:       s.UpdatedAt,
	}
}

// InventoryResourceFromSnapshot constructs an [InventoryResource] from
// a snapshot. Used by repository implementations to reconstitute
// inventory state from storage.
func InventoryResourceFromSnapshot(s InventoryResourceSnapshot) *InventoryResource {
	conds := make([]Condition, len(s.Conditions))
	for i, cs := range s.Conditions {
		conds[i] = Condition{
			conditionType:      cs.Type,
			status:             cs.Status,
			reason:             cs.Reason,
			message:            cs.Message,
			lastTransitionTime: cs.LastTransitionTime,
		}
	}
	labels := make(map[string]string, len(s.Labels))
	for k, v := range s.Labels {
		labels[k] = v
	}
	return &InventoryResource{
		labels:      labels,
		observation: observationFromSnapshot(s.Observation),
		conditions:  conds,
		observedAt:  s.ObservedAt,
		updatedAt:   s.UpdatedAt,
	}
}

// ObservationFromSnapshot constructs an [Observation] from a snapshot.
func ObservationFromSnapshot(s ObservationSnapshot) Observation {
	return Observation{
		id:                   s.ID,
		extensionResourceUID: s.ExtensionResourceUID,
		observation:          s.Observation,
		observedAt:           s.ObservedAt,
		createdAt:            s.CreatedAt,
	}
}

// ConditionTransitionFromSnapshot constructs a [ConditionTransition]
// from a snapshot.
func ConditionTransitionFromSnapshot(s ConditionTransitionSnapshot) ConditionTransition {
	return ConditionTransition{
		id:                   s.ID,
		extensionResourceUID: s.ExtensionResourceUID,
		conditionType:        s.ConditionType,
		status:               s.Status,
		reason:               s.Reason,
		message:              s.Message,
		lastTransitionTime:   s.LastTransitionTime,
		observedAt:           s.ObservedAt,
		createdAt:            s.CreatedAt,
	}
}

// Snapshot returns an [ObservationSnapshot] capturing all state.
func (o Observation) Snapshot() ObservationSnapshot {
	return ObservationSnapshot{
		ID:                   o.id,
		ExtensionResourceUID: o.extensionResourceUID,
		Observation:          o.observation,
		ObservedAt:           o.observedAt,
		CreatedAt:            o.createdAt,
	}
}

// Snapshot returns a [ConditionTransitionSnapshot] capturing all state.
func (t ConditionTransition) Snapshot() ConditionTransitionSnapshot {
	return ConditionTransitionSnapshot{
		ID:                   t.id,
		ExtensionResourceUID: t.extensionResourceUID,
		ConditionType:        t.conditionType,
		Status:               t.status,
		Reason:               t.reason,
		Message:              t.message,
		LastTransitionTime:   t.lastTransitionTime,
		ObservedAt:           t.observedAt,
		CreatedAt:            t.createdAt,
	}
}

// ---------------------------------------------------------------------------
// JSON marshaling -- delegate to snapshot types so private-field domain
// objects survive encoding/json round-trips (e.g. memworkflow's JSON
// fidelity pass for activity inputs/outputs).
// ---------------------------------------------------------------------------

// Value receivers are used for MarshalJSON so the method is in the
// value-receiver method set. This matters when json.Marshal encounters
// a non-addressable struct value (e.g. a field of a value passed to
// jsonRoundTrip): pointer-receiver methods would not be found, and the
// encoder would fall back to field-based encoding -- producing {} for
// private fields.

func (f Fulfillment) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Snapshot())
}

func (f *Fulfillment) UnmarshalJSON(data []byte) error {
	var s FulfillmentSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*f = *FulfillmentFromSnapshot(s)
	return nil
}

func (d Deployment) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Snapshot())
}

func (d *Deployment) UnmarshalJSON(data []byte) error {
	var s DeploymentSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*d = DeploymentFromSnapshot(s)
	return nil
}

func (d Delivery) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Snapshot())
}

func (d *Delivery) UnmarshalJSON(data []byte) error {
	var s DeliverySnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*d = DeliveryFromSnapshot(s)
	return nil
}

func (t TargetInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Snapshot())
}

func (t *TargetInfo) UnmarshalJSON(data []byte) error {
	var s TargetInfoSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*t = TargetInfoFromSnapshot(s)
	return nil
}

func (i InventoryItem) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.Snapshot())
}

func (i *InventoryItem) UnmarshalJSON(data []byte) error {
	var s InventoryItemSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*i = InventoryItemFromSnapshot(s)
	return nil
}

func (m AuthMethod) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Snapshot())
}

func (m *AuthMethod) UnmarshalJSON(data []byte) error {
	var s AuthMethodSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*m = AuthMethodFromSnapshot(s)
	return nil
}

func (e SignerEnrollment) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Snapshot())
}

func (e *SignerEnrollment) UnmarshalJSON(data []byte) error {
	var s SignerEnrollmentSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*e = SignerEnrollmentFromSnapshot(s)
	return nil
}

func (r PlatformResource) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Snapshot())
}

func (r *PlatformResource) UnmarshalJSON(data []byte) error {
	var s PlatformResourceSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*r = *PlatformResourceFromSnapshot(s)
	return nil
}

// ExtensionResource JSON marshaling delegates to its snapshot DTO.
// MarshalJSON on ExtensionResourceType is defined in
// extension_resource.go because it needs custom handling for the
// FulfillmentRelation interface within ManagementType.

func (r ExtensionResource) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Snapshot())
}

func (r *ExtensionResource) UnmarshalJSON(data []byte) error {
	var s ExtensionResourceSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*r = *ExtensionResourceFromSnapshot(s)
	return nil
}
