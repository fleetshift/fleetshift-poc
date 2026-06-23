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
	Type             InventoryType
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
type PlatformResourceSnapshot struct {
	UID       PlatformResourceUID
	Name      ResourceName
	Labels    map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time

	Representations []ResourceRepresentationSnapshot
	Aliases         []ResourceAliasSnapshot
	Relationships   []ResourceRelationshipSnapshot
}

// ResourceRepresentationSnapshot is the persistence DTO for
// [ResourceRepresentation].
type ResourceRepresentationSnapshot struct {
	PlatformUID PlatformResourceUID
	ServiceName ServiceName
	Version     APIVersion
	Name        ResourceName
	Roles       []RepresentationRole
	Labels      map[string]string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Deleted     bool
}

// ResourceAliasSnapshot is the persistence DTO for an [Alias] bound
// to a platform resource.
type ResourceAliasSnapshot struct {
	Namespace   AliasNamespace
	Key         AliasKey
	Value       AliasValue
	PlatformUID PlatformResourceUID
	CreatedAt   time.Time
}

// ResourceRelationshipSnapshot is the persistence DTO for
// [ResourceRelationship].
type ResourceRelationshipSnapshot struct {
	SourceUID     PlatformResourceUID
	Type          RelationshipType
	TargetUID     PlatformResourceUID
	SourceService ServiceName
	CreatedAt     time.Time
}

// ManagedResourceSnapshot is the persistence DTO for [ManagedResource].
//
// It captures persisted state and pending intents. On the read path,
// [ManagedResourceFromSnapshot] initializes pending intents as empty.
type ManagedResourceSnapshot struct {
	ResourceType   ResourceType
	Name           ResourceName
	UID            ManagedResourceUID
	CurrentVersion IntentVersion
	FulfillmentID  FulfillmentID
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      *time.Time

	// Pending intents collected by RecordIntent.
	// Populated on Snapshot() for write-path serialization;
	// empty when constructed by ManagedResourceFromSnapshot (read path).
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

// Snapshot returns a [ManagedResourceSnapshot] capturing all persisted
// state and pending intents.
func (mr *ManagedResource) Snapshot() ManagedResourceSnapshot {
	return ManagedResourceSnapshot{
		ResourceType:   mr.resourceType,
		Name:           mr.name,
		UID:            mr.uid,
		CurrentVersion: mr.currentVersion,
		FulfillmentID:  mr.fulfillmentID,
		CreatedAt:      mr.createdAt,
		UpdatedAt:      mr.updatedAt,
		DeletedAt:      mr.deletedAt,
		PendingIntents: mr.pendingIntents,
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
		uid:             s.UID,
		name:            s.Name,
		labels:          labels,
		createdAt:       s.CreatedAt,
		updatedAt:       s.UpdatedAt,
		representations: reps,
		aliases:         aliases,
		relationships:   rels,
	}
}

// ManagedResourceFromSnapshot constructs a [ManagedResource] from a
// snapshot. Pending intents start nil regardless of what the snapshot
// contains. CurrentVersion is restored so that future [RecordIntent]
// calls increment from the correct baseline.
func ManagedResourceFromSnapshot(s ManagedResourceSnapshot) *ManagedResource {
	return &ManagedResource{
		resourceType:   s.ResourceType,
		name:           s.Name,
		uid:            s.UID,
		currentVersion: s.CurrentVersion,
		fulfillmentID:  s.FulfillmentID,
		createdAt:      s.CreatedAt,
		updatedAt:      s.UpdatedAt,
		deletedAt:      s.DeletedAt,
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

func (mr ManagedResource) MarshalJSON() ([]byte, error) {
	return json.Marshal(mr.Snapshot())
}

func (mr *ManagedResource) UnmarshalJSON(data []byte) error {
	var s ManagedResourceSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*mr = *ManagedResourceFromSnapshot(s)
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
