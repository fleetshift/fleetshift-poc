package domain

import (
	"encoding/json"
	"time"
)

// ResourceName uniquely identifies a managed resource instance within
// its [ResourceType]. Following AIP naming conventions, this is the
// leaf segment of the resource name (e.g. "prod-us-east-1" for a
// resource named "clusters/prod-us-east-1").
type ResourceName string

// IntentVersion is a monotonically increasing counter for versioned
// resource intent within a managed resource. Each spec update creates
// a new version; the HEAD table tracks which version is current.
type IntentVersion int64

// RawSchema is an unparsed JSON Schema document as stored in the
// database. It is compiled into a [Schema] for runtime validation.
type RawSchema json.RawMessage

// MarshalJSON implements json.Marshaler.
func (r RawSchema) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return json.RawMessage(r).MarshalJSON()
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *RawSchema) UnmarshalJSON(data []byte) error {
	return (*json.RawMessage)(r).UnmarshalJSON(data)
}

// ManagedResourceTypeDef is the metadata record that an addon registers
// to declare ownership of a managed resource type. It carries the
// fulfillment relation (how resources of this type map to fulfillments),
// the addon's cryptographic proof of that claim, and an optional JSON
// Schema for validating resource specs.
type ManagedResourceTypeDef struct {
	ResourceType ResourceType
	Relation     FulfillmentRelation
	Signature    Signature
	SpecSchema   *RawSchema
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// MarshalJSON implements json.Marshaler so the interface-typed Relation
// field survives JSON round-trips (required for durable workflow replay).
func (d ManagedResourceTypeDef) MarshalJSON() ([]byte, error) {
	rel, err := marshalFulfillmentRelation(d.Relation)
	if err != nil {
		return nil, err
	}
	type alias struct {
		ResourceType ResourceType       `json:"ResourceType"`
		Relation     fulfillmentRelJSON `json:"Relation"`
		Signature    Signature          `json:"Signature"`
		SpecSchema   *RawSchema         `json:"SpecSchema,omitempty"`
		CreatedAt    time.Time          `json:"CreatedAt"`
		UpdatedAt    time.Time          `json:"UpdatedAt"`
	}
	return json.Marshal(alias{
		ResourceType: d.ResourceType,
		Relation:     rel,
		Signature:    d.Signature,
		SpecSchema:   d.SpecSchema,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *ManagedResourceTypeDef) UnmarshalJSON(data []byte) error {
	type alias struct {
		ResourceType ResourceType       `json:"ResourceType"`
		Relation     fulfillmentRelJSON `json:"Relation"`
		Signature    Signature          `json:"Signature"`
		SpecSchema   *RawSchema         `json:"SpecSchema,omitempty"`
		CreatedAt    time.Time          `json:"CreatedAt"`
		UpdatedAt    time.Time          `json:"UpdatedAt"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	d.ResourceType = a.ResourceType
	d.Signature = a.Signature
	d.SpecSchema = a.SpecSchema
	d.CreatedAt = a.CreatedAt
	d.UpdatedAt = a.UpdatedAt
	rel, err := unmarshalFulfillmentRelation(a.Relation)
	if err != nil {
		return err
	}
	d.Relation = rel
	return nil
}

// ResourceIntent is an immutable version of a managed resource spec.
// INSERT only — never updated. The managed resource HEAD table tracks
// which version is current.
type ResourceIntent struct {
	ResourceType ResourceType
	Name         ResourceName
	Version      IntentVersion
	Spec         json.RawMessage
	CreatedAt    time.Time
}

// ManagedResource is the HEAD table aggregate for a managed resource
// instance. It owns identity, fulfillment linkage, and intent
// versioning. Mutations that affect orchestration go through the
// referenced [Fulfillment].
//
// Intent versioning follows the same drain pattern as
// [Fulfillment.DrainPendingStrategyRecords]: call [RecordIntent] to
// advance the version and collect a pending record, then the repository
// flushes it during Create/Update.
type ManagedResource struct {
	ResourceType   ResourceType
	Name           ResourceName
	UID            string
	CurrentVersion IntentVersion
	FulfillmentID  FulfillmentID
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      *time.Time

	pendingIntents []ResourceIntent
}

// RecordIntent advances the intent version and collects a pending
// [ResourceIntent] record for the repository to flush. Returns the
// recorded intent for use in downstream derivation (e.g.
// [FulfillmentRelation.DeriveStrategies]). This is the only way to
// create intents — the aggregate owns the version counter.
func (mr *ManagedResource) RecordIntent(spec json.RawMessage, now time.Time) ResourceIntent {
	mr.CurrentVersion++
	intent := ResourceIntent{
		ResourceType: mr.ResourceType,
		Name:         mr.Name,
		Version:      mr.CurrentVersion,
		Spec:         spec,
		CreatedAt:    now,
	}
	mr.pendingIntents = append(mr.pendingIntents, intent)
	return intent
}

// DrainPendingIntents returns and clears the collected intent records.
// Called by the repository implementation inside Create and Update.
func (mr *ManagedResource) DrainPendingIntents() []ResourceIntent {
	p := mr.pendingIntents
	mr.pendingIntents = nil
	return p
}

// ManagedResourceView is the read model that joins a [ManagedResource]
// with its current [ResourceIntent] and [Fulfillment]. Constructed by
// the repository via joins; never written directly.
type ManagedResourceView struct {
	ManagedResource ManagedResource
	Intent          ResourceIntent
	Fulfillment     Fulfillment
}
