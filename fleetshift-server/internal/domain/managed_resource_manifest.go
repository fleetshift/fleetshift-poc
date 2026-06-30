package domain

import (
	"context"
	"encoding/json"
	"fmt"
)

// ManagedResourceManifestStrategy resolves a [ResourceIntent] by
// reference and produces a single manifest containing the spec. This
// avoids duplicating the spec payload into the fulfillment's strategy
// record — only the coordinates are stored.
type ManagedResourceManifestStrategy struct {
	Ref   IntentRef
	Store Store
}

func (s *ManagedResourceManifestStrategy) Generate(ctx context.Context, _ GenerateContext) ([]Manifest, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	intent, err := tx.ExtensionResources().GetIntent(ctx, s.Ref.ExtensionResourceUID, s.Ref.Version)
	if err != nil {
		return nil, err
	}

	resource, err := tx.ExtensionResources().GetByUID(ctx, s.Ref.ExtensionResourceUID)
	if err != nil {
		return nil, err
	}

	raw, err := WrapManagedResourceSpec(resource.Name(), s.Ref.ExtensionResourceUID, intent.Spec)
	if err != nil {
		return nil, fmt.Errorf("wrap managed resource spec: %w", err)
	}

	return []Manifest{{
		ManifestType: s.Ref.ManifestType,
		ManifestID:   ManifestID(s.Ref.ExtensionResourceUID.String()),
		Raw:          raw,
	}}, nil
}

func (s *ManagedResourceManifestStrategy) OnRemoved(_ context.Context, _ TargetID) error {
	return nil
}

// ManagedResourceSpecManifest wraps a managed resource spec with
// identity fields so addons can extract the resource name and inner
// spec from the manifest payload.
type ManagedResourceSpecManifest struct {
	Name ResourceName         `json:"name"`
	UID  ExtensionResourceUID `json:"uid"`
	Spec json.RawMessage      `json:"spec"`
}

// WrapManagedResourceSpec marshals a ManagedResourceSpecManifest into
// JSON suitable for use as Manifest.Raw.
func WrapManagedResourceSpec(name ResourceName, uid ExtensionResourceUID, spec json.RawMessage) (json.RawMessage, error) {
	return json.Marshal(ManagedResourceSpecManifest{
		Name: name,
		UID:  uid,
		Spec: spec,
	})
}

// UnwrapManagedResourceSpec extracts identity and the inner spec from
// a manifest payload produced by WrapManagedResourceSpec.
func UnwrapManagedResourceSpec(raw json.RawMessage) (*ManagedResourceSpecManifest, error) {
	var m ManagedResourceSpecManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("unwrap managed resource spec: %w", err)
	}
	if m.Name == "" {
		return nil, fmt.Errorf("unwrap managed resource spec: name is required")
	}
	if m.Spec == nil {
		return nil, fmt.Errorf("unwrap managed resource spec: spec is required")
	}
	return &m, nil
}
