package domain

import "context"

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

	return []Manifest{{
		ManifestType: s.Ref.ManifestType,
		ManifestID:   ManifestID(s.Ref.ExtensionResourceUID.String()),
		ResourceName: resource.Name(),
		Raw:          intent.Spec,
	}}, nil
}

func (s *ManagedResourceManifestStrategy) OnRemoved(_ context.Context, _ TargetID) error {
	return nil
}
