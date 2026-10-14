package domain

import "context"

// InlineManifestStrategy returns manifests verbatim from the spec.
type InlineManifestStrategy struct {
	Manifests []Manifest
}

func (s *InlineManifestStrategy) Generate(_ context.Context, _ GenerateContext) ([]Manifest, error) {
	result := make([]Manifest, len(s.Manifests))
	copy(result, s.Manifests)
	return result, nil
}

func (s *InlineManifestStrategy) OnRemoved(_ context.Context, _ TargetID) error {
	return nil
}
