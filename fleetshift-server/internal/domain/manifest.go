package domain

import "encoding/json"

// ResourceType identifies the kind of declarative resource in a [Manifest].
// The platform treats manifest content as opaque; ResourceType provides
// metadata that delivery agents use for dispatch and validation.
type ResourceType string

// Manifest is an opaque declarative payload delivered to a target.
// ResourceType identifies what the payload represents (e.g.
// "api.kind.cluster"); Raw holds the actual content as opaque JSON.
type Manifest struct {
	ResourceType ResourceType
	Name         ResourceName
	Raw          json.RawMessage
}

// FilterAcceptedManifests returns the subset of manifests whose
// [ResourceType] is accepted by the target. If the target has no
// AcceptedResourceTypes (unconstrained / legacy), all manifests are
// returned unchanged.
func FilterAcceptedManifests(target TargetInfo, manifests []Manifest) []Manifest {
	if len(target.AcceptedResourceTypes) == 0 {
		return manifests
	}
	accepted := make(map[ResourceType]struct{}, len(target.AcceptedResourceTypes))
	for _, rt := range target.AcceptedResourceTypes {
		accepted[rt] = struct{}{}
	}
	out := make([]Manifest, 0, len(manifests))
	for _, m := range manifests {
		if _, ok := accepted[m.ResourceType]; ok {
			out = append(out, m)
		}
	}
	return out
}
