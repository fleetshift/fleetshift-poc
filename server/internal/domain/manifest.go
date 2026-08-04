package domain

import (
	"encoding/json"
	"fmt"
)

// ManifestType is an opaque dispatch label for delivery targets.
// It identifies what kind of payload a [Manifest] carries (e.g.
// "api.kind.cluster", "idp-trust-bundle") so agents can route and
// validate without understanding manifest content.
type ManifestType string

// NewManifestType validates and returns a [ManifestType]. It rejects
// empty values.
func NewManifestType(s string) (ManifestType, error) {
	if s == "" {
		return "", fmt.Errorf("manifest type: %w: must not be empty", ErrInvalidArgument)
	}
	return ManifestType(s), nil
}

// ManifestID is an opaque identifier for a manifest instance. Unlike
// [ResourceName], it carries no AIP naming convention — manifests in a
// fulfillment are opaque payloads, not AIP resources.
type ManifestID string

// Manifest is an opaque declarative payload delivered to a target.
// ManifestType identifies what the payload represents (e.g.
// "api.kind.cluster"); Raw holds the actual content as opaque JSON.
type Manifest struct {
	ManifestType ManifestType
	ManifestID   ManifestID
	Raw          json.RawMessage
}

// FilterAcceptedManifests returns the subset of manifests whose
// [ManifestType] is accepted by the target. If the target has no
// AcceptedManifestTypes (unconstrained / legacy), all manifests are
// returned unchanged.
func FilterAcceptedManifests(target TargetInfo, manifests []Manifest) []Manifest {
	if len(target.AcceptedManifestTypes()) == 0 {
		return manifests
	}
	accepted := make(map[ManifestType]struct{}, len(target.AcceptedManifestTypes()))
	for _, mt := range target.AcceptedManifestTypes() {
		accepted[mt] = struct{}{}
	}
	out := make([]Manifest, 0, len(manifests))
	for _, m := range manifests {
		if _, ok := accepted[m.ManifestType]; ok {
			out = append(out, m)
		}
	}
	return out
}
