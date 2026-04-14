// Package canonical provides the deterministic signed envelope
// construction for attestation signing and verification. Both the CLI
// (signer) and server (verifier) import this package to produce
// byte-identical canonical JSON from deployment parameters.
package canonical

import (
	"crypto/sha256"
	"encoding/json"
	"sort"
	"time"
)

// ManifestStrategy is the canonical representation of a manifest strategy
// in the signed envelope. Mirrors the domain type without importing it.
type ManifestStrategy struct {
	Type      string
	Manifests []Manifest
}

// Manifest is a single manifest entry for canonical serialization.
type Manifest struct {
	ResourceType string
	Raw          json.RawMessage
}

// PlacementStrategy is the canonical representation of a placement strategy.
type PlacementStrategy struct {
	Type           string
	Targets        []string
	MatchLabels    map[string]string
}

// OutputConstraint is a CEL predicate signed with the input.
type OutputConstraint struct {
	Name       string
	Expression string
}

// BuildSignedInputEnvelope constructs the canonical JSON envelope
// that gets hashed and signed. Matches the hybrid PoC's
// signed_input_envelope function. Both signers and verifiers call
// this with identical inputs to produce identical bytes.
//
// The envelope structure is:
//
//	{
//	    "content": {"deployment_id": ..., "manifest_strategy": ..., "placement_strategy": ...},
//	    "output_constraints": [...],
//	    "valid_until": <unix_timestamp_float>,
//	    "expected_generation": <int>  // omitted when zero
//	}
func BuildSignedInputEnvelope(
	deploymentID string,
	ms ManifestStrategy,
	ps PlacementStrategy,
	validUntil time.Time,
	constraints []OutputConstraint,
	expectedGeneration int64,
) ([]byte, error) {
	content := envelopeContent{
		DeploymentID:      deploymentID,
		ManifestStrategy:  marshalManifestStrategy(ms),
		PlacementStrategy: marshalPlacementStrategy(ps),
	}

	env := signedInputEnvelope{
		Content:           content,
		OutputConstraints: marshalOutputConstraints(constraints),
		ValidUntil:        float64(validUntil.Unix()),
	}
	if expectedGeneration != 0 {
		env.ExpectedGeneration = &expectedGeneration
	}

	return json.Marshal(env)
}

// HashIntent computes the SHA-256 digest of canonical envelope bytes.
func HashIntent(envelope []byte) []byte {
	h := sha256.Sum256(envelope)
	return h[:]
}

type signedInputEnvelope struct {
	Content            envelopeContent      `json:"content"`
	OutputConstraints  []envelopeConstraint `json:"output_constraints"`
	ValidUntil         float64              `json:"valid_until"`
	ExpectedGeneration *int64               `json:"expected_generation,omitempty"`
}

type envelopeContent struct {
	DeploymentID      string                    `json:"deployment_id"`
	ManifestStrategy  envelopeManifestStrategy  `json:"manifest_strategy"`
	PlacementStrategy envelopePlacementStrategy `json:"placement_strategy"`
}

type envelopeManifestStrategy struct {
	Type      string          `json:"type"`
	Manifests json.RawMessage `json:"manifests,omitempty"`
}

type envelopePlacementStrategy struct {
	Type        string            `json:"type"`
	Targets     []string          `json:"targets,omitempty"`
	MatchLabels map[string]string `json:"match_labels,omitempty"`
}

type envelopeConstraint struct {
	Expression string `json:"expression"`
	Name       string `json:"name"`
}

func marshalManifestStrategy(ms ManifestStrategy) envelopeManifestStrategy {
	out := envelopeManifestStrategy{
		Type: ms.Type,
	}
	if len(ms.Manifests) > 0 {
		type manifestDoc struct {
			ResourceType string          `json:"resource_type"`
			Content      json.RawMessage `json:"content"`
		}
		docs := make([]manifestDoc, len(ms.Manifests))
		for i, m := range ms.Manifests {
			docs[i] = manifestDoc{
				ResourceType: m.ResourceType,
				Content:      m.Raw,
			}
		}
		raw, _ := json.Marshal(docs)
		out.Manifests = raw
	}
	return out
}

func marshalPlacementStrategy(ps PlacementStrategy) envelopePlacementStrategy {
	out := envelopePlacementStrategy{
		Type: ps.Type,
	}
	if len(ps.Targets) > 0 {
		out.Targets = ps.Targets
	}
	if len(ps.MatchLabels) > 0 {
		out.MatchLabels = ps.MatchLabels
	}
	return out
}

func marshalOutputConstraints(constraints []OutputConstraint) []envelopeConstraint {
	if len(constraints) == 0 {
		return []envelopeConstraint{}
	}
	docs := make([]envelopeConstraint, len(constraints))
	for i, c := range constraints {
		docs[i] = envelopeConstraint{
			Expression: c.Expression,
			Name:       c.Name,
		}
	}
	sort.Slice(docs, func(i, j int) bool {
		a, _ := json.Marshal(docs[i])
		b, _ := json.Marshal(docs[j])
		return string(a) < string(b)
	})
	return docs
}
