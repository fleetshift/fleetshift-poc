package domain

import (
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"
)

// Signature is a detached signature over a canonical content hash.
// Matches the hybrid PoC's Signature dataclass.
type Signature struct {
	SignerID       SubjectID
	PublicKey      []byte // raw EC public key bytes (from JWK x,y coordinates)
	ContentHash    []byte // SHA-256 of the canonical signed envelope
	SignatureBytes []byte // ECDSA-P256 ASN.1 signature
}

// OutputConstraint is a CEL predicate that delivery output must satisfy.
// Matches the hybrid PoC's OutputConstraint. Empty for Cap 7.
type OutputConstraint struct {
	Name       string
	Expression string
}

// Provenance carries the cryptographic proof that a user authorized
// a deployment. Stored on the deployment. Does NOT carry deployment
// content (that's on the Deployment itself — no duplication).
type Provenance struct {
	Sig                Signature
	KeyBinding         SigningKeyBinding
	ValidUntil         time.Time
	ExpectedGeneration Generation
	OutputConstraints  []OutputConstraint
}

// Attestation is the self-contained verification bundle assembled at
// delivery time. Matches the hybrid PoC's Attestation = Input + Output.
// The input side composes Provenance (proof) with deployment content.
type Attestation struct {
	Provenance        Provenance
	DeploymentID      DeploymentID
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
	Output            DeliveryOutput
}

// DeliveryOutput is the typed delivery action.
// Exactly one field is set.
type DeliveryOutput struct {
	PutManifests         *PutManifests
	RemoveByDeploymentId *RemoveByDeploymentId
}

// PutManifests delivers manifests to a target.
type PutManifests struct {
	Manifests []Manifest
	// TODO: Cap 8+ — ManifestSignature, Placement
}

// RemoveByDeploymentId removes a deployment from a target.
type RemoveByDeploymentId struct {
	DeploymentID DeploymentID
	// TODO: Cap 8+ — Placement
}

// HashIntent computes the SHA-256 digest of canonical envelope bytes.
func HashIntent(envelope []byte) []byte {
	return canonical.HashIntent(envelope)
}

// BuildSignedInputEnvelope constructs the canonical JSON envelope
// that gets hashed and signed. Delegates to [canonical.BuildSignedInputEnvelope]
// after converting domain types to canonical types.
func BuildSignedInputEnvelope(
	id DeploymentID,
	ms ManifestStrategySpec,
	ps PlacementStrategySpec,
	validUntil time.Time,
	constraints []OutputConstraint,
	expectedGeneration Generation,
) ([]byte, error) {
	return canonical.BuildSignedInputEnvelope(
		string(id),
		toCanonicalManifestStrategy(ms),
		toCanonicalPlacementStrategy(ps),
		validUntil,
		toCanonicalConstraints(constraints),
		int64(expectedGeneration),
	)
}

func toCanonicalManifestStrategy(ms ManifestStrategySpec) canonical.ManifestStrategy {
	out := canonical.ManifestStrategy{
		Type: string(ms.Type),
	}
	for _, m := range ms.Manifests {
		out.Manifests = append(out.Manifests, canonical.Manifest{
			ResourceType: string(m.ResourceType),
			Raw:          m.Raw,
		})
	}
	return out
}

func toCanonicalPlacementStrategy(ps PlacementStrategySpec) canonical.PlacementStrategy {
	out := canonical.PlacementStrategy{
		Type: string(ps.Type),
	}
	for _, t := range ps.Targets {
		out.Targets = append(out.Targets, string(t))
	}
	if ps.TargetSelector != nil {
		out.MatchLabels = ps.TargetSelector.MatchLabels
	}
	return out
}

func toCanonicalConstraints(constraints []OutputConstraint) []canonical.OutputConstraint {
	if len(constraints) == 0 {
		return nil
	}
	out := make([]canonical.OutputConstraint, len(constraints))
	for i, c := range constraints {
		out[i] = canonical.OutputConstraint{
			Name:       c.Name,
			Expression: c.Expression,
		}
	}
	return out
}
