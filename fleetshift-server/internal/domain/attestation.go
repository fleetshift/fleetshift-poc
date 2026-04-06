package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"
)

// Signature is a detached signature over a canonical content hash.
// Matches the hybrid PoC's Signature dataclass.
type Signature struct {
	Signer         FederatedIdentity
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
//
// The full key binding is NOT stored here; the issuer needed for
// delivery-time key binding lookup is carried inside Sig.Signer.
type Provenance struct {
	Sig                Signature
	ValidUntil         time.Time
	ExpectedGeneration Generation
	OutputConstraints  []OutputConstraint
}

// DeploymentContent groups the identity and strategy fields that the
// signer authorizes. Matches the hybrid PoC's DeploymentContent.
type DeploymentContent struct {
	DeploymentID      DeploymentID
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
}

// SignedInput is a first-class composition of content + proof,
// assembled at delivery time from stored Provenance plus the
// looked-up key binding. Matches the hybrid PoC's SignedInput.
type SignedInput struct {
	Content            DeploymentContent
	Sig                Signature
	KeyBinding         SigningKeyBinding // full bundle, looked up at assembly time
	ValidUntil         time.Time
	OutputConstraints  []OutputConstraint
	ExpectedGeneration Generation
}

// Attestation is the self-contained verification bundle assembled at
// delivery time. Matches the hybrid PoC's Attestation = Input + Output.
type Attestation struct {
	Input  SignedInput
	Output DeliveryOutput // one of [*PutManifests] or [*RemoveByDeploymentId]
}

// DeliveryOutput is a sealed sum type for delivery actions.
// Valid implementations are [*PutManifests] and [*RemoveByDeploymentId].
type DeliveryOutput interface {
	deliveryOutput() // sealed
}

// PutManifests delivers manifests to a target.
type PutManifests struct {
	Manifests []Manifest
	// TODO: Cap 8+ — ManifestSignature, Placement
}

func (*PutManifests) deliveryOutput() {}

// RemoveByDeploymentId removes a deployment from a target.
type RemoveByDeploymentId struct {
	DeploymentID DeploymentID
	// TODO: Cap 8+ — Placement
}

func (*RemoveByDeploymentId) deliveryOutput() {}

// attestationJSON is the wire representation used by Attestation's custom
// JSON codec. A discriminator field (OutputType) tells the decoder which
// concrete DeliveryOutput variant to instantiate.
type attestationJSON struct {
	Input                SignedInput            `json:"Input"`
	OutputType           string                 `json:"OutputType"`
	PutManifests         *PutManifests          `json:"PutManifests,omitempty"`
	RemoveByDeploymentId *RemoveByDeploymentId  `json:"RemoveByDeploymentId,omitempty"`
}

func (a Attestation) MarshalJSON() ([]byte, error) {
	j := attestationJSON{Input: a.Input}
	switch o := a.Output.(type) {
	case *PutManifests:
		j.OutputType = "PutManifests"
		j.PutManifests = o
	case *RemoveByDeploymentId:
		j.OutputType = "RemoveByDeploymentId"
		j.RemoveByDeploymentId = o
	case nil:
		// no output
	default:
		return nil, fmt.Errorf("attestation: unknown DeliveryOutput type %T", a.Output)
	}
	return json.Marshal(j)
}

func (a *Attestation) UnmarshalJSON(data []byte) error {
	var j attestationJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	a.Input = j.Input
	switch j.OutputType {
	case "PutManifests":
		a.Output = j.PutManifests
	case "RemoveByDeploymentId":
		a.Output = j.RemoveByDeploymentId
	case "":
		a.Output = nil
	default:
		return fmt.Errorf("attestation: unknown OutputType %q", j.OutputType)
	}
	return nil
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
