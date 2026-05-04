package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/canonical"
)

// Signature is a detached signature over a canonical content hash.
type Signature struct {
	Signer         FederatedIdentity
	ContentHash    []byte // SHA-256 of the canonical signed envelope
	SignatureBytes []byte // ECDSA-P256 ASN.1 signature
}

// OutputConstraint is a CEL predicate that delivery output must satisfy.
// Matches the hybrid PoC's OutputConstraint. Empty for Cap 7.
type OutputConstraint struct {
	Name       string
	Expression string
}

// InputContent is a typed union for the content that a signer
// authorizes. Matches the hybrid attestation PoC's InputContent
// protocol. Valid implementations are [DeploymentContent] and (in
// OME-44) ManagedResourceContent.
type InputContent interface {
	ContentID() string
	ContentType() string
	inputContent() // sealed
}

// DeploymentContent groups the identity and strategy fields that the
// signer authorizes. Matches the hybrid PoC's DeploymentContent.
type DeploymentContent struct {
	DeploymentID      DeploymentID
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
}

func (c DeploymentContent) ContentID() string   { return string(c.DeploymentID) }
func (c DeploymentContent) ContentType() string  { return "deployment" }
func (DeploymentContent) inputContent()          {}

// Provenance carries the cryptographic proof that a user authorized
// a fulfillment. Stored on the [Fulfillment] and composed into
// [SignedInput] at delivery time. Content carries the typed
// [InputContent] that was signed.
type Provenance struct {
	Content            InputContent // what the user signed (typed)
	Sig                Signature
	ValidUntil         time.Time
	ExpectedGeneration Generation
	OutputConstraints  []OutputConstraint
}

// provenanceJSON is the wire representation for [Provenance]. The
// Content field uses a discriminated union (ContentType + typed field)
// for polymorphic InputContent serialization.
type provenanceJSON struct {
	ContentType        string             `json:"ContentType"`
	DeploymentContent  *DeploymentContent `json:"DeploymentContent,omitempty"`
	Sig                Signature          `json:"Sig"`
	ValidUntil         time.Time          `json:"ValidUntil"`
	ExpectedGeneration Generation         `json:"ExpectedGeneration"`
	OutputConstraints  []OutputConstraint `json:"OutputConstraints,omitempty"`
}

// MarshalJSON implements [json.Marshaler] for Provenance.
func (p Provenance) MarshalJSON() ([]byte, error) {
	j := provenanceJSON{
		Sig:                p.Sig,
		ValidUntil:         p.ValidUntil,
		ExpectedGeneration: p.ExpectedGeneration,
		OutputConstraints:  p.OutputConstraints,
	}
	switch c := p.Content.(type) {
	case DeploymentContent:
		j.ContentType = "deployment"
		j.DeploymentContent = &c
	case *DeploymentContent:
		j.ContentType = "deployment"
		j.DeploymentContent = c
	case nil:
		// no content
	default:
		return nil, fmt.Errorf("provenance: unknown InputContent type %T", p.Content)
	}
	return json.Marshal(j)
}

// UnmarshalJSON implements [json.Unmarshaler] for Provenance.
func (p *Provenance) UnmarshalJSON(data []byte) error {
	var j provenanceJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	p.Sig = j.Sig
	p.ValidUntil = j.ValidUntil
	p.ExpectedGeneration = j.ExpectedGeneration
	p.OutputConstraints = j.OutputConstraints
	switch j.ContentType {
	case "deployment":
		if j.DeploymentContent != nil {
			p.Content = *j.DeploymentContent
		}
	case "":
		p.Content = nil
	default:
		return fmt.Errorf("provenance: unknown ContentType %q", j.ContentType)
	}
	return nil
}

// SignerAssertion carries the minimal data a delivery agent needs
// to independently resolve a signer's public keys from an external
// registry.
type SignerAssertion struct {
	IdentityToken   RawToken        // enrollment ID token (agent re-verifies via JWKS)
	RegistryID      KeyRegistryID   // which registry to query
	RegistrySubject RegistrySubject // derived from CEL mapping (agent re-derives to confirm)
}

// SignedInput is a first-class composition of content + proof,
// assembled at delivery time from stored Provenance plus the signer
// assertion derived from the enrollment record.
type SignedInput struct {
	Provenance Provenance
	Signer     SignerAssertion
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
	Input                SignedInput           `json:"Input"`
	OutputType           string                `json:"OutputType"`
	PutManifests         *PutManifests         `json:"PutManifests,omitempty"`
	RemoveByDeploymentId *RemoveByDeploymentId `json:"RemoveByDeploymentId,omitempty"`
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
