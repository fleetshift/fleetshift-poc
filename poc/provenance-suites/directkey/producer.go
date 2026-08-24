package directkey

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

var _ protocol.ProducerAPI = (*Producer)(nil)

// Producer is the direct-key/v1 producer API. It holds one signing key pair and
// creates evidence for a single principal.
type Producer struct {
	principal  protocol.Principal
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
}

// NewProducer generates a signing key pair bound to principal.
func NewProducer(principal protocol.Principal) (*Producer, error) {
	canonical, err := principal.Canonicalize()
	if err != nil {
		return nil, err
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	return &Producer{
		principal:  canonical,
		privateKey: privateKey,
		publicKey:  publicKey,
	}, nil
}

// Principal returns the producer-bound canonical principal.
func (p *Producer) Principal() protocol.Principal {
	return p.principal
}

// PublicKey returns a copy of the public key. Tests and enrollment use it;
// delivery evidence does not carry it.
func (p *Producer) PublicKey() []byte {
	return append([]byte(nil), p.publicKey...)
}

// ProvenanceType implements protocol.ProducerAPI.
func (p *Producer) ProvenanceType() protocol.ProvenanceType {
	return protocol.ProvenanceTypeDirectKeyV1
}

// CreateEnrollment is the typed lifecycle operation that directly shares the
// public key and a proof of possession. It is not a generic RegisterKey.
//
// This is trust-on-first-use: the claimed principal is not authenticated by
// an issuer. Whoever first enrolls a subject under this naive profile owns
// the retained mapping until an explicit replacement protocol exists.
func (p *Producer) CreateEnrollment() (protocol.TypedEvidence, error) {
	body := EnrollmentBody{
		Principal: p.principal,
		PublicKey: append([]byte(nil), p.publicKey...),
	}
	proof, err := sign(p.privateKey, purposeEnrollmentProof, enrollmentProofMaterial(body.Principal, body.PublicKey))
	if err != nil {
		return protocol.TypedEvidence{}, fmt.Errorf("sign enrollment proof of possession: %w", err)
	}
	body.ProofOfPossession = proof
	raw, err := encodeJSON(body)
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return protocol.TypedEvidence{
		ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
		Encoded: protocol.Encoded{
			MediaType: MediaTypeEnrollment,
			Bytes:     raw,
		},
	}, nil
}

// CreateEvidence implements protocol.ProducerAPI. The resulting evidence
// carries the inner statement, a signature, and a user reference, not the
// public key.
func (p *Producer) CreateEvidence(_ context.Context, assertion protocol.TypedAssertion) (protocol.TypedEvidence, error) {
	if assertion.PredicateType == "" || len(assertion.Bytes) == 0 {
		return protocol.TypedEvidence{}, errors.New("assertion predicate type and bytes are required")
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	signed := signatureMaterial(p.principal, assertion.PredicateType, contentDigest)
	signature, err := sign(p.privateKey, purposeAssertion, signed)
	if err != nil {
		return protocol.TypedEvidence{}, fmt.Errorf("sign assertion: %w", err)
	}
	body := SignatureBody{
		Principal: p.principal,
		Assertion: protocol.TypedAssertion{
			PredicateType: assertion.PredicateType,
			Bytes:         append([]byte(nil), assertion.Bytes...),
		},
		Signature: signature,
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return protocol.TypedEvidence{
		ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
		Encoded: protocol.Encoded{
			MediaType: MediaTypeSignature,
			Bytes:     raw,
		},
	}, nil
}

func enrollmentProofMaterial(principal protocol.Principal, publicKey []byte) any {
	return struct {
		Principal protocol.Principal `json:"principal"`
		PublicKey []byte             `json:"public_key"`
	}{Principal: principal, PublicKey: publicKey}
}

func signatureMaterial(principal protocol.Principal, predicateType protocol.PredicateType, contentDigest protocol.Digest) any {
	return struct {
		Principal     protocol.Principal     `json:"principal"`
		PredicateType protocol.PredicateType `json:"predicate_type"`
		ContentDigest protocol.Digest        `json:"content_digest"`
	}{Principal: principal, PredicateType: predicateType, ContentDigest: contentDigest}
}
