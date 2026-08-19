package directkey

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/poc/provenance-suites/protocol"
)

// Client is the direct-key/v1 client API. It holds one signing key pair and
// creates evidence for a single principal.
type Client struct {
	principal  protocol.Principal
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
}

// NewClient generates a signing key pair bound to principal.
func NewClient(principal protocol.Principal) (*Client, error) {
	canonical, err := principal.Canonicalize()
	if err != nil {
		return nil, err
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	return &Client{
		principal:  canonical,
		privateKey: privateKey,
		publicKey:  publicKey,
	}, nil
}

// Principal returns the client-bound canonical principal.
func (c *Client) Principal() protocol.Principal {
	return c.principal
}

// PublicKey returns a copy of the public key. Tests and enrollment use it;
// delivery evidence does not carry it.
func (c *Client) PublicKey() []byte {
	return append([]byte(nil), c.publicKey...)
}

// ProvenanceType implements protocol.ClientAPI.
func (c *Client) ProvenanceType() protocol.ProvenanceType {
	return protocol.ProvenanceTypeDirectKeyV1
}

// CreateEnrollment is the typed lifecycle operation that directly shares the
// public key and a proof of possession. It is not a generic RegisterKey.
//
// This is trust-on-first-use: the claimed principal is not authenticated by
// an issuer. Whoever first enrolls a subject under this naive profile owns
// the retained mapping until an explicit replacement protocol exists.
func (c *Client) CreateEnrollment() (protocol.TypedEvidence, error) {
	body := EnrollmentBody{
		Principal: c.principal,
		PublicKey: append([]byte(nil), c.publicKey...),
	}
	proof, err := sign(c.privateKey, purposeEnrollmentProof, enrollmentProofMaterial(body.Principal, body.PublicKey))
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
		MediaType:      MediaTypeEnrollment,
		Bytes:          raw,
	}, nil
}

// CreateEvidence implements protocol.ClientAPI. The resulting evidence carries
// the signature and user reference, not the public key.
func (c *Client) CreateEvidence(_ context.Context, assertion protocol.TypedAssertion) (protocol.TypedEvidence, error) {
	if assertion.ContentType == "" || len(assertion.Bytes) == 0 {
		return protocol.TypedEvidence{}, errors.New("assertion content type and bytes are required")
	}
	contentDigest, err := assertion.Digest()
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	signed := signatureMaterial(c.principal, assertion.ContentType, contentDigest)
	signature, err := sign(c.privateKey, purposeAssertion, signed)
	if err != nil {
		return protocol.TypedEvidence{}, fmt.Errorf("sign assertion: %w", err)
	}
	body := SignatureBody{
		Principal:     c.principal,
		ContentType:   assertion.ContentType,
		ContentDigest: contentDigest,
		Signature:     signature,
	}
	raw, err := encodeJSON(body)
	if err != nil {
		return protocol.TypedEvidence{}, err
	}
	return protocol.TypedEvidence{
		ProvenanceType: protocol.ProvenanceTypeDirectKeyV1,
		MediaType:      MediaTypeSignature,
		Bytes:          raw,
	}, nil
}

func enrollmentProofMaterial(principal protocol.Principal, publicKey []byte) any {
	return struct {
		Principal protocol.Principal `json:"principal"`
		PublicKey []byte             `json:"public_key"`
	}{Principal: principal, PublicKey: publicKey}
}

func signatureMaterial(principal protocol.Principal, contentType protocol.ContentType, contentDigest protocol.Digest) any {
	return struct {
		Principal     protocol.Principal   `json:"principal"`
		ContentType   protocol.ContentType `json:"content_type"`
		ContentDigest protocol.Digest      `json:"content_digest"`
	}{Principal: principal, ContentType: contentType, ContentDigest: contentDigest}
}
