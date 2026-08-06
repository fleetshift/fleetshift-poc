// Package client models the controlled client. It owns the user's continuity
// private key, performs OIDC enrollment, and signs purpose-separated delivery
// and rotation objects.
package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"

	"github.com/fleetshift/fleetshift-poc/poc/trust-model-v3/protocol"
)

type Config struct {
	TenantID     string
	Issuer       string
	OIDCClientID string
	RedirectURI  string
	HTTPClient   *http.Client
}

type Delivery struct {
	TargetID      string
	FulfillmentID string
	Generation    uint64
	Action        string
	Content       []byte
}

type Client struct {
	config Config

	privateKey  ed25519.PrivateKey
	publicKey   ed25519.PublicKey
	identityID  string
	subject     string
	state       protocol.ContinuityState
	stateDigest string
}

func New(config Config) (*Client, error) {
	if config.TenantID == "" {
		return nil, errors.New("tenant ID is required")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate continuity key: %w", err)
	}
	return &Client{
		config:     config,
		privateKey: privateKey,
		publicKey:  publicKey,
	}, nil
}

func (c *Client) Enroll(ctx context.Context, loginHint string) (protocol.EnrollmentPackage, error) {
	if c.identityID != "" {
		return protocol.EnrollmentPackage{}, errors.New("client is already enrolled")
	}
	if c.config.Issuer == "" || c.config.OIDCClientID == "" || c.config.RedirectURI == "" {
		return protocol.EnrollmentPackage{}, errors.New("issuer, OIDC client ID, and redirect URI are required for enrollment")
	}

	enrollmentID, err := randomToken()
	if err != nil {
		return protocol.EnrollmentPackage{}, fmt.Errorf("create enrollment ID: %w", err)
	}
	intent := protocol.EnrollmentIntent{
		Protocol:            protocol.EnrollmentProtocol,
		TenantID:            c.config.TenantID,
		ExpectedIssuer:      c.config.Issuer,
		EnrollmentClientID:  c.config.OIDCClientID,
		ContinuityKeyDigest: protocol.DigestBytes(c.publicKey),
		EnrollmentID:        enrollmentID,
	}
	nonce, err := protocol.EnrollmentNonce(intent)
	if err != nil {
		return protocol.EnrollmentPackage{}, fmt.Errorf("create enrollment nonce: %w", err)
	}
	identity, idToken, err := authenticateOIDC(ctx, oidcConfig{
		issuer:      c.config.Issuer,
		clientID:    c.config.OIDCClientID,
		redirectURI: c.config.RedirectURI,
		httpClient:  c.config.HTTPClient,
	}, nonce, loginHint)
	if err != nil {
		return protocol.EnrollmentPackage{}, fmt.Errorf("OIDC enrollment: %w", err)
	}
	proof, err := protocol.Sign(c.privateKey, "enrollment-proof-of-possession/v1", intent)
	if err != nil {
		return protocol.EnrollmentPackage{}, fmt.Errorf("sign enrollment intent: %w", err)
	}

	identityID := protocol.IdentityID(c.config.TenantID, identity.Issuer, identity.Subject)
	state := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            c.config.TenantID,
		IdentityID:          identityID,
		Generation:          0,
		ContinuityPublicKey: append([]byte(nil), c.publicKey...),
	}
	stateDigest, err := state.Digest()
	if err != nil {
		return protocol.EnrollmentPackage{}, fmt.Errorf("digest continuity state: %w", err)
	}
	c.identityID = identityID
	c.subject = identity.Subject
	c.state = state
	c.stateDigest = stateDigest

	return protocol.EnrollmentPackage{
		Intent:              intent,
		IdentityID:          identityID,
		ContinuityPublicKey: append([]byte(nil), c.publicKey...),
		ProofOfPossession:   proof,
		IDToken:             idToken,
	}, nil
}

// PrepareRotation authorizes a successor key without accepting a cutoff from
// the resource manager. The exact cutoff is the position at which this package
// is later serialized as a rotation marker in the tenant delivery log.
func (c *Client) PrepareRotation() (protocol.RotationPackage, *Client, error) {
	if c.identityID == "" {
		return protocol.RotationPackage{}, nil, errors.New("client is not enrolled")
	}
	newPublicKey, newPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return protocol.RotationPackage{}, nil, fmt.Errorf("generate successor continuity key: %w", err)
	}
	intent := protocol.RotationIntent{
		Protocol:               protocol.RotationProtocol,
		TenantID:               c.config.TenantID,
		IdentityID:             c.identityID,
		PreviousStateDigest:    c.stateDigest,
		NewGeneration:          c.state.Generation + 1,
		NewContinuityKeyDigest: protocol.DigestBytes(newPublicKey),
	}
	oldSignature, err := protocol.Sign(c.privateKey, "continuity-rotation-old-key/v1", intent)
	if err != nil {
		return protocol.RotationPackage{}, nil, fmt.Errorf("authorize rotation: %w", err)
	}
	newProof, err := protocol.Sign(newPrivateKey, "continuity-rotation-new-key/v1", intent)
	if err != nil {
		return protocol.RotationPackage{}, nil, fmt.Errorf("prove successor key possession: %w", err)
	}
	newState := protocol.ContinuityState{
		Protocol:            protocol.ContinuityStateProtocol,
		TenantID:            c.config.TenantID,
		IdentityID:          c.identityID,
		Generation:          intent.NewGeneration,
		ContinuityPublicKey: append([]byte(nil), newPublicKey...),
		PreviousStateDigest: c.stateDigest,
	}
	newStateDigest, err := newState.Digest()
	if err != nil {
		return protocol.RotationPackage{}, nil, fmt.Errorf("digest successor state: %w", err)
	}

	rotated := &Client{
		config:      c.config,
		privateKey:  newPrivateKey,
		publicKey:   newPublicKey,
		identityID:  c.identityID,
		subject:     c.subject,
		state:       newState,
		stateDigest: newStateDigest,
	}
	return protocol.RotationPackage{
		Intent:                 intent,
		NewContinuityPublicKey: append([]byte(nil), newPublicKey...),
		SignatureByOldKey:      oldSignature,
		ProofByNewKey:          newProof,
	}, rotated, nil
}

func (c *Client) SignDelivery(delivery Delivery) (protocol.SignedDelivery, error) {
	if c.identityID == "" || c.stateDigest == "" {
		return protocol.SignedDelivery{}, errors.New("client is not enrolled")
	}
	return c.SignDeliveryAs(c.identityID, c.stateDigest, delivery)
}

// SignDeliveryAs is exposed for adversarial tests. It demonstrates that an
// attacker can put any claimed identity into an attestation signed by the
// attacker's key, but cannot make it verify under that identity's enrolled key.
func (c *Client) SignDeliveryAs(identityID, stateDigest string, delivery Delivery) (protocol.SignedDelivery, error) {
	if delivery.TargetID == "" || delivery.FulfillmentID == "" || delivery.Action == "" {
		return protocol.SignedDelivery{}, errors.New("target, fulfillment, and action are required")
	}
	attestation := protocol.ContentAttestation{
		Protocol:           protocol.DeliveryProtocol,
		TenantID:           c.config.TenantID,
		IdentityID:         identityID,
		SigningStateDigest: stateDigest,
		TargetID:           delivery.TargetID,
		FulfillmentID:      delivery.FulfillmentID,
		Generation:         delivery.Generation,
		Action:             delivery.Action,
		ContentDigest:      protocol.DigestBytes(delivery.Content),
	}
	signature, err := protocol.Sign(c.privateKey, "content-delivery/v1", attestation)
	if err != nil {
		return protocol.SignedDelivery{}, fmt.Errorf("sign content attestation: %w", err)
	}
	return protocol.SignedDelivery{
		Attestation: attestation,
		Content:     append([]byte(nil), delivery.Content...),
		Signature:   signature,
	}, nil
}

func (c *Client) IdentityID() string {
	return c.identityID
}

func (c *Client) ContinuityStateDigest() string {
	return c.stateDigest
}

func (c *Client) ContinuityPublicKey() []byte {
	return append([]byte(nil), c.publicKey...)
}
