package protocol

import (
	"errors"
	"fmt"
)

// ProvisionedTrustManifest returns the bootstrap tenant trust manifest used
// by this POC. Manifest rotation is out of scope.
func ProvisionedTrustManifest(tenantID, issuer, enrollmentClientID string) TenantTrustManifest {
	return TenantTrustManifest{
		Protocol:                      TrustManifestProtocol,
		TenantID:                      tenantID,
		Version:                       1,
		TrustUpdatePolicy:             TrustUpdatePolicyProvisioned,
		OIDCIssuer:                    issuer,
		EnrollmentClientID:            enrollmentClientID,
		PermittedIDTokenAlgorithms:    []string{"ES256"},
		PermittedContinuityAlgorithms: []string{"Ed25519"},
	}
}

func (m TenantTrustManifest) MatchesEnrollmentIntent(intent EnrollmentIntent) error {
	if intent.Protocol != EnrollmentProtocol {
		return fmt.Errorf("enrollment protocol %q, want %q", intent.Protocol, EnrollmentProtocol)
	}
	if intent.TenantID != m.TenantID {
		return errors.New("enrollment tenant does not match trust manifest")
	}
	if intent.ExpectedIssuer != m.OIDCIssuer || intent.EnrollmentClientID != m.EnrollmentClientID {
		return errors.New("enrollment issuer or client does not match trust manifest")
	}
	return nil
}

// EnrolledContinuityState constructs generation-0 state from an enrollment
// public key. Recovery policy is empty in this POC.
func EnrolledContinuityState(tenantID, identityID string, publicKey []byte) (ContinuityState, string, error) {
	state := ContinuityState{
		Protocol:            ContinuityStateProtocol,
		TenantID:            tenantID,
		IdentityID:          identityID,
		Generation:          0,
		ContinuityPublicKey: append([]byte(nil), publicKey...),
		ContinuityKeyDigest: DigestBytes(publicKey),
	}
	digest, err := state.Digest()
	if err != nil {
		return ContinuityState{}, "", err
	}
	return state, digest, nil
}

type rotationTransitionMaterial struct {
	Protocol               string `json:"protocol"`
	TenantID               string `json:"tenant_id"`
	IdentityID             string `json:"identity_id"`
	PreviousStateDigest    string `json:"previous_state_digest"`
	NewGeneration          uint64 `json:"new_generation"`
	NewContinuityKeyDigest string `json:"new_continuity_key_digest"`
	RecoveryPolicyDigest   string `json:"recovery_policy_digest,omitempty"`
}

// SuccessorContinuityState reconstructs the design's successor state from a
// predecessor and new public key, then returns the cutoff-free authorization
// that binds both state digests.
func SuccessorContinuityState(previous ContinuityState, previousDigest string, newPublicKey []byte) (ContinuityState, RotationAuthorization, error) {
	if previousDigest == "" {
		return ContinuityState{}, RotationAuthorization{}, errors.New("previous state digest is required")
	}
	keyDigest := DigestBytes(newPublicKey)
	newGeneration := previous.Generation + 1
	transitionDigest, err := ObjectDigest(rotationTransitionMaterial{
		Protocol:               RotationProtocol,
		TenantID:               previous.TenantID,
		IdentityID:             previous.IdentityID,
		PreviousStateDigest:    previousDigest,
		NewGeneration:          newGeneration,
		NewContinuityKeyDigest: keyDigest,
		RecoveryPolicyDigest:   previous.RecoveryPolicyDigest,
	})
	if err != nil {
		return ContinuityState{}, RotationAuthorization{}, fmt.Errorf("digest rotation transition: %w", err)
	}
	state := ContinuityState{
		Protocol:             ContinuityStateProtocol,
		TenantID:             previous.TenantID,
		IdentityID:           previous.IdentityID,
		Generation:           newGeneration,
		ContinuityPublicKey:  append([]byte(nil), newPublicKey...),
		ContinuityKeyDigest:  keyDigest,
		RecoveryPolicyDigest: previous.RecoveryPolicyDigest,
		PreviousStateDigest:  previousDigest,
		TransitionDigest:     transitionDigest,
	}
	digest, err := state.Digest()
	if err != nil {
		return ContinuityState{}, RotationAuthorization{}, err
	}
	return state, RotationAuthorization{
		Protocol:            RotationProtocol,
		TenantID:            previous.TenantID,
		IdentityID:          previous.IdentityID,
		PreviousStateDigest: previousDigest,
		NewStateDigest:      digest,
	}, nil
}

// ReconstructSuccessorState rebuilds the successor ContinuityState from a
// rotation package. NewGeneration and the new public key are bound by
// Authorization.NewStateDigest.
func ReconstructSuccessorState(rotation RotationPackage) (ContinuityState, string, error) {
	authorization := rotation.Authorization
	if authorization.Protocol != RotationProtocol || authorization.IdentityID == "" || authorization.PreviousStateDigest == "" {
		return ContinuityState{}, "", errors.New("rotation authorization is malformed")
	}
	keyDigest := DigestBytes(rotation.NewContinuityPublicKey)
	transitionDigest, err := ObjectDigest(rotationTransitionMaterial{
		Protocol:               RotationProtocol,
		TenantID:               authorization.TenantID,
		IdentityID:             authorization.IdentityID,
		PreviousStateDigest:    authorization.PreviousStateDigest,
		NewGeneration:          rotation.NewGeneration,
		NewContinuityKeyDigest: keyDigest,
		RecoveryPolicyDigest:   "",
	})
	if err != nil {
		return ContinuityState{}, "", fmt.Errorf("digest rotation transition: %w", err)
	}
	state := ContinuityState{
		Protocol:            ContinuityStateProtocol,
		TenantID:            authorization.TenantID,
		IdentityID:          authorization.IdentityID,
		Generation:          rotation.NewGeneration,
		ContinuityPublicKey: append([]byte(nil), rotation.NewContinuityPublicKey...),
		ContinuityKeyDigest: keyDigest,
		PreviousStateDigest: authorization.PreviousStateDigest,
		TransitionDigest:    transitionDigest,
	}
	digest, err := state.Digest()
	if err != nil {
		return ContinuityState{}, "", err
	}
	if digest != authorization.NewStateDigest {
		return ContinuityState{}, "", errors.New("rotation authorization does not bind the reconstructed successor state")
	}
	return state, digest, nil
}

func VerifyEnrollmentProofOfPossession(enrollment EnrollmentPackage) error {
	intent := enrollment.Intent
	if intent.Protocol != EnrollmentProtocol {
		return fmt.Errorf("enrollment protocol %q, want %q", intent.Protocol, EnrollmentProtocol)
	}
	if len(enrollment.ContinuityPublicKey) == 0 || intent.ContinuityKeyDigest == "" {
		return errors.New("enrollment continuity key is missing")
	}
	if DigestBytes(enrollment.ContinuityPublicKey) != intent.ContinuityKeyDigest {
		return errors.New("continuity public key does not match nonce-bound digest")
	}
	if err := Verify(enrollment.ContinuityPublicKey, "enrollment-proof-of-possession/v1", intent, enrollment.ProofOfPossession); err != nil {
		return fmt.Errorf("continuity-key proof of possession: %w", err)
	}
	return nil
}

func VerifyRotationAuthorization(rotation RotationPackage, oldPublicKey []byte) error {
	authorization := rotation.Authorization
	if authorization.Protocol != RotationProtocol {
		return fmt.Errorf("rotation protocol %q, want %q", authorization.Protocol, RotationProtocol)
	}
	if len(rotation.NewContinuityPublicKey) == 0 {
		return errors.New("successor continuity key is missing")
	}
	if err := Verify(oldPublicKey, "continuity-rotation-old-key/v1", authorization, rotation.SignatureByOldKey); err != nil {
		return fmt.Errorf("old key did not authorize rotation: %w", err)
	}
	if err := Verify(rotation.NewContinuityPublicKey, "continuity-rotation-new-key/v1", authorization, rotation.ProofByNewKey); err != nil {
		return fmt.Errorf("new key did not prove possession: %w", err)
	}
	return nil
}

func VerifyDeliverySignature(delivery SignedDelivery, publicKey []byte) error {
	if err := Verify(publicKey, "content-delivery/v1", delivery.Attestation, delivery.Signature); err != nil {
		return fmt.Errorf("delivery signature: %w", err)
	}
	return nil
}

func ContinuityPublicKeyFromEvent(event KeyEvent) ([]byte, error) {
	switch event.Kind {
	case KeyEventEnrollment:
		if event.Enrollment == nil {
			return nil, errors.New("enrollment event has no package")
		}
		return append([]byte(nil), event.Enrollment.ContinuityPublicKey...), nil
	case KeyEventRotation:
		if event.Rotation == nil {
			return nil, errors.New("rotation event has no package")
		}
		return append([]byte(nil), event.Rotation.NewContinuityPublicKey...), nil
	default:
		return nil, fmt.Errorf("unknown key event kind %q", event.Kind)
	}
}

func RotationAuthorizationDigest(rotation RotationPackage) (string, error) {
	return ObjectDigest(rotation)
}
