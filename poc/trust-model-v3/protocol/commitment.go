package protocol

import (
	"errors"
	"fmt"
)

func DeliveryPackageDigest(delivery SignedDelivery) (string, error) {
	return ObjectDigest(delivery)
}

func CommitmentFromDelivery(delivery SignedDelivery) (DeliveryCommitment, error) {
	attestation := delivery.Attestation
	if attestation.DeliveryID == "" || attestation.FulfillmentID == "" || attestation.TargetID == "" {
		return DeliveryCommitment{}, errors.New("delivery identity, fulfillment, and target are required")
	}
	digest, err := DeliveryPackageDigest(delivery)
	if err != nil {
		return DeliveryCommitment{}, fmt.Errorf("digest delivery package: %w", err)
	}
	return DeliveryCommitment{
		TenantID:              attestation.TenantID,
		DeliveryID:            attestation.DeliveryID,
		FulfillmentID:         attestation.FulfillmentID,
		TargetID:              attestation.TargetID,
		Generation:            attestation.Generation,
		Action:                attestation.Action,
		SigningIdentityID:     attestation.IdentityID,
		SigningStateDigest:    attestation.SigningStateDigest,
		DeliveryPackageDigest: digest,
	}, nil
}

func MarkerFromRotation(rotation RotationPackage) (KeyRotationMarker, error) {
	digest, err := RotationAuthorizationDigest(rotation)
	if err != nil {
		return KeyRotationMarker{}, fmt.Errorf("digest rotation authorization: %w", err)
	}
	return KeyRotationMarker{
		TenantID:                    rotation.Authorization.TenantID,
		IdentityID:                  rotation.Authorization.IdentityID,
		RotationAuthorizationDigest: digest,
	}, nil
}

func NewDeliveryLogRecord(index uint64, delivery SignedDelivery) (DeliveryRecord, error) {
	commitment, err := CommitmentFromDelivery(delivery)
	if err != nil {
		return DeliveryRecord{}, err
	}
	record, err := NewDeliveryRecord(index, DeliveryLogEvent{
		Kind:       DeliveryLogEventDelivery,
		Commitment: &commitment,
	})
	if err != nil {
		return DeliveryRecord{}, err
	}
	cloned := cloneSignedDelivery(delivery)
	record.Delivery = &cloned
	return record, nil
}

func NewRotationLogRecord(index uint64, rotation RotationPackage) (DeliveryRecord, error) {
	marker, err := MarkerFromRotation(rotation)
	if err != nil {
		return DeliveryRecord{}, err
	}
	record, err := NewDeliveryRecord(index, DeliveryLogEvent{
		Kind:   DeliveryLogEventRotation,
		Marker: &marker,
	})
	if err != nil {
		return DeliveryRecord{}, err
	}
	cloned := cloneRotationPackage(rotation)
	record.Rotation = &cloned
	return record, nil
}

func VerifyDeliveryMatchesCommitment(delivery SignedDelivery, commitment DeliveryCommitment) error {
	got, err := CommitmentFromDelivery(delivery)
	if err != nil {
		return err
	}
	if got != commitment {
		return errors.New("signed delivery does not recompute the log commitment")
	}
	if DigestBytes(delivery.Content) != delivery.Attestation.ContentDigest {
		return errors.New("delivered content does not match signed digest")
	}
	return nil
}

func VerifyRotationMatchesMarker(rotation RotationPackage, marker KeyRotationMarker) error {
	got, err := MarkerFromRotation(rotation)
	if err != nil {
		return err
	}
	if got != marker {
		return errors.New("rotation package does not recompute the log marker")
	}
	return nil
}

func cloneSignedDelivery(in SignedDelivery) SignedDelivery {
	out := in
	out.Content = append([]byte(nil), in.Content...)
	out.Signature = append([]byte(nil), in.Signature...)
	return out
}

func cloneRotationPackage(in RotationPackage) RotationPackage {
	out := in
	out.NewContinuityPublicKey = append([]byte(nil), in.NewContinuityPublicKey...)
	out.SignatureByOldKey = append([]byte(nil), in.SignatureByOldKey...)
	out.ProofByNewKey = append([]byte(nil), in.ProofByNewKey...)
	return out
}
