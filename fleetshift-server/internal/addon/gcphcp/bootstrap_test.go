package gcphcp_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

func TestGuestTargetID(t *testing.T) {
	targetID := gcphcp.GuestTargetID("test-cluster")
	if targetID != "k8s-test-cluster" {
		t.Fatalf("target ID = %q, want %q", targetID, "k8s-test-cluster")
	}
}

func TestDeliverySecretRef(t *testing.T) {
	ref := gcphcp.DeliverySecretRef(gcphcp.GuestTargetID("test-cluster"))
	if ref != "targets/k8s-test-cluster/sa-token" {
		t.Errorf("ref = %q", ref)
	}
}

func TestGuestTargetID_DiffersFromSeededProviderTarget(t *testing.T) {
	seededProviderID := "gcphcp-provider"
	guestTargetID := string(gcphcp.GuestTargetID("test-cluster"))

	if guestTargetID == seededProviderID {
		t.Fatalf("guest target ID %q unexpectedly matches provider target ID", guestTargetID)
	}
}
