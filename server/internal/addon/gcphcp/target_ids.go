package gcphcp

import "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"

// GuestTargetID returns the ID of the emitted Kubernetes target for a
// provisioned hosted cluster. This must remain distinct from the seeded
// gcphcp self-target ID used for provisioning.
func GuestTargetID(clusterName string) domain.TargetID {
	return domain.TargetID("k8s-" + clusterName)
}
