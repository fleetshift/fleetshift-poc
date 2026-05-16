package gcphcp

import (
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// postProvisionRegistrationError marks failures that happen after the hosted
// cluster is provisioned and management-plane ready, but before the guest
// cluster can be registered as a FleetShift target.
type postProvisionRegistrationError struct {
	err error
}

func (e *postProvisionRegistrationError) Error() string {
	return e.err.Error()
}

func (e *postProvisionRegistrationError) Unwrap() error {
	return e.err
}

func newPostProvisionRegistrationError(err error) error {
	if err == nil || isPostProvisionRegistrationError(err) {
		return err
	}
	return &postProvisionRegistrationError{err: err}
}

func isPostProvisionRegistrationError(err error) bool {
	var target *postProvisionRegistrationError
	return errors.As(err, &target)
}

func deliveryResultForReconcileError(err error) domain.DeliveryResult {
	if isPostProvisionRegistrationError(err) {
		return domain.DeliveryResult{
			State:   domain.DeliveryStateFailed,
			Message: fmt.Sprintf("cluster provisioned and management-plane ready, but guest target registration did not complete: %v", err),
		}
	}
	return domain.DeliveryResult{
		State:   domain.DeliveryStateFailed,
		Message: fmt.Sprintf("cluster provisioning failed: %v", err),
	}
}
