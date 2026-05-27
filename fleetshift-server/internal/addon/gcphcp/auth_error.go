package gcphcp

import "errors"

// authExpiredError marks failures caused by expired or invalidated
// credentials during a reconciliation pass. When this error propagates
// to deliveryResultForReconcileError it produces DeliveryStateAuthFailed
// instead of DeliveryStateFailed, which causes the platform to
// transition the fulfillment to FulfillmentStatePausedAuth.
//
// HTTP 401 (Unauthorized) from any backend (CLS, IAM, STS) and OAuth
// "invalid_grant" from the STS endpoint are wrapped in this type.
// HTTP 403 (Forbidden) is NOT wrapped because it indicates a
// permission/configuration issue that fresh user credentials will not
// resolve.
type authExpiredError struct {
	err error
}

func (e *authExpiredError) Error() string {
	return e.err.Error()
}

func (e *authExpiredError) Unwrap() error {
	return e.err
}

func newAuthExpiredError(err error) error {
	if err == nil || IsAuthExpiredError(err) {
		return err
	}
	return &authExpiredError{err: err}
}

// IsAuthExpiredError reports whether err (or any error in its chain)
// is an authExpiredError.
func IsAuthExpiredError(err error) bool {
	var target *authExpiredError
	return errors.As(err, &target)
}
