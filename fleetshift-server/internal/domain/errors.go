package domain

import (
	"errors"
	"strings"
)

var (
	// ErrNotFound indicates that a requested resource does not exist.
	ErrNotFound = errors.New("not found")

	// ErrAlreadyExists indicates that a resource with the same identity
	// already exists.
	ErrAlreadyExists = errors.New("already exists")

	// ErrInvalidArgument indicates that a caller-provided value violates
	// a precondition.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrAlreadyRunning indicates that a workflow instance with the
	// same ID is already active. Callers should treat this as a no-op:
	// the running workflow will pick up any new generation when it
	// completes.
	ErrAlreadyRunning = errors.New("workflow already running")

	// ErrIllegalStateTransition indicates that a requested state
	// transition violates the entity's lifecycle rules (e.g.
	// transitioning a terminal delivery back to progressing).
	ErrIllegalStateTransition = errors.New("illegal state transition")

	// ErrAuthExpired indicates that a delivery agent's credentials
	// have expired or been invalidated. The orchestration layer
	// translates this into FulfillmentStatePausedAuth so the
	// fulfillment waits for fresh credentials.
	ErrAuthExpired = errors.New("delivery auth expired")
)

// terminalPrefix is the marker prepended to terminal errors.
// [IsTerminal] uses both type assertion and string matching so the
// classification survives serialization across process/engine boundaries.
const terminalPrefix = "terminal: "

// terminalError marks an error as non-retryable. The operation will
// never succeed without external intervention. Unclassified errors
// default to retryable.
type terminalError struct{ err error }

func (e *terminalError) Error() string { return terminalPrefix + e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

// TerminalError wraps err as non-retryable. Returns nil if err is nil.
func TerminalError(err error) error {
	if err == nil {
		return nil
	}
	return &terminalError{err: err}
}

// IsTerminal reports whether any error in err's chain is terminal.
// It first checks the Go type chain (for in-process errors), then
// falls back to inspecting the error string for the terminal prefix
// (for errors that lost type information through serialization, e.g.
// durable workflow engine activity errors).
func IsTerminal(err error) bool {
	var t *terminalError
	if errors.As(err, &t) {
		return true
	}
	return err != nil && strings.Contains(err.Error(), terminalPrefix)
}

// IsAuthExpired reports whether err indicates expired or missing
// delivery credentials. Like [IsTerminal], it checks the Go error
// chain first, then falls back to string matching so the
// classification survives serialization across workflow engine
// boundaries.
func IsAuthExpired(err error) bool {
	if errors.Is(err, ErrAuthExpired) {
		return true
	}
	return err != nil && strings.Contains(err.Error(), ErrAuthExpired.Error())
}
