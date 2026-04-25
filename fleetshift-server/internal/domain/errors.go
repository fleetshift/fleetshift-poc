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

	// ErrAlreadyRunning indicates that a reconciliation workflow for
	// the requested deployment is already active. Callers should treat
	// this as a no-op: the running workflow will pick up any new
	// generation when it completes.
	ErrAlreadyRunning = errors.New("reconciliation workflow already running")

	// ErrConcurrentUpdate indicates that another mutation of the same
	// type is already in progress for this deployment at the same
	// observed generation. The caller should retry or let the
	// in-progress mutation converge.
	ErrConcurrentUpdate = errors.New("concurrent update")
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
