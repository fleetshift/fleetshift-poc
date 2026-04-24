package domain

import "errors"

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
