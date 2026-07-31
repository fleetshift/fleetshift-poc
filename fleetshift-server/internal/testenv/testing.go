package testenv

import (
	"context"
	"testing"
)

// StartT starts a hermetic environment for a Go test. It fails the test
// on start error and registers cleanup via t.Cleanup.
func StartT(t *testing.T, opts ...Option) *Env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), DefaultStartupTimeout)
	t.Cleanup(cancel)

	env, err := Start(ctx, opts...)
	if err != nil {
		t.Fatalf("testenv.Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), DefaultTeardownTimeout)
		defer closeCancel()
		if err := env.Close(closeCtx); err != nil {
			t.Errorf("testenv.Close: %v", err)
		}
	})
	return env
}
