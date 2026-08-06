package testenv

import (
	"context"
	"testing"
)

// StartT starts a test environment for a Go test. It fails the test
// on start error and registers cleanup via t.Cleanup.
//
// Cleanup calls [Env.Finish]: Start-owned work directories are removed on a
// clean pass and retained on failure (with private server.log under the
// work dir). Set [WithKeepWorkDir] or [KeepWorkDirEnv] to retain always.
// Register any artifact assertions in t.Cleanup after StartT so they run
// before Finish (LIFO).
func StartT(t *testing.T, opts ...Option) *Env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), DefaultStartupTimeout)
	t.Cleanup(cancel)

	env, err := Start(ctx, opts...)
	if err != nil {
		if env != nil {
			finishCtx, finishCancel := context.WithTimeout(context.Background(), DefaultTeardownTimeout)
			defer finishCancel()
			_ = env.Finish(finishCtx, false)
			t.Fatalf("testenv.Start: %v (work dir retained: %s)", err, env.WorkDir())
		}
		t.Fatalf("testenv.Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), DefaultTeardownTimeout)
		defer closeCancel()
		passed := !t.Failed()
		if err := env.Finish(closeCtx, passed); err != nil {
			t.Errorf("testenv.Finish: %v", err)
		}
		if env.Kept() {
			t.Logf("testenv work dir retained: %s (private %s; allow-listed artifacts under artifacts/)",
				env.WorkDir(), ServerLogFile)
		}
	})
	return env
}
