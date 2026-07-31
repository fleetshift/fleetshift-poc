package testenv_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testenv"
)

func startTestEnv(t *testing.T, opts ...testenv.Option) *testenv.Env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testenv.DefaultStartupTimeout)
	t.Cleanup(cancel)

	env, err := testenv.Start(ctx, opts...)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return env
}

func finishEnv(t *testing.T, env *testenv.Env, passed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testenv.DefaultTeardownTimeout)
	defer cancel()
	if err := env.Finish(ctx, passed); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

func TestFinish_DeletesOwnedDirOnPass(t *testing.T) {
	env := startTestEnv(t)
	dir := env.WorkDir()
	logPath := env.ServerLogPath()
	if logPath == "" {
		t.Fatal("ServerLogPath empty")
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("server.log missing during run: %v", err)
	}
	rel, err := filepath.Rel(env.Artifacts.Root, logPath)
	if err != nil || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		t.Fatalf("server.log must be outside artifacts root; rel=%q err=%v", rel, err)
	}

	finishEnv(t, env, true)
	if env.Kept() {
		t.Fatal("Kept() = true after clean pass, want false")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("work dir still exists after clean pass: %s (stat err=%v)", dir, err)
	}
}

func TestFinish_RetainsOwnedDirOnFailure(t *testing.T) {
	env := startTestEnv(t)
	dir := env.WorkDir()
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	finishEnv(t, env, false)
	if !env.Kept() {
		t.Fatal("Kept() = false after failure, want true")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("work dir missing after failure retention: %v", err)
	}
	raw, err := os.ReadFile(env.ServerLogPath())
	if err != nil {
		t.Fatalf("read server.log: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("server.log is empty")
	}
	if _, err := os.Stat(filepath.Join(dir, "artifacts", "summary.json")); err != nil {
		t.Fatalf("artifacts/summary.json missing: %v", err)
	}
}

func TestFinish_KeepWorkDirRetainsOnPass(t *testing.T) {
	tests := []struct {
		name string
		opts []testenv.Option
		env  func(t *testing.T)
	}{
		{
			name: "WithKeepWorkDir",
			opts: []testenv.Option{testenv.WithKeepWorkDir()},
		},
		{
			name: "KeepWorkDirEnv",
			env: func(t *testing.T) {
				t.Helper()
				t.Setenv(testenv.KeepWorkDirEnv, "1")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != nil {
				tt.env(t)
			}
			env := startTestEnv(t, tt.opts...)
			dir := env.WorkDir()
			t.Cleanup(func() { _ = os.RemoveAll(dir) })

			finishEnv(t, env, true)
			if !env.Kept() {
				t.Fatal("Kept() = false, want true")
			}
			if _, err := os.Stat(dir); err != nil {
				t.Fatalf("work dir missing: %v", err)
			}
		})
	}
}

func TestFinish_CallerOwnedWorkDirNeverRemoved(t *testing.T) {
	dir := t.TempDir()
	env := startTestEnv(t, testenv.WithWorkDir(dir))

	finishEnv(t, env, true)
	if env.Kept() {
		t.Fatal("Kept() = true for caller-owned dir, want false")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("caller-owned work dir was removed: %v", err)
	}
	if _, err := os.Stat(env.ServerLogPath()); err != nil {
		t.Fatalf("server.log missing: %v", err)
	}
}

func TestWithLogger_SkipsServerLogFile(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	env := startTestEnv(t, testenv.WithWorkDir(dir), testenv.WithLogger(logger))
	if env.ServerLogPath() != "" {
		t.Fatalf("ServerLogPath = %q, want empty with WithLogger", env.ServerLogPath())
	}
	if _, err := os.Stat(filepath.Join(dir, testenv.ServerLogFile)); !os.IsNotExist(err) {
		t.Fatalf("server.log should not be created with WithLogger; err=%v", err)
	}
	finishEnv(t, env, true)
}

func TestFinish_Idempotent(t *testing.T) {
	env := startTestEnv(t)
	dir := env.WorkDir()

	finishEnv(t, env, true)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("work dir still exists after clean pass: %s", dir)
	}
	if env.Kept() {
		t.Fatal("Kept() = true after delete, want false")
	}

	ctx, cancel := context.WithTimeout(context.Background(), testenv.DefaultTeardownTimeout)
	defer cancel()
	if err := env.Finish(ctx, true); err != nil {
		t.Fatalf("second Finish: %v", err)
	}
}

func TestStart_FailureReturnsEnvForRetention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	env, err := testenv.Start(ctx)
	if err == nil {
		if env != nil {
			finishEnv(t, env, true)
		}
		t.Fatal("expected Start error with canceled context")
	}
	if env == nil {
		t.Fatal("expected non-nil Env after start failure for retention")
	}
	dir := env.WorkDir()
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	finishEnv(t, env, false)
	if !env.Kept() {
		t.Fatal("Kept() = false after failed start, want true")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("work dir missing after failed start retention: %v", err)
	}
	if path := env.ServerLogPath(); path != "" {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("server.log missing: %v", err)
		}
	}
}
