package bootstrap

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestGoWorkflowRegistry_CloseCancelsAndIsIdempotent(t *testing.T) {
	rt, err := NewGoWorkflowRegistry(SQLite{Path: filepath.Join(t.TempDir(), "wf.db")}, testLogger())
	if err != nil {
		t.Fatalf("NewGoWorkflowRegistry: %v", err)
	}

	// Parent ctx is never cancelled; Close must stop the worker itself.
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second Close must reuse the memoized WaitForCompletion result.
	closeCtx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := rt.Close(closeCtx2); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestGoWorkflowRegistry_ConcurrentClose(t *testing.T) {
	rt, err := NewGoWorkflowRegistry(SQLite{Path: filepath.Join(t.TempDir(), "wf.db")}, testLogger())
	if err != nil {
		t.Fatalf("NewGoWorkflowRegistry: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs[i] = rt.Close(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Close[%d]: %v", i, err)
		}
	}
}

func TestLifecycle_GoWorkflowRegistryCloseThroughServer(t *testing.T) {
	// Default unit helpers use the in-memory registry; exercise production
	// go-workflows through Server shutdown (appCancel + Close) and a second Close.
	dbPath := filepath.Join(t.TempDir(), "fleetshift.db")
	rt, err := NewGoWorkflowRegistry(SQLite{Path: dbPath}, testLogger())
	if err != nil {
		t.Fatalf("NewGoWorkflowRegistry: %v", err)
	}

	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   dbPath,
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv, err := Start(ctx, cfg, testLogger(),
		WithWorkflowRegistry(rt),
		WithIdentity(Identity{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) { return nil, nil }),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer closeCancel()
	if err := srv.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := srv.Close(closeCtx); err != nil {
		t.Fatalf("second Server.Close: %v", err)
	}
}
