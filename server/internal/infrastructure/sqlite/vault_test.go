package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func openVault(t *testing.T) *sqlite.VaultStore {
	t.Helper()
	return &sqlite.VaultStore{DB: sqlite.OpenTestDB(t)}
}

func TestVault_PutAndGet(t *testing.T) {
	v := openVault(t)
	ctx := context.Background()

	ref := domain.SecretRef("targets/foo/kubeconfig")
	value := []byte("apiVersion: v1\nkind: Config")

	if err := v.Put(ctx, ref, value); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := v.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(value) {
		t.Errorf("Get = %q, want %q", got, value)
	}
}

func TestVault_PutOverwrites(t *testing.T) {
	v := openVault(t)
	ctx := context.Background()

	ref := domain.SecretRef("targets/bar/kubeconfig")
	if err := v.Put(ctx, ref, []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := v.Put(ctx, ref, []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	got, err := v.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("Get = %q, want %q", got, "v2")
	}
}

func TestVault_GetNotFound(t *testing.T) {
	v := openVault(t)
	ctx := context.Background()

	_, err := v.Get(ctx, "nonexistent")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Get error = %v, want ErrNotFound", err)
	}
}

func TestVault_Delete(t *testing.T) {
	v := openVault(t)
	ctx := context.Background()

	ref := domain.SecretRef("targets/baz/kubeconfig")
	if err := v.Put(ctx, ref, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := v.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := v.Get(ctx, ref)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Get after delete: error = %v, want ErrNotFound", err)
	}
}

func TestVault_DeleteNotFound(t *testing.T) {
	v := openVault(t)
	ctx := context.Background()

	err := v.Delete(ctx, "nonexistent")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Delete error = %v, want ErrNotFound", err)
	}
}
