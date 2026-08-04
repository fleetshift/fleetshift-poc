// Package vaulttest provides contract tests for [domain.Vault]
// implementations.
package vaulttest

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.Vault] for each test invocation.
type Factory func(t *testing.T) domain.Vault

// Run exercises the [domain.Vault] contract.
func Run(t *testing.T, factory Factory) {
	t.Run("PutAndGet", func(t *testing.T) {
		v := factory(t)
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
	})

	t.Run("PutOverwrites", func(t *testing.T) {
		v := factory(t)
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
	})

	t.Run("GetNotFound", func(t *testing.T) {
		v := factory(t)
		_, err := v.Get(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("Get error = %v, want ErrNotFound", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		v := factory(t)
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
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		v := factory(t)
		err := v.Delete(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Errorf("Delete error = %v, want ErrNotFound", err)
		}
	})
}
