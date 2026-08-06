package domain

import "context"

// SecretRef identifies a secret stored in a [Vault].
type SecretRef string

// Vault stores and retrieves opaque secrets keyed by [SecretRef].
// Implementations may use any backing store (SQLite, HashiCorp Vault,
// cloud KMS, etc.).
type Vault interface {
	Put(ctx context.Context, ref SecretRef, value []byte) error
	Get(ctx context.Context, ref SecretRef) ([]byte, error)
	Delete(ctx context.Context, ref SecretRef) error
}
