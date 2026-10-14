package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

const (
	// startupListAttempts bounds TargetLister retries during startup replay.
	startupListAttempts = 3
	// startupListBackoff is the delay between list retries.
	startupListBackoff = 200 * time.Millisecond
	// startupCredentialAttempts bounds vault/property credential resolve retries.
	startupCredentialAttempts = 3
	// startupCredentialBackoff is the delay between credential resolve retries.
	startupCredentialBackoff = 200 * time.Millisecond
	// StartupReplayGeneration is the producer generation used for one-shot
	// post-restart EnsureIndexer calls. Producers use delivery generations;
	// replay uses a fixed low watermark that cannot replace a newer ready
	// process started by an in-flight delivery.
	StartupReplayGeneration domain.Generation = 1
)

// TargetLister lists every known target. Used by startup replay.
type TargetLister interface {
	// ListTargets returns all persisted targets visible to startup replay.
	ListTargets(ctx context.Context) ([]domain.TargetInfo, error)
}

// ReplayPersistedIndexers lists persisted targets once, resolves each
// applicable Kubernetes target's indexing credential, and calls
// EnsureIndexer. Per-target failures are logged and skipped. It does not
// poll or re-list afterward; targets that appear only after the list
// returns are not started by this call. Callers that must not block
// listen should invoke it from a goroutine. logger must be non-nil.
func ReplayPersistedIndexers(
	ctx context.Context,
	lister TargetLister,
	vault domain.Vault,
	runtime IndexingRuntime,
	logger *slog.Logger,
) {
	logger = logger.With("component", "kubernetes-index-startup-replay")

	if runtime == nil || lister == nil {
		logger.Warn("startup replay skipped; missing indexing runtime or target lister")
		return
	}

	targets, err := listTargetsWithRetry(ctx, lister)
	if err != nil {
		logger.Warn("startup replay aborted; target list failed", "error", err)
		return
	}

	for _, target := range targets {
		if target.Type() != TargetType {
			continue
		}
		input, ok, err := indexRuntimeInputFromTarget(ctx, vault, target)
		if err != nil {
			logger.Warn("startup replay skipped target",
				"target", string(target.ID()),
				"error", err,
			)
			continue
		}
		if !ok {
			continue
		}
		err = RetryLocalEnvelope(ctx, LocalEnsureRetryDeadline, func(attemptCtx context.Context) error {
			return runtime.EnsureIndexer(attemptCtx, input)
		})
		if err != nil {
			logger.Warn("startup replay EnsureIndexer exhausted",
				"target", string(target.ID()),
				"error", err,
			)
			continue
		}
		logger.Info("startup replay started indexer", "target", string(target.ID()))
	}
}

// listTargetsWithRetry calls TargetLister with bounded retries and backoff.
func listTargetsWithRetry(ctx context.Context, lister TargetLister) ([]domain.TargetInfo, error) {
	var lastErr error
	for attempt := 1; attempt <= startupListAttempts; attempt++ {
		targets, err := lister.ListTargets(ctx)
		if err == nil {
			return targets, nil
		}
		lastErr = err
		if attempt == startupListAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(startupListBackoff):
		}
	}
	return nil, lastErr
}

// indexRuntimeInputFromTarget builds EnsureIndexer input from a persisted
// target. ok is false when the target has no resolvable indexing credential
// (not an error — skip quietly).
func indexRuntimeInputFromTarget(
	ctx context.Context,
	vault domain.Vault,
	target domain.TargetInfo,
) (IndexRuntimeInput, bool, error) {
	props := target.Properties()
	apiServer := props[PropAPIServer]
	if apiServer == "" {
		return IndexRuntimeInput{}, false, nil
	}

	// Permanent target-property errors before credential retries so
	// vault-backed targets with bad cluster names fail fast.
	clusterResourceName, err := clusterResourceNameFromTarget(target)
	if err != nil {
		return IndexRuntimeInput{}, false, err
	}

	var lastErr error
	var token []byte
	var secretRef domain.SecretRef
	for attempt := 1; attempt <= startupCredentialAttempts; attempt++ {
		token, secretRef, lastErr = resolveIndexingCredential(ctx, vault, target)
		if lastErr == nil {
			break
		}
		if attempt == startupCredentialAttempts {
			return IndexRuntimeInput{}, false, lastErr
		}
		select {
		case <-ctx.Done():
			return IndexRuntimeInput{}, false, ctx.Err()
		case <-time.After(startupCredentialBackoff):
		}
	}
	if len(token) == 0 {
		return IndexRuntimeInput{}, false, nil
	}

	input, err := NewIndexRuntimeInput(
		target.ID(),
		clusterResourceName,
		apiServer,
		props[PropCACert],
		token,
		secretRef,
		StartupReplayGeneration,
		DefaultIndexConfig(),
	)
	if err != nil {
		return IndexRuntimeInput{}, false, err
	}
	return input, true, nil
}

// clusterResourceNameFromTarget reads [PropClusterResourceName] from the
// persisted kubernetes target. Missing or malformed values are permanent
// errors so startup replay does not invent a parent segment.
func clusterResourceNameFromTarget(target domain.TargetInfo) (domain.ResourceName, error) {
	raw := target.Properties()[PropClusterResourceName]
	if raw == "" {
		return "", fmt.Errorf("%w: target %q missing %s property", domain.ErrInvalidArgument, target.ID(), PropClusterResourceName)
	}
	name, err := ParseClusterResourceName(raw)
	if err != nil {
		return "", fmt.Errorf("target %q %s: %w", target.ID(), PropClusterResourceName, err)
	}
	return name, nil
}

// resolveIndexingCredential returns bearer token bytes and optional vault
// SecretRef from target properties. Empty token with nil error means skip.
func resolveIndexingCredential(
	ctx context.Context,
	vault domain.Vault,
	target domain.TargetInfo,
) ([]byte, domain.SecretRef, error) {
	props := target.Properties()
	if token := props[PropServiceAccountToken]; token != "" {
		return []byte(token), "", nil
	}
	ref := props[PropServiceAccountTokenRef]
	if ref == "" {
		return nil, "", nil
	}
	if vault == nil {
		return nil, "", fmt.Errorf("target %q has %s but no vault configured", target.ID(), PropServiceAccountTokenRef)
	}
	val, err := vault.Get(ctx, domain.SecretRef(ref))
	if err != nil {
		return nil, "", err
	}
	return val, domain.SecretRef(ref), nil
}
