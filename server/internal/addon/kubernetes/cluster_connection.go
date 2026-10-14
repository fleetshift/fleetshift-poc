package kubernetes

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Target property keys used by both delivery and in-process indexing.
// Provisioners (kind, gcphcp) write these onto kubernetes targets so one
// target description drives both paths.
const (
	// PropAPIServer is the Kubernetes API server URL.
	PropAPIServer = "api_server"
	// PropCACert is the PEM-encoded cluster CA certificate.
	PropCACert = "ca_cert"
	// PropServiceAccountToken is a direct bearer token (tests / simple setups).
	PropServiceAccountToken = "service_account_token"
	// PropServiceAccountTokenRef is a vault [domain.SecretRef] for the
	// bearer token when PropServiceAccountToken is unset.
	PropServiceAccountTokenRef = "service_account_token_ref"
	// PropClusterResourceName is the managed cluster [domain.ResourceName]
	// (e.g. "clusters/c1") whose ID is the object inventory parent segment.
	// Distinct from the kubernetes target ID when that target is prefixed
	// (e.g. "k8s-c1").
	PropClusterResourceName = "cluster_resource_name"
)

// defaultDeliveryClientTimeout bounds short-lived delivery apply/delete RPCs.
const defaultDeliveryClientTimeout = 30 * time.Second

// BuildTargetRESTConfig constructs a [rest.Config] from the target's
// properties and optional vault-backed service account token. The
// bearer token is optional: when neither PropServiceAccountToken nor
// PropServiceAccountTokenRef is set, BearerToken is left empty. Used by
// in-process indexing, which may start before credentials are present.
//
// Timeout stays 0 so long-lived watches are not killed by http.Client.Timeout.
// Discovery applies its own default when Timeout is 0; delivery builders set
// Timeout separately.
func BuildTargetRESTConfig(ctx context.Context, vault domain.Vault, target domain.TargetInfo) (*rest.Config, error) {
	props := target.Properties()
	host := props[PropAPIServer]
	if host == "" {
		return nil, fmt.Errorf("target %q missing property %q", target.ID(), PropAPIServer)
	}

	cfg := &rest.Config{Host: host}
	if ca := props[PropCACert]; ca != "" {
		cfg.TLSClientConfig = rest.TLSClientConfig{CAData: []byte(ca)}
	}

	token, err := resolvePlatformTokenOptional(ctx, vault, target)
	if err != nil {
		return nil, err
	}
	cfg.BearerToken = token
	return cfg, nil
}

// BuildPlatformRESTConfig is like [BuildTargetRESTConfig] but requires a
// platform bearer token (direct property or vault ref). Used by attested
// delivery's run-as-platform path.
func BuildPlatformRESTConfig(ctx context.Context, vault domain.Vault, target domain.TargetInfo) (*rest.Config, error) {
	cfg, err := BuildTargetRESTConfig(ctx, vault, target)
	if err != nil {
		return nil, err
	}
	if cfg.BearerToken == "" {
		return nil, fmt.Errorf("target %q missing %s or %s for platform delivery",
			target.ID(), PropServiceAccountToken, PropServiceAccountTokenRef)
	}
	cfg.Timeout = defaultDeliveryClientTimeout
	return cfg, nil
}

// buildCallerRESTConfig builds a REST config using the caller's JWT
// (token-passthrough delivery).
func buildCallerRESTConfig(target domain.TargetInfo, token domain.RawToken) (*rest.Config, error) {
	apiServer := target.Properties()[PropAPIServer]
	if apiServer == "" {
		return nil, fmt.Errorf("target %q missing property %q", target.ID(), PropAPIServer)
	}
	cfg := &rest.Config{
		Host:        apiServer,
		BearerToken: string(token),
		Timeout:     defaultDeliveryClientTimeout,
	}
	if ca := target.Properties()[PropCACert]; ca != "" {
		cfg.TLSClientConfig.CAData = []byte(ca)
	}
	return cfg, nil
}

// resolvePlatformTokenOptional returns the platform bearer token when
// present. An empty string with a nil error means no credentials were
// configured. A non-nil error means a ref was set but could not be
// resolved (missing vault or vault lookup failure).
func resolvePlatformTokenOptional(ctx context.Context, vault domain.Vault, target domain.TargetInfo) (string, error) {
	props := target.Properties()
	if token := props[PropServiceAccountToken]; token != "" {
		return token, nil
	}
	ref := props[PropServiceAccountTokenRef]
	if ref == "" {
		return "", nil
	}
	if vault == nil {
		return "", fmt.Errorf("target %q has %s but no vault configured", target.ID(), PropServiceAccountTokenRef)
	}
	val, err := vault.Get(ctx, domain.SecretRef(ref))
	if err != nil {
		return "", fmt.Errorf("resolve %s %q for target %q: %w", PropServiceAccountTokenRef, ref, target.ID(), err)
	}
	return string(val), nil
}
