package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

// dial opens a gRPC client to flags.server using the token store for per-RPC credentials.
func dial(flags globalFlags) (*grpc.ClientConn, error) {
	if err := validateTransportFlags(flags); err != nil {
		return nil, err
	}

	transportCreds, err := buildTransportCredentials(flags)
	if err != nil {
		return nil, err
	}

	creds := &tokenCredentials{store: flags.store(), configDir: flags.configDir}
	conn, err := grpc.NewClient(flags.server,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithPerRPCCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", flags.server, err)
	}
	return conn, nil
}

// validateTransportFlags requires CA and insecure flags to be used with --server-tls.
func validateTransportFlags(flags globalFlags) error {
	if !flags.serverTLS && flags.serverCAFile != "" {
		return fmt.Errorf("--server-ca-file requires --server-tls")
	}
	if !flags.serverTLS && flags.serverInsecure {
		return fmt.Errorf("--server-insecure requires --server-tls")
	}
	return nil
}

// buildTransportCredentials returns TLS credentials when --server-tls is set, otherwise insecure.
func buildTransportCredentials(flags globalFlags) (credentials.TransportCredentials, error) {
	if !flags.serverTLS {
		return insecure.NewCredentials(), nil
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}

	if flags.serverCAFile != "" {
		caPEM, err := os.ReadFile(flags.serverCAFile)
		if err != nil {
			return nil, fmt.Errorf("read server CA file %s: %w", flags.serverCAFile, err)
		}
		if ok := pool.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("parse server CA file %s: no certificates found", flags.serverCAFile)
		}
	}

	return credentials.NewTLS(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            pool,
		InsecureSkipVerify: flags.serverInsecure, //nolint:gosec // explicit debug flag
	}), nil
}

// tokenCredentials implements [credentials.PerRPCCredentials] by loading
// tokens from the token store and refreshing them if needed.
type tokenCredentials struct {
	store     auth.TokenStore
	configDir string
}

// GetRequestMetadata implements [credentials.PerRPCCredentials].
// It never returns an error: gRPC would then skip the RPC. Missing or
// unusable credentials mean no auth headers, not a failed call.
func (t *tokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	return t.bearerMetadata(ctx), nil
}

// bearerMetadata returns Authorization when a usable access token is
// available. Failures (missing config, missing tokens, refresh, OIDC CA)
// yield nil so the RPC still goes out.
func (t *tokenCredentials) bearerMetadata(ctx context.Context) map[string]string {
	cfg, err := auth.LoadConfigFrom(t.configDir)
	if err != nil {
		return nil
	}

	tokens, err := t.store.Load(ctx)
	if err != nil {
		return nil
	}

	if auth.NeedsRefresh(tokens) {
		refreshCtx, httpErr := withOIDCHTTPClient(ctx, cfg)
		if httpErr != nil {
			return nil
		}
		tokens, _, err = auth.RefreshIfNeeded(refreshCtx, t.store, oauthConfig(cfg))
		if err != nil {
			return nil
		}
	}

	if tokens.AccessToken == "" {
		return nil
	}
	if !tokens.Expiry.IsZero() && !time.Now().Before(tokens.Expiry) {
		return nil
	}

	return map[string]string{
		"authorization": "Bearer " + tokens.AccessToken,
	}
}

// RequireTransportSecurity reports false so plaintext gRPC still attaches tokens.
func (t *tokenCredentials) RequireTransportSecurity() bool {
	return false
}
