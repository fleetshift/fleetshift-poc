package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

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

// GetRequestMetadata returns a Bearer token after a refresh if needed.
// Missing config or tokens yield no metadata rather than an error.
func (t *tokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	cfg, err := auth.LoadConfigFrom(t.configDir)
	if err != nil {
		return nil, nil
	}

	ctx, err = withOIDCHTTPClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	tokens, _, err := auth.RefreshIfNeeded(ctx, t.store, oauthConfig(cfg))
	if err != nil {
		return nil, nil
	}

	if tokens.AccessToken == "" {
		return nil, nil
	}

	return map[string]string{
		"authorization": "Bearer " + tokens.AccessToken,
	}, nil
}

// RequireTransportSecurity reports false so plaintext gRPC still attaches tokens.
func (t *tokenCredentials) RequireTransportSecurity() bool {
	return false
}
