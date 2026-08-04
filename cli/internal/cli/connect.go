package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func dial(flags globalFlags) (*grpc.ClientConn, error) {
	if err := validateTransportFlags(flags); err != nil {
		return nil, err
	}

	transportCreds, err := buildTransportCredentials(flags)
	if err != nil {
		return nil, err
	}

	creds := &tokenCredentials{store: auth.KeyringTokenStore{}}
	conn, err := grpc.NewClient(flags.server,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithPerRPCCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", flags.server, err)
	}
	return conn, nil
}

func validateTransportFlags(flags globalFlags) error {
	if !flags.serverTLS && flags.serverCAFile != "" {
		return fmt.Errorf("--server-ca-file requires --server-tls")
	}
	if !flags.serverTLS && flags.serverInsecure {
		return fmt.Errorf("--server-insecure requires --server-tls")
	}
	return nil
}

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
	store auth.TokenStore
}

func (t *tokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	cfg, err := auth.LoadConfig()
	if err != nil {
		return nil, nil
	}

	oauthCfg := &oauth2.Config{
		ClientID: cfg.ClientID,
		Endpoint: oauth2.Endpoint{
			AuthURL:   cfg.AuthorizationEndpoint,
			TokenURL:  cfg.TokenEndpoint,
			AuthStyle: oauth2.AuthStyleInParams,
		},
		Scopes: cfg.Scopes,
	}

	tokens, _, err := auth.RefreshIfNeeded(ctx, t.store, oauthCfg)
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

func (t *tokenCredentials) RequireTransportSecurity() bool {
	return false
}
