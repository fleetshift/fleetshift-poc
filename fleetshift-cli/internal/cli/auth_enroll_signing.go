package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh"
	"golang.org/x/oauth2"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

func newAuthEnrollSigningCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enroll-signing",
		Short: "Generate a signing key pair and enroll with the server",
		Long: `Generates an ECDSA P-256 key pair, authenticates via a dedicated OIDC
client to get a purpose-scoped ID token, and submits the token to the
server to create a signer enrollment. The private key is stored in the
OS keyring. The public key is exported in SSH format for the user to
upload to GitHub as a signing key.

Use --reuse-key to re-enroll with an existing key pair already stored
in the OS keyring (e.g. after a server-side reset or team change).`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthEnrollSigning(cmd, ctx)
		},
	}
	cmd.Flags().Bool("reuse-key", false, "reuse the signing key already stored in the OS keyring instead of generating a new one")
	return cmd
}

func runAuthEnrollSigning(cmd *cobra.Command, ctx *cmdContext) error {
	cfg, err := auth.LoadConfig()
	if err != nil {
		return fmt.Errorf("load auth config (run 'fleetctl auth setup' first): %w", err)
	}
	if cfg.KeyEnrollmentClientID == "" {
		return fmt.Errorf("no key enrollment client ID configured (set --key-enrollment-client-id during 'fleetctl auth setup')")
	}

	reuseKey, _ := cmd.Flags().GetBool("reuse-key")
	privateKey, generated, err := acquireSigningKey(reuseKey)
	if err != nil {
		return err
	}

	idToken, err := performEnrollmentOIDCFlow(cmd, cfg)
	if err != nil {
		return fmt.Errorf("enrollment OIDC flow: %w", err)
	}

	enrollmentID, err := generateID()
	if err != nil {
		return fmt.Errorf("generate enrollment ID: %w", err)
	}

	client := pb.NewSignerEnrollmentServiceClient(ctx.conn)
	enrollment, err := client.CreateSignerEnrollment(cmd.Context(), &pb.CreateSignerEnrollmentRequest{
		SignerEnrollmentId: enrollmentID,
		IdentityToken:      idToken,
	})
	if err != nil {
		return fmt.Errorf("create signer enrollment: %w", err)
	}

	if generated {
		pemData, err := marshalECPrivateKeyPEM(privateKey)
		if err != nil {
			return fmt.Errorf("marshal private key: %w", err)
		}
		if err := auth.SaveSigningKey(pemData); err != nil {
			return fmt.Errorf("save signing key to keyring: %w", err)
		}
	}

	sshPubKey, err := sshPublicKey(&privateKey.PublicKey)
	if err != nil {
		return fmt.Errorf("export SSH public key: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Signer enrolled successfully.\n")
	fmt.Fprintf(cmd.OutOrStdout(), "  Enrollment:       %s\n", enrollment.GetName())
	fmt.Fprintf(cmd.OutOrStdout(), "  Registry subject: %s\n", enrollment.GetRegistrySubject())
	fmt.Fprintf(cmd.OutOrStdout(), "\n")
	fmt.Fprintf(cmd.OutOrStdout(), "Your SSH signing public key (upload to GitHub → Settings → SSH and GPG keys → New SSH key, type \"Signing Key\"):\n\n")
	fmt.Fprintf(cmd.OutOrStdout(), "  %s\n\n", sshPubKey)
	fmt.Fprintf(cmd.OutOrStdout(), "Or visit: https://github.com/settings/ssh/new\n")

	return nil
}

// acquireSigningKey either loads an existing key from the keyring (when
// reuseKey is true) or generates a fresh ECDSA P-256 key pair. The
// generated return value indicates whether the key was newly created and
// needs to be persisted to the keyring.
func acquireSigningKey(reuseKey bool) (key *ecdsa.PrivateKey, generated bool, err error) {
	if reuseKey {
		key, err = loadSigningPrivateKey()
		if err != nil {
			return nil, false, fmt.Errorf("reuse existing signing key: %w", err)
		}
		return key, false, nil
	}
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, false, fmt.Errorf("generate key pair: %w", err)
	}
	return key, true, nil
}

func sshPublicKey(pub *ecdsa.PublicKey) (string, error) {
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return "", err
	}
	return string(ssh.MarshalAuthorizedKey(sshPub)), nil
}

func performEnrollmentOIDCFlow(cmd *cobra.Command, cfg auth.Config) (string, error) {
	pkce, err := auth.GeneratePKCE()
	if err != nil {
		return "", fmt.Errorf("generate PKCE: %w", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("start callback listener: %w", err)
	}
	defer lis.Close()

	callbackURL := fmt.Sprintf("http://127.0.0.1:%d/callback", lis.Addr().(*net.TCPAddr).Port)

	oauthCfg := &oauth2.Config{
		ClientID: cfg.KeyEnrollmentClientID,
		Endpoint: oauth2.Endpoint{
			AuthURL:   cfg.AuthorizationEndpoint,
			TokenURL:  cfg.TokenEndpoint,
			AuthStyle: oauth2.AuthStyleInParams,
		},
		RedirectURL: callbackURL,
		Scopes:      []string{"openid", "profile", "email"},
	}

	authURL := oauthCfg.AuthCodeURL("state",
		oauth2.SetAuthURLParam("code_challenge", pkce.Challenge),
		oauth2.SetAuthURLParam("code_challenge_method", pkce.ChallengeMethod),
	)

	fmt.Fprintf(cmd.OutOrStdout(), "Opening browser for signer enrollment...\n  %s\n\nWaiting for callback...\n", authURL)
	if err := auth.OpenBrowser(authURL); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Failed to open browser: %v\nPlease open the URL manually.\n", err)
	}

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("code")
		if code == "" {
			errMsg := r.URL.Query().Get("error")
			if errMsg == "" {
				errMsg = "no authorization code in callback"
			}
			errCh <- fmt.Errorf("callback error: %s", errMsg)
			http.Error(w, "Enrollment failed", http.StatusBadRequest)
			return
		}
		codeCh <- code
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<!DOCTYPE html><html><body>
<p>Signer enrollment callback received!</p>
<script>window.close()</script>
</body></html>`)
	})

	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(lis); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	var code string
	select {
	case code = <-codeCh:
	case err := <-errCh:
		return "", err
	case <-cmd.Context().Done():
		return "", cmd.Context().Err()
	}

	_ = server.Shutdown(context.Background())

	exchangeCtx := cmd.Context()
	if httpClient, err := cfg.HTTPClient(); err != nil {
		return "", fmt.Errorf("create HTTP client: %w", err)
	} else if httpClient != nil {
		exchangeCtx = context.WithValue(exchangeCtx, oauth2.HTTPClient, httpClient)
	}

	tok, err := oauthCfg.Exchange(exchangeCtx, code,
		oauth2.SetAuthURLParam("code_verifier", pkce.Verifier),
	)
	if err != nil {
		return "", fmt.Errorf("exchange code for token: %w", err)
	}

	idToken, ok := tok.Extra("id_token").(string)
	if !ok || idToken == "" {
		return "", fmt.Errorf("no id_token in token response")
	}
	return idToken, nil
}

func generateID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func marshalECPrivateKeyPEM(key *ecdsa.PrivateKey) (string, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", err
	}
	block := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: der,
	}
	return string(pem.EncodeToMemory(block)), nil
}
