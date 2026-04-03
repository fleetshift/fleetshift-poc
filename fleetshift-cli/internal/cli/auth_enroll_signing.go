package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

func newAuthEnrollSigningCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enroll-signing",
		Short: "Generate a signing key pair and enroll the public key with the server",
		Long: `Generates an ECDSA P-256 key pair, authenticates via a dedicated OIDC
client to get a purpose-scoped ID token, creates a self-certifying key
binding bundle, and submits it to the server. The private key is stored
in the OS keyring.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthEnrollSigning(cmd, ctx)
		},
	}
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

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate key pair: %w", err)
	}

	pubJWK, err := ecPublicKeyToJWK(&privateKey.PublicKey)
	if err != nil {
		return fmt.Errorf("export public key as JWK: %w", err)
	}

	idToken, err := performEnrollmentOIDCFlow(cmd, cfg)
	if err != nil {
		return fmt.Errorf("enrollment OIDC flow: %w", err)
	}

	claims, err := parseUnsafeJWTClaims(idToken)
	if err != nil {
		return fmt.Errorf("parse ID token claims: %w", err)
	}

	doc := keyBindingDoc{
		PublicKeyJWK: json.RawMessage(pubJWK),
		Subject:      claims.Sub,
		Issuer:       claims.Iss,
		EnrolledAt:   time.Now().UTC().Format(time.RFC3339),
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal key binding doc: %w", err)
	}

	hash := sha256.Sum256(docBytes)
	sig, err := ecdsa.SignASN1(rand.Reader, privateKey, hash[:])
	if err != nil {
		return fmt.Errorf("sign key binding doc: %w", err)
	}

	bindingID, err := generateID()
	if err != nil {
		return fmt.Errorf("generate binding ID: %w", err)
	}

	client := pb.NewSigningKeyBindingServiceClient(ctx.conn)
	_, err = client.CreateSigningKeyBinding(cmd.Context(), &pb.CreateSigningKeyBindingRequest{
		SigningKeyBindingId: bindingID,
		KeyBindingDoc:       docBytes,
		KeyBindingSignature: sig,
		IdentityToken:       idToken,
	})
	if err != nil {
		return fmt.Errorf("create signing key binding: %w", err)
	}

	pemData, err := marshalECPrivateKeyPEM(privateKey)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}
	if err := auth.SaveSigningKey(pemData); err != nil {
		return fmt.Errorf("save signing key to keyring: %w", err)
	}

	fingerprint := sha256.Sum256(pubJWK)
	fmt.Fprintf(cmd.OutOrStdout(), "Signing key enrolled successfully.\n")
	fmt.Fprintf(cmd.OutOrStdout(), "  Binding ID:  %s\n", bindingID)
	fmt.Fprintf(cmd.OutOrStdout(), "  Fingerprint: %s\n", base64.RawURLEncoding.EncodeToString(fingerprint[:16]))

	return nil
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

	fmt.Fprintf(cmd.OutOrStdout(), "Opening browser for signing key enrollment...\n  %s\n\nWaiting for callback...\n", authURL)
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
<p>Signing key enrollment callback received!</p>
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

type keyBindingDoc struct {
	PublicKeyJWK json.RawMessage `json:"public_key_jwk"`
	Subject      string          `json:"subject"`
	Issuer       string          `json:"issuer"`
	EnrolledAt   string          `json:"enrolled_at"`
}

type jwtClaims struct {
	Sub string `json:"sub"`
	Iss string `json:"iss"`
}

func parseUnsafeJWTClaims(token string) (jwtClaims, error) {
	parts := splitJWT(token)
	if len(parts) != 3 {
		return jwtClaims{}, fmt.Errorf("invalid JWT: expected 3 parts, got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return jwtClaims{}, fmt.Errorf("decode JWT payload: %w", err)
	}
	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return jwtClaims{}, fmt.Errorf("unmarshal JWT claims: %w", err)
	}
	return claims, nil
}

func splitJWT(token string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(token); i++ {
		if token[i] == '.' {
			parts = append(parts, token[start:i])
			start = i + 1
		}
	}
	parts = append(parts, token[start:])
	return parts
}

func ecPublicKeyToJWK(pub *ecdsa.PublicKey) ([]byte, error) {
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	xBytes := pub.X.Bytes()
	yBytes := pub.Y.Bytes()

	xPadded := make([]byte, byteLen)
	yPadded := make([]byte, byteLen)
	copy(xPadded[byteLen-len(xBytes):], xBytes)
	copy(yPadded[byteLen-len(yBytes):], yBytes)

	jwk := struct {
		Kty string `json:"kty"`
		Crv string `json:"crv"`
		X   string `json:"x"`
		Y   string `json:"y"`
	}{
		Kty: "EC",
		Crv: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(xPadded),
		Y:   base64.RawURLEncoding.EncodeToString(yPadded),
	}
	return json.Marshal(jwk)
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
