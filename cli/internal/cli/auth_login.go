package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func newAuthLoginCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Authenticate with the configured OIDC provider",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthLogin(cmd, ctx)
		},
	}
	return cmd
}

func runAuthLogin(cmd *cobra.Command, _ *cmdContext) error {
	cfg, err := auth.LoadConfig()
	if err != nil {
		return fmt.Errorf("load auth config (run 'fleetctl auth setup' first): %w", err)
	}

	pkce, err := auth.GeneratePKCE()
	if err != nil {
		return fmt.Errorf("generate PKCE: %w", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("start callback listener: %w", err)
	}
	defer lis.Close()

	callbackURL := fmt.Sprintf("http://127.0.0.1:%d/callback", lis.Addr().(*net.TCPAddr).Port)

	oauthCfg := &oauth2.Config{
		ClientID: cfg.ClientID,
		Endpoint: oauth2.Endpoint{
			AuthURL:   cfg.AuthorizationEndpoint,
			TokenURL:  cfg.TokenEndpoint,
			AuthStyle: oauth2.AuthStyleInParams,
		},
		RedirectURL: callbackURL,
		Scopes:      cfg.Scopes,
	}

	authURL := oauthCfg.AuthCodeURL("state",
		oauth2.SetAuthURLParam("code_challenge", pkce.Challenge),
		oauth2.SetAuthURLParam("code_challenge_method", pkce.ChallengeMethod),
	)

	fmt.Fprintf(cmd.OutOrStdout(), "Opening browser to:\n  %s\n\nWaiting for callback...\n", authURL)
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
			http.Error(w, "Authentication failed", http.StatusBadRequest)
			return
		}
		codeCh <- code
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<!DOCTYPE html><html><body>
<p>Authentication successful!</p>
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
		return err
	case <-cmd.Context().Done():
		return cmd.Context().Err()
	}

	_ = server.Shutdown(context.Background())

	exchangeCtx := cmd.Context()
	if httpClient, err := cfg.HTTPClient(); err != nil {
		return fmt.Errorf("create HTTP client: %w", err)
	} else if httpClient != nil {
		exchangeCtx = context.WithValue(exchangeCtx, oauth2.HTTPClient, httpClient)
	}

	tok, err := oauthCfg.Exchange(exchangeCtx, code,
		oauth2.SetAuthURLParam("code_verifier", pkce.Verifier),
	)
	if err != nil {
		return fmt.Errorf("exchange code for token: %w", err)
	}

	tokens := auth.Tokens{
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		TokenType:    tok.TokenType,
		Expiry:       tok.Expiry,
	}
	if idTok, ok := tok.Extra("id_token").(string); ok {
		tokens.IDToken = idTok
	}

	store := auth.KeyringTokenStore{}
	if err := store.Save(cmd.Context(), tokens); err != nil {
		return fmt.Errorf("save tokens: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Login successful!\n")
	return nil
}
