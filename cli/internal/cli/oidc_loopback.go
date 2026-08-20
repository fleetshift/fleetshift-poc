package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

const (
	oidcLoginFailStatus  = "Authentication failed"
	oidcEnrollFailStatus = "Enrollment failed"
	oidcLoginSuccessHTML = `<!DOCTYPE html><html><body>
<p>Authentication successful!</p>
<script>window.close()</script>
</body></html>`
	oidcEnrollSuccessHTML = `<!DOCTYPE html><html><body>
<p>Signer enrollment callback received!</p>
<script>window.close()</script>
</body></html>`
)

// openBrowser launches the system browser. Tests replace this to assert
// --no-browser skips the call.
var openBrowser = auth.OpenBrowser

// oauthConfig returns the oauth2 client for cfg. RedirectURL is filled in by
// runLoopbackOIDC.
func oauthConfig(cfg auth.Config) *oauth2.Config {
	return &oauth2.Config{
		ClientID: cfg.ClientID,
		Endpoint: oauth2.Endpoint{
			AuthURL:   cfg.AuthorizationEndpoint,
			TokenURL:  cfg.TokenEndpoint,
			AuthStyle: oauth2.AuthStyleInParams,
		},
		Scopes: cfg.Scopes,
	}
}

// withOIDCHTTPClient attaches cfg.HTTPClient to ctx when an OIDC CA file is configured.
func withOIDCHTTPClient(ctx context.Context, cfg auth.Config) (context.Context, error) {
	httpClient, err := cfg.HTTPClient()
	if err != nil {
		return ctx, fmt.Errorf("create HTTP client: %w", err)
	}
	if httpClient != nil {
		return context.WithValue(ctx, oauth2.HTTPClient, httpClient), nil
	}
	return ctx, nil
}

// runLoopbackOIDC runs the loopback-redirect OIDC authorization-code flow
// and returns the token. When noBrowser is true, it prints AUTH_URL and
// does not call openBrowser.
func runLoopbackOIDC(cmd *cobra.Command, cfg auth.Config, oauthCfg *oauth2.Config, noBrowser bool, failStatus, successHTML string) (*oauth2.Token, error) {
	pkce, err := auth.GeneratePKCE()
	if err != nil {
		return nil, fmt.Errorf("generate PKCE: %w", err)
	}

	state, err := auth.GenerateOAuthState()
	if err != nil {
		return nil, err
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("start callback listener: %w", err)
	}
	defer lis.Close()

	cfgCopy := *oauthCfg
	oauthCfg = &cfgCopy
	oauthCfg.RedirectURL = fmt.Sprintf("http://127.0.0.1:%d/callback", lis.Addr().(*net.TCPAddr).Port)

	authURL := oauthCfg.AuthCodeURL(state,
		oauth2.SetAuthURLParam("code_challenge", pkce.Challenge),
		oauth2.SetAuthURLParam("code_challenge_method", pkce.ChallengeMethod),
	)

	fmt.Fprintf(cmd.OutOrStdout(), "AUTH_URL %s\nWaiting for callback...\n", authURL)
	if !noBrowser {
		if err := openBrowser(authURL); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Failed to open browser: %v\nPlease open the URL manually.\n", err)
		}
	}

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("state"); got != state {
			errCh <- fmt.Errorf("callback state mismatch")
			http.Error(w, failStatus, http.StatusBadRequest)
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			errMsg := r.URL.Query().Get("error")
			if errMsg == "" {
				errMsg = "no authorization code in callback"
			}
			errCh <- fmt.Errorf("callback error: %s", errMsg)
			http.Error(w, failStatus, http.StatusBadRequest)
			return
		}
		codeCh <- code
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, successHTML)
	})

	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	var code string
	select {
	case code = <-codeCh:
	case err := <-errCh:
		return nil, err
	case <-cmd.Context().Done():
		return nil, cmd.Context().Err()
	}

	_ = server.Shutdown(context.Background())

	exchangeCtx, err := withOIDCHTTPClient(cmd.Context(), cfg)
	if err != nil {
		return nil, err
	}

	tok, err := oauthCfg.Exchange(exchangeCtx, code,
		oauth2.SetAuthURLParam("code_verifier", pkce.Verifier),
	)
	if err != nil {
		return nil, fmt.Errorf("exchange code for token: %w", err)
	}
	return tok, nil
}
