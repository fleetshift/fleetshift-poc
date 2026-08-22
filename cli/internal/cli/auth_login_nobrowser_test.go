package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestAuthLogin_NoBrowserFlagRegistered(t *testing.T) {
	login, _, err := New().Find([]string{"auth", "login"})
	if err != nil {
		t.Fatalf("Find auth login: %v", err)
	}
	f := login.Flags().Lookup("no-browser")
	if f == nil {
		t.Fatal("--no-browser is not registered on auth login")
	}
	if f.DefValue != "false" {
		t.Fatalf("default = %q, want false", f.DefValue)
	}
	if login.Flags().Lookup("username") != nil || login.Flags().Lookup("password") != nil {
		t.Fatal("auth login must not register --username or --password")
	}
	if New().PersistentFlags().Lookup("no-browser") != nil {
		t.Fatal("--no-browser must not be a persistent flag")
	}
}

func TestAuthLogin_NoBrowserCompletesAndSavesTokens(t *testing.T) {
	tokenURL := startOIDCTokenServer(t)
	stateDir := seedLoginConfig(t, tokenURL)

	out, err := executeAuthLogin(t, stateDir, true, func(authURL string) {
		hitOIDCCallback(t, authURL, "test-code", "")
	})
	if err != nil {
		t.Fatalf("auth login: %v\nstdout=%s", err, out)
	}
	if !strings.Contains(out, "AUTH_URL ") {
		t.Fatalf("stdout = %q, want AUTH_URL", out)
	}
	if !strings.Contains(out, "Login successful!") {
		t.Fatalf("stdout = %q, want success", out)
	}

	got, err := (auth.FileStore{Dir: stateDir}).Load(context.Background())
	if err != nil {
		t.Fatalf("Load tokens: %v", err)
	}
	if got.AccessToken != "access-from-oidc" || got.RefreshToken != "refresh-from-oidc" || got.IDToken != "id-from-oidc" {
		t.Fatalf("saved tokens = %#v", got)
	}
}

func TestAuthLogin_CallbackStateMismatch(t *testing.T) {
	stateDir := seedLoginConfig(t, "https://issuer.example/idp/token")
	out, err := executeAuthLogin(t, stateDir, true, func(authURL string) {
		hitOIDCCallback(t, authURL, "test-code", "wrong-state")
	})
	if err == nil {
		t.Fatalf("expected state mismatch, stdout=%s", out)
	}
	if !strings.Contains(err.Error(), "callback state mismatch") {
		t.Fatalf("error = %v, want callback state mismatch", err)
	}
	if _, loadErr := (auth.FileStore{Dir: stateDir}).Load(context.Background()); loadErr == nil {
		t.Fatal("must not save tokens after state mismatch")
	}
}

func TestAuthLogin_CallbackMissingCode(t *testing.T) {
	stateDir := seedLoginConfig(t, "https://issuer.example/idp/token")
	out, err := executeAuthLogin(t, stateDir, true, func(authURL string) {
		hitOIDCCallback(t, authURL, "", "")
	})
	if err == nil {
		t.Fatalf("expected missing code error, stdout=%s", out)
	}
	if !strings.Contains(err.Error(), "callback error") {
		t.Fatalf("error = %v, want callback error", err)
	}
	if _, loadErr := (auth.FileStore{Dir: stateDir}).Load(context.Background()); loadErr == nil {
		t.Fatal("must not save tokens after missing code")
	}
}

func TestAuthLogin_NoBrowserSkipsOpenBrowser(t *testing.T) {
	called := stubOpenBrowser(t)
	tokenURL := startOIDCTokenServer(t)
	out, err := executeAuthLogin(t, seedLoginConfig(t, tokenURL), true, func(authURL string) {
		hitOIDCCallback(t, authURL, "test-code", "")
	})
	if err != nil {
		t.Fatalf("auth login: %v\nstdout=%s", err, out)
	}
	if called() {
		t.Fatal("OpenBrowser must not run with --no-browser")
	}
}

func TestAuthLogin_OpensBrowserByDefault(t *testing.T) {
	called := stubOpenBrowser(t)
	tokenURL := startOIDCTokenServer(t)
	out, err := executeAuthLogin(t, seedLoginConfig(t, tokenURL), false, func(authURL string) {
		hitOIDCCallback(t, authURL, "test-code", "")
	})
	if err != nil {
		t.Fatalf("auth login: %v\nstdout=%s", err, out)
	}
	if !called() {
		t.Fatal("OpenBrowser must run when --no-browser is unset")
	}
	if !strings.Contains(out, "AUTH_URL ") {
		t.Fatalf("stdout = %q, want AUTH_URL even when the system browser is opened", out)
	}
}

func stubOpenBrowser(t *testing.T) func() bool {
	t.Helper()
	orig := openBrowser
	called := false
	openBrowser = func(string) error {
		called = true
		return nil
	}
	t.Cleanup(func() { openBrowser = orig })
	return func() bool { return called }
}

func startOIDCTokenServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.Form.Get("code") == "" {
			http.Error(w, "missing code", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"access-from-oidc","token_type":"Bearer","expires_in":3600,"refresh_token":"refresh-from-oidc","id_token":"id-from-oidc"}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func seedLoginConfig(t *testing.T, tokenURL string) string {
	t.Helper()
	stateDir := t.TempDir()
	if err := auth.SaveConfigTo(stateDir, auth.Config{
		IssuerURL:             "https://issuer.example/idp",
		ClientID:              "fleetshift-cli",
		Scopes:                []string{"openid"},
		AuthorizationEndpoint: "https://issuer.example/idp/auth",
		TokenEndpoint:         tokenURL,
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	return stateDir
}

func executeAuthLogin(t *testing.T, stateDir string, noBrowser bool, afterAuthURL func(string)) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"--config-dir", stateDir, "--insecure-storage", "auth", "login"}
	if noBrowser {
		args = append(args, "--no-browser")
	}

	w := newAuthURLCapture()
	cmd := New()
	cmd.SetOut(w)
	cmd.SetErr(w)
	cmd.SetArgs(args)

	errCh := make(chan error, 1)
	go func() { errCh <- cmd.ExecuteContext(ctx) }()

	select {
	case authURL := <-w.authCh:
		afterAuthURL(authURL)
	case err := <-errCh:
		return w.String(), err
	case <-ctx.Done():
		return w.String(), fmt.Errorf("timeout waiting for AUTH_URL: %s", w.String())
	}

	select {
	case err := <-errCh:
		return w.String(), err
	case <-ctx.Done():
		return w.String(), fmt.Errorf("timeout waiting for login to finish: %s", w.String())
	}
}

func hitOIDCCallback(t *testing.T, authURL, code, stateOverride string) {
	t.Helper()
	u, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("parse AUTH_URL: %v", err)
	}
	state := stateOverride
	if state == "" {
		state = u.Query().Get("state")
	}
	redirect := u.Query().Get("redirect_uri")
	if redirect == "" {
		t.Fatalf("AUTH_URL missing redirect_uri: %s", authURL)
	}
	cb, err := url.Parse(redirect)
	if err != nil {
		t.Fatalf("redirect_uri: %v", err)
	}
	q := cb.Query()
	if code != "" {
		q.Set("code", code)
	}
	q.Set("state", state)
	cb.RawQuery = q.Encode()

	client := &http.Client{Timeout: 3 * time.Second}
	var resp *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for {
		var err error
		resp, err = client.Get(cb.String())
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback GET: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

type authURLCapture struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	authCh chan string
	once   sync.Once
}

func newAuthURLCapture() *authURLCapture {
	return &authURLCapture{authCh: make(chan string, 1)}
}

func (w *authURLCapture) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	if u, ok := authURLFromOutput(w.buf.String()); ok {
		w.once.Do(func() { w.authCh <- u })
	}
	return n, err
}

func (w *authURLCapture) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func authURLFromOutput(stdout string) (string, bool) {
	for _, line := range strings.Split(stdout, "\n") {
		rest, ok := strings.CutPrefix(line, "AUTH_URL ")
		if ok && rest != "" {
			return rest, true
		}
	}
	return "", false
}
