package oidc_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

func TestVerifier_ValidToken(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	rawToken := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-123"})

	claims, err := verifier.Verify(ctx, idp.OIDCConfig(), rawToken)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if claims.Subject != "user-123" {
		t.Errorf("Subject: got %q, want %q", claims.Subject, "user-123")
	}
	if claims.Issuer != idp.IssuerURL() {
		t.Errorf("Issuer: got %q, want %q", claims.Issuer, idp.IssuerURL())
	}
}

func TestVerifier_ExpiredToken(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	rawToken := idp.IssueToken(t, oidctest.TokenClaims{
		Subject: "user-123",
		Expiry:  -time.Hour,
	})

	_, err = verifier.Verify(ctx, idp.OIDCConfig(), rawToken)
	if err == nil {
		t.Fatal("Verify: expected error for expired token, got nil")
	}
}

func TestVerifier_WrongIssuer(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	// Issue a token, then verify against a config with a different issuer.
	rawToken := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-123"})

	config := idp.OIDCConfig()
	config.IssuerURL = "https://wrong-issuer"

	_, err = verifier.Verify(ctx, config, rawToken)
	if err == nil {
		t.Fatal("Verify: expected error for wrong issuer, got nil")
	}

	errMsg := err.Error()
	actualIssuer := string(idp.IssuerURL())
	if !strings.Contains(errMsg, actualIssuer) {
		t.Errorf("error should contain token's actual issuer %q, got: %s", actualIssuer, errMsg)
	}
	if !strings.Contains(errMsg, "https://wrong-issuer") {
		t.Errorf("error should contain expected issuer %q, got: %s", "https://wrong-issuer", errMsg)
	}
	if !strings.Contains(errMsg, "expected:") || !strings.Contains(errMsg, "got:") {
		t.Errorf("error should contain expected/got diagnostics, got: %s", errMsg)
	}
}

// jwksFlappingProxy serves cached JWKS bytes only while up is true.
func jwksFlappingProxy(t *testing.T, jwksBody []byte, up *atomic.Bool) *httptest.Server {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksBody)
	}))
	t.Cleanup(proxy.Close)
	return proxy
}

func fetchJWKSBody(t *testing.T, client *http.Client, jwksURI string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, jwksURI, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("fetch JWKS: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read JWKS: %v", err)
	}
	return body
}

func TestVerifier_RegisterKeySetFailsFastWhenJWKSUnavailable(t *testing.T) {
	// Cement fail-fast: a 503 must not burn the caller's full deadline waiting
	// on httprc Ready() (which only unblocks on first success or ctx cancel).
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))
	var up atomic.Bool
	proxy := jwksFlappingProxy(t, fetchJWKSBody(t, idp.HTTPClient(), string(idp.OIDCConfig().JWKSURI)), &up)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(&http.Client{
		Timeout: 500 * time.Millisecond,
	}))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	up.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = verifier.RegisterKeySet(ctx, domain.EndpointURL(proxy.URL+"/jwks"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected RegisterKeySet error while JWKS down")
	}
	if elapsed > time.Second {
		t.Fatalf("RegisterKeySet took %v while JWKS returned 503; want fail-fast (<1s), not Ready()-until-deadline", elapsed)
	}
}

func TestVerifier_RegisterKeySetDoesNotBlockOtherURIs(t *testing.T) {
	// A slow/hung registration for URI A must not delay registration for URI B.
	// Before the fix, v.mu was held across Fetch, serializing all URIs.
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))
	jwksBody := fetchJWKSBody(t, idp.HTTPClient(), string(idp.OIDCConfig().JWKSURI))

	releaseA := make(chan struct{})
	proxyA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-releaseA:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(jwksBody)
		case <-r.Context().Done():
			return
		}
	}))
	t.Cleanup(proxyA.Close)

	proxyB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksBody)
	}))
	t.Cleanup(proxyB.Close)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(&http.Client{
		Timeout: 5 * time.Second,
	}))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	errA := make(chan error, 1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errA <- verifier.RegisterKeySet(ctx, domain.EndpointURL(proxyA.URL+"/jwks"))
	}()

	// Give URI A time to enter Fetch and hold (without holding a global lock).
	time.Sleep(50 * time.Millisecond)

	startB := time.Now()
	ctxB, cancelB := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelB()
	if err := verifier.RegisterKeySet(ctxB, domain.EndpointURL(proxyB.URL+"/jwks")); err != nil {
		t.Fatalf("RegisterKeySet B while A blocked: %v", err)
	}
	if elapsed := time.Since(startB); elapsed > time.Second {
		t.Fatalf("RegisterKeySet B took %v while A was blocked; want independent progress", elapsed)
	}

	close(releaseA)
	wg.Wait()
	// A may succeed after release or time out depending on scheduling; either is fine.
	_ = <-errA
}

func TestVerifier_RegisterKeySetShortLeaderDoesNotPoisonWaiter(t *testing.T) {
	// Same-URI registration must use each caller's ctx. A short-lived first
	// attempt must not cause a concurrent waiter with a healthy budget to fail.
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))
	jwksBody := fetchJWKSBody(t, idp.HTTPClient(), string(idp.OIDCConfig().JWKSURI))

	var entered atomic.Int32
	hold := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Add(1)
		select {
		case <-hold:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(jwksBody)
		case <-r.Context().Done():
			return
		}
	}))
	t.Cleanup(proxy.Close)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(&http.Client{
		Timeout: 5 * time.Second,
	}))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	uri := domain.EndpointURL(proxy.URL + "/jwks")

	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer leaderCancel()

	var wg sync.WaitGroup
	wg.Add(2)
	errLeader := make(chan error, 1)
	errFollower := make(chan error, 1)

	go func() {
		defer wg.Done()
		errLeader <- verifier.RegisterKeySet(leaderCtx, uri)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for entered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if entered.Load() == 0 {
		t.Fatal("leader never entered JWKS handler")
	}

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errFollower <- verifier.RegisterKeySet(ctx, uri)
	}()

	// Expire the leader while still held, then release so the waiter's own
	// attempt can fetch successfully with its healthy context.
	time.Sleep(150 * time.Millisecond)
	close(hold)
	wg.Wait()

	if err := <-errLeader; err == nil {
		t.Fatal("expected leader RegisterKeySet to fail after short deadline")
	}
	if err := <-errFollower; err != nil {
		t.Fatalf("follower RegisterKeySet: %v (must not inherit leader ctx failure)", err)
	}
}

func TestVerifier_RegisterKeySetRecoversAfterInitialFetchFailure(t *testing.T) {
	// Cement on-demand recovery: repeated failed RegisterKeySet (IdP/JWKS
	// down) must not poison the URI or the httprc worker pool; a later
	// Verify (getKeySet → RegisterKeySet) succeeds. Uses >DefaultWorkers(5)
	// failures so a Refresh-based approach that kills workers on sync
	// failure would permanently break recovery.
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))
	cfg := idp.OIDCConfig()
	var up atomic.Bool
	proxy := jwksFlappingProxy(t, fetchJWKSBody(t, idp.HTTPClient(), string(cfg.JWKSURI)), &up)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(&http.Client{
		Timeout: 500 * time.Millisecond,
	}))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	jwksURI := domain.EndpointURL(proxy.URL + "/jwks")
	cfg.JWKSURI = jwksURI
	up.Store(false)
	for i := 0; i < 10; i++ {
		downCtx, downCancel := context.WithTimeout(context.Background(), time.Second)
		err = verifier.RegisterKeySet(downCtx, jwksURI)
		downCancel()
		if err == nil {
			t.Fatalf("iteration %d: expected RegisterKeySet error while JWKS down", i)
		}
	}

	up.Store(true)
	upCtx, upCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer upCancel()
	raw := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-1"})
	// Recover through Verify only — production auth path, not an explicit
	// second RegisterKeySet call from the test.
	if _, err := verifier.Verify(upCtx, cfg, raw); err != nil {
		t.Fatalf("Verify after recovery: %v", err)
	}
}

func TestVerifier_WrongAudience(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	rawToken := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-123"})

	config := domain.OIDCConfig{
		IssuerURL: idp.IssuerURL(),
		Audience:  "wrong-audience",
		JWKSURI:   idp.OIDCConfig().JWKSURI,
	}

	_, err = verifier.Verify(ctx, config, rawToken)
	if err == nil {
		t.Fatal("Verify: expected error for wrong audience, got nil")
	}
}
