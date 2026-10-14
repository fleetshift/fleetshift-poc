package oidc

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Verifier implements [domain.OIDCTokenVerifier] using lestrrat-go/jwx.
// It manages a [jwk.Cache] internally for JWKS auto-refresh.
type Verifier struct {
	cache      *jwk.Cache
	httpClient httprc.HTTPClient // optional; nil means http.DefaultClient for preflight

	mu      sync.RWMutex
	keySets map[string]jwk.Set // jwksURI -> cached set

	// registerMu guards registerLocks. Per-URI mutexes serialize registration
	// for one JWKS URL without holding v.mu across network I/O, and without
	// sharing one caller's context across waiters (unlike singleflight).
	// registerLocks is keyed by configured auth-method JWKS URIs only (not
	// request-supplied). Entries are never removed; cardinality is bounded by
	// the number of OIDC auth methods.
	//
	// TODO: During a prolonged IdP/JWKS outage, every Verify → getKeySet →
	// RegisterJWKS for an unpublished URI re-drives jwk.Fetch under the
	// per-URI mutex, so waiters queue and then each attempt another upstream
	// fetch. Consider a short negative-result cache (few seconds) so outage
	// load is bounded, while still allowing on-demand recovery when the IdP
	// returns (cool-down may delay first success by the TTL).
	registerMu    sync.Mutex
	registerLocks map[string]*sync.Mutex
}

// VerifierOption configures a [Verifier].
type VerifierOption func(*verifierConfig)

// verifierConfig holds optional settings collected by [VerifierOption]s.
type verifierConfig struct {
	httpClient httprc.HTTPClient
}

// WithHTTPClient sets the HTTP client used for JWKS fetching. This is
// useful for injecting custom CA trust (e.g., self-signed TLS in tests)
// or proxy configuration.
func WithHTTPClient(c httprc.HTTPClient) VerifierOption {
	return func(cfg *verifierConfig) { cfg.httpClient = c }
}

// NewVerifier creates a verifier with a background JWKS cache.
func NewVerifier(ctx context.Context, opts ...VerifierOption) (*Verifier, error) {
	var cfg verifierConfig
	for _, o := range opts {
		o(&cfg)
	}

	var clientOpts []httprc.NewClientOption
	if cfg.httpClient != nil {
		clientOpts = append(clientOpts, httprc.WithHTTPClient(cfg.httpClient))
	}
	client := httprc.NewClient(clientOpts...)
	cache, err := jwk.NewCache(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("create JWK cache: %w", err)
	}
	return &Verifier{
		cache:         cache,
		httpClient:    cfg.httpClient,
		keySets:       make(map[string]jwk.Set),
		registerLocks: make(map[string]*sync.Mutex),
	}, nil
}

// RegisterJWKS registers a JWKS URI with the background cache so keys
// are refreshed automatically. Called at process start for auth methods
// and on demand from Verify when keys were not cached at boot.
//
// Registration is fail-fast and recoverable:
//  1. Preflight fetch under ctx (surfaces 503/timeout immediately).
//  2. On success, register with httprc for background auto-refresh.
//  3. Publish into keySets only after the cache has a ready set.
//
// A failed attempt leaves keySets untouched and clears any partial httprc
// registration so a later call (including on-demand from Verify) can recover.
// We intentionally avoid cache.Refresh on the failure path: httprc workers
// exit on synchronous refresh failure, which permanently breaks the cache
// after a handful of down-IdP attempts.
//
// Network I/O runs outside v.mu. Same-URI callers serialize on a per-URI
// mutex and each attempt uses that caller's ctx; other URIs are not blocked.
func (v *Verifier) RegisterJWKS(ctx context.Context, jwksURI domain.EndpointURL) error {
	uri := string(jwksURI)
	if v.hasKeySet(uri) {
		return nil
	}

	uriMu := v.lockForURI(uri)
	uriMu.Lock()
	defer uriMu.Unlock()

	if v.hasKeySet(uri) {
		return nil
	}
	return v.doRegisterJWKS(ctx, uri)
}

// lockForURI returns the per-URI mutex used to serialize registration.
func (v *Verifier) lockForURI(uri string) *sync.Mutex {
	v.registerMu.Lock()
	defer v.registerMu.Unlock()
	m, ok := v.registerLocks[uri]
	if !ok {
		m = &sync.Mutex{}
		v.registerLocks[uri] = m
	}
	return m
}

// doRegisterJWKS performs preflight fetch, cache registration, and publish.
// It must not hold v.mu across network or cache I/O.
func (v *Verifier) doRegisterJWKS(ctx context.Context, uri string) error {
	if v.cache.IsRegistered(ctx, uri) {
		// Prior success that wasn't published yet (shouldn't happen), or a
		// stale registration left after a failed WaitReady — reuse if ready,
		// otherwise clear and continue.
		if _, err := v.cache.Lookup(ctx, uri); err == nil {
			return v.publishCachedSet(ctx, uri)
		}
		if err := v.cache.Unregister(ctx, uri); err != nil {
			return fmt.Errorf("unregister stale JWKS URI %s: %w", uri, err)
		}
	}

	// Preflight: httprc Ready() only unblocks on first *success* or ctx
	// cancel — a 503 never fails Register quickly. Fetch ourselves first.
	if _, err := jwk.Fetch(ctx, uri, v.fetchOptions()...); err != nil {
		return fmt.Errorf("fetch JWKS for %s: %w", uri, err)
	}

	// JWKS is reachable; register for auto-refresh. Default WaitReady waits
	// for httprc's own first fetch (second GET). Bound by the same ctx.
	if err := v.cache.Register(ctx, uri); err != nil {
		if v.cache.IsRegistered(ctx, uri) {
			_ = v.cache.Unregister(ctx, uri)
		}
		return fmt.Errorf("register JWKS URI %s: %w", uri, err)
	}

	return v.publishCachedSet(ctx, uri)
}

// fetchOptions returns jwk.Fetch options using the verifier's HTTP client,
// or http.DefaultClient when none was configured.
func (v *Verifier) fetchOptions() []jwk.FetchOption {
	client := v.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	return []jwk.FetchOption{jwk.WithHTTPClient(client)}
}

// hasKeySet reports whether uri is already published in keySets.
func (v *Verifier) hasKeySet(uri string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, ok := v.keySets[uri]
	return ok
}

// publishCachedSet stores a ready CachedSet in keySets. On CachedSet failure
// it unregisters the URI so a later RegisterJWKS can recover.
func (v *Verifier) publishCachedSet(ctx context.Context, uri string) error {
	cached, err := v.cache.CachedSet(uri)
	if err != nil {
		_ = v.cache.Unregister(ctx, uri)
		return fmt.Errorf("create cached set for %s: %w", uri, err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.keySets[uri]; ok {
		return nil
	}
	v.keySets[uri] = cached
	return nil
}

// Verify validates a JWT against the OIDC configuration.
func (v *Verifier) Verify(ctx context.Context, config domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	keySet, err := v.getKeySet(ctx, config.JWKSURI)
	if err != nil {
		return domain.SubjectClaims{}, fmt.Errorf("get key set: %w", err)
	}

	parseOpts := []jwt.ParseOption{
		jwt.WithKeySet(keySet),
		jwt.WithValidate(true),
		jwt.WithAcceptableSkew(30 * time.Second),
	}
	if config.IssuerURL != "" {
		parseOpts = append(parseOpts, jwt.WithIssuer(string(config.IssuerURL)))
	}
	if config.Audience != "" {
		parseOpts = append(parseOpts, jwt.WithAudience(string(config.Audience)))
	}

	tok, err := jwt.ParseString(rawToken, parseOpts...)
	if err != nil {
		return domain.SubjectClaims{}, fmt.Errorf("parse/verify token: %w%s",
			err, tokenDiagnostics(rawToken, config))
	}

	sub, _ := tok.Subject()
	iss, _ := tok.Issuer()

	claims := domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: domain.SubjectID(sub),
			Issuer:  domain.IssuerURL(iss),
		},
		Extra: make(map[string][]string),
	}

	var email string
	if err := tok.Get("email", &email); err == nil {
		claims.Extra["email"] = []string{email}
	}

	if groups := getStringSliceClaim(tok, "groups"); len(groups) > 0 {
		claims.Extra["groups"] = groups
	}

	var azp string
	if err := tok.Get("azp", &azp); err == nil {
		claims.Extra["azp"] = []string{azp}
	}

	return claims, nil
}

// getStringSliceClaim extracts a string-array private claim from a JWT.
// The jwx library deserializes JSON arrays as []interface{}, so a direct
// Get into *[]string fails. This helper converts element-by-element.
func getStringSliceClaim(tok jwt.Token, key string) []string {
	var raw interface{}
	if err := tok.Get(key, &raw); err != nil {
		return nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// tokenDiagnostics does a best-effort insecure parse of the token and returns
// a suffix string with claim values useful for debugging verification failures.
// Returns empty string if the token cannot be decoded.
func tokenDiagnostics(rawToken string, config domain.OIDCConfig) string {
	tok, err := jwt.ParseInsecure([]byte(rawToken))
	if err != nil {
		return ""
	}
	iss, _ := tok.Issuer()
	aud, _ := tok.Audience()
	return fmt.Sprintf(" [expected: iss=%q aud=%q, got: iss=%q aud=%v]",
		config.IssuerURL, config.Audience, iss, aud)
}

// getKeySet returns the cached key set for jwksURI, registering it on demand
// when it was not published at boot (IdP recovery path).
func (v *Verifier) getKeySet(ctx context.Context, jwksURI domain.EndpointURL) (jwk.Set, error) {
	uri := string(jwksURI)
	if ks, ok := v.lookupKeySet(uri); ok {
		return ks, nil
	}

	if err := v.RegisterJWKS(ctx, jwksURI); err != nil {
		return nil, err
	}

	ks, ok := v.lookupKeySet(uri)
	if !ok {
		return nil, fmt.Errorf("key set for %s missing after registration", uri)
	}
	return ks, nil
}

// lookupKeySet returns the published key set for uri under v.mu.
func (v *Verifier) lookupKeySet(uri string) (jwk.Set, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ks, ok := v.keySets[uri]
	return ks, ok
}
