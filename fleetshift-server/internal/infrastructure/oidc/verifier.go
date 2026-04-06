package oidc

import (
	"context"
	"fmt"
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
	cache *jwk.Cache

	mu      sync.RWMutex
	keySets map[string]jwk.Set // jwksURI -> cached set
}

// VerifierOption configures a [Verifier].
type VerifierOption func(*verifierConfig)

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
		cache:   cache,
		keySets: make(map[string]jwk.Set),
	}, nil
}

// RegisterKeySet registers a JWKS URI with the background cache so keys
// are refreshed automatically. Call this on startup for persisted auth
// methods and after creating new ones.
func (v *Verifier) RegisterKeySet(ctx context.Context, jwksURI domain.EndpointURL) error {
	uri := string(jwksURI)
	v.mu.Lock()
	defer v.mu.Unlock()

	if _, ok := v.keySets[uri]; ok {
		return nil
	}

	if err := v.cache.Register(ctx, uri); err != nil {
		return fmt.Errorf("register JWKS URI %s: %w", uri, err)
	}
	cached, err := v.cache.CachedSet(uri)
	if err != nil {
		return fmt.Errorf("create cached set for %s: %w", uri, err)
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
		return domain.SubjectClaims{}, fmt.Errorf("parse/verify token: %w", err)
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

func (v *Verifier) getKeySet(ctx context.Context, jwksURI domain.EndpointURL) (jwk.Set, error) {
	uri := string(jwksURI)
	v.mu.RLock()
	ks, ok := v.keySets[uri]
	v.mu.RUnlock()
	if ok {
		return ks, nil
	}

	if err := v.RegisterKeySet(ctx, jwksURI); err != nil {
		return nil, err
	}

	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.keySets[uri], nil
}
