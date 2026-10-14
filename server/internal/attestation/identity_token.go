package attestation

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"

	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

// jwksFetcher lazily fetches and caches JWKS key sets using an
// injected [http.Client]. Each [Verifier] owns its own fetcher,
// avoiding package-level global state.
type jwksFetcher struct {
	httpClient *http.Client

	mu      sync.Mutex
	cache   *jwk.Cache
	keySets map[string]jwk.Set
}

func newJWKSFetcher(httpClient *http.Client) *jwksFetcher {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &jwksFetcher{
		httpClient: httpClient,
		keySets:    make(map[string]jwk.Set),
	}
}

func (f *jwksFetcher) getKeySet(ctx context.Context, jwksURI string) (jwk.Set, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ks, ok := f.keySets[jwksURI]; ok {
		return ks, nil
	}

	if f.cache == nil {
		client := httprc.NewClient(httprc.WithHTTPClient(f.httpClient))
		cache, err := jwk.NewCache(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("create JWK cache: %w", err)
		}
		f.cache = cache
	}

	if err := f.cache.Register(ctx, jwksURI); err != nil {
		return nil, fmt.Errorf("register JWKS URI %s: %w", jwksURI, err)
	}
	cached, err := f.cache.CachedSet(jwksURI)
	if err != nil {
		return nil, fmt.Errorf("create cached set for %s: %w", jwksURI, err)
	}
	f.keySets[jwksURI] = cached
	return cached, nil
}

// verifyIdentityToken verifies an identity token's cryptographic
// signature against the issuer's JWKS and checks the audience claim.
// Temporal claims (exp, iat, nbf) are NOT validated because the
// identity token was issued at key enrollment time and is typically
// expired by delivery time. The agent only confirms the IdP vouched
// for this signer.
func (v *Verifier) verifyIdentityToken(ctx context.Context, rawToken string, issuer TrustedIssuer) error {
	keySet, err := v.jwks.getKeySet(ctx, string(issuer.JWKSURI))
	if err != nil {
		return fmt.Errorf("fetch JWKS for %s: %w", issuer.JWKSURI, err)
	}

	tok, err := jwt.ParseString(rawToken,
		jwt.WithKeySet(keySet),
		jwt.WithValidate(false),
	)
	if err != nil {
		return fmt.Errorf("parse/verify identity token: %w", err)
	}

	if issuer.Audience != "" {
		aud, _ := tok.Audience()
		if !slices.Contains(aud, string(issuer.Audience)) {
			return fmt.Errorf("identity token audience %v does not contain %q", aud, issuer.Audience)
		}
	}
	return nil
}

// extractSubjectFromToken parses a JWT without signature verification
// to extract the sub claim. Called after verifyIdentityToken has
// already confirmed the signature.
func extractSubjectFromToken(rawToken string) (string, error) {
	tok, err := jwt.ParseString(rawToken, jwt.WithVerify(false), jwt.WithValidate(false))
	if err != nil {
		return "", fmt.Errorf("parse token for subject: %w", err)
	}
	sub, ok := tok.Subject()
	if !ok {
		return "", fmt.Errorf("token has no sub claim")
	}
	return sub, nil
}
