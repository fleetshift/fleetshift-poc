package auth_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestTokensFrom_CopiesFieldsAndIDToken(t *testing.T) {
	expiry := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	tok := (&oauth2.Token{
		AccessToken:  "access",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       expiry,
	}).WithExtra(map[string]any{"id_token": "id-jwt"})

	got := auth.TokensFrom(tok)
	if got.AccessToken != "access" || got.RefreshToken != "refresh" || got.TokenType != "Bearer" {
		t.Fatalf("TokensFrom() = %#v", got)
	}
	if !got.Expiry.Equal(expiry) {
		t.Fatalf("Expiry = %v, want %v", got.Expiry, expiry)
	}
	if got.IDToken != "id-jwt" {
		t.Fatalf("IDToken = %q, want id-jwt", got.IDToken)
	}
}

func TestTokensFrom_OmitsNonStringIDToken(t *testing.T) {
	tok := (&oauth2.Token{AccessToken: "access", TokenType: "Bearer"}).
		WithExtra(map[string]any{"id_token": 1})
	got := auth.TokensFrom(tok)
	if got.IDToken != "" {
		t.Fatalf("IDToken = %q, want empty for non-string extra", got.IDToken)
	}
}

func TestRefreshIfNeeded_LoadError(t *testing.T) {
	_, _, err := auth.RefreshIfNeeded(context.Background(), &tokenStoreStub{loadErr: errors.New("missing")}, &oauth2.Config{})
	if err == nil || !strings.Contains(err.Error(), "load tokens") {
		t.Fatalf("error = %v, want load tokens", err)
	}
}

func TestRefreshIfNeeded_StillValidSkipsRefresh(t *testing.T) {
	store := &tokenStoreStub{tokens: auth.Tokens{
		AccessToken:  "current",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("token endpoint must not be called for a still-valid token")
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	got, refreshed, err := auth.RefreshIfNeeded(context.Background(), store, &oauth2.Config{
		ClientID: "fleetshift-cli",
		Endpoint: oauth2.Endpoint{TokenURL: srv.URL},
	})
	if err != nil {
		t.Fatalf("RefreshIfNeeded: %v", err)
	}
	if refreshed {
		t.Fatal("refreshed = true, want false")
	}
	if got.AccessToken != "current" {
		t.Fatalf("AccessToken = %q, want current", got.AccessToken)
	}
}

func TestRefreshIfNeeded_ExpiredWithoutRefreshToken(t *testing.T) {
	original := auth.Tokens{
		AccessToken: "expired",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(-time.Minute),
	}
	store := &tokenStoreStub{tokens: original}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("token endpoint must not be called without a refresh token")
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	got, refreshed, err := auth.RefreshIfNeeded(context.Background(), store, &oauth2.Config{
		ClientID: "fleetshift-cli",
		Endpoint: oauth2.Endpoint{TokenURL: srv.URL},
	})
	if err != nil {
		t.Fatalf("RefreshIfNeeded: %v", err)
	}
	if refreshed {
		t.Fatal("refreshed = true, want false")
	}
	if got.AccessToken != original.AccessToken {
		t.Fatalf("AccessToken = %q, want %q", got.AccessToken, original.AccessToken)
	}
}

func TestRefreshIfNeeded_RefreshesAndSavesIDToken(t *testing.T) {
	store := &tokenStoreStub{tokens: auth.Tokens{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Minute),
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "old-refresh" {
			http.Error(w, "unexpected refresh", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"new-access","token_type":"Bearer","expires_in":3600,"refresh_token":"new-refresh","id_token":"new-id"}`)
	}))
	t.Cleanup(srv.Close)

	got, refreshed, err := auth.RefreshIfNeeded(context.Background(), store, &oauth2.Config{
		ClientID: "fleetshift-cli",
		Endpoint: oauth2.Endpoint{
			TokenURL:  srv.URL,
			AuthStyle: oauth2.AuthStyleInParams,
		},
	})
	if err != nil {
		t.Fatalf("RefreshIfNeeded: %v", err)
	}
	if !refreshed {
		t.Fatal("refreshed = false, want true")
	}
	if got.AccessToken != "new-access" || got.RefreshToken != "new-refresh" || got.IDToken != "new-id" {
		t.Fatalf("refreshed tokens = %#v", got)
	}
	if store.tokens.AccessToken != "new-access" || store.tokens.IDToken != "new-id" {
		t.Fatalf("store not updated: %#v", store.tokens)
	}
}

func TestRefreshIfNeeded_RefreshError(t *testing.T) {
	store := &tokenStoreStub{tokens: auth.Tokens{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Minute),
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	_, _, err := auth.RefreshIfNeeded(context.Background(), store, &oauth2.Config{
		ClientID: "fleetshift-cli",
		Endpoint: oauth2.Endpoint{TokenURL: srv.URL, AuthStyle: oauth2.AuthStyleInParams},
	})
	if err == nil || !strings.Contains(err.Error(), "refresh token") {
		t.Fatalf("error = %v, want refresh token", err)
	}
}

func TestRefreshIfNeeded_SaveError(t *testing.T) {
	store := &tokenStoreStub{
		tokens: auth.Tokens{
			AccessToken:  "old-access",
			RefreshToken: "old-refresh",
			TokenType:    "Bearer",
			Expiry:       time.Now().Add(-time.Minute),
		},
		saveErr: errors.New("disk full"),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"new-access","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(srv.Close)

	_, _, err := auth.RefreshIfNeeded(context.Background(), store, &oauth2.Config{
		ClientID: "fleetshift-cli",
		Endpoint: oauth2.Endpoint{TokenURL: srv.URL, AuthStyle: oauth2.AuthStyleInParams},
	})
	if err == nil || !strings.Contains(err.Error(), "save refreshed tokens") {
		t.Fatalf("error = %v, want save refreshed tokens", err)
	}
}

type tokenStoreStub struct {
	tokens  auth.Tokens
	loadErr error
	saveErr error
}

func (s *tokenStoreStub) Save(_ context.Context, tokens auth.Tokens) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.tokens = tokens
	return nil
}

func (s *tokenStoreStub) Load(_ context.Context) (auth.Tokens, error) {
	if s.loadErr != nil {
		return auth.Tokens{}, s.loadErr
	}
	return s.tokens, nil
}

func (s *tokenStoreStub) Clear(context.Context) error { return nil }
