package auth

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/oauth2"
)

// Tokens holds the OAuth2 tokens obtained from a login flow.
type Tokens struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	IDToken      string    `json:"id_token,omitempty"`
	Expiry       time.Time `json:"expiry"`
	TokenType    string    `json:"token_type"`
}

// TokenStore persists OAuth tokens.
type TokenStore interface {
	// Save writes tokens, replacing any previously stored set.
	Save(ctx context.Context, tokens Tokens) error
	// Load returns the stored tokens. Missing storage is an error.
	Load(ctx context.Context) (Tokens, error)
	// Clear removes stored OAuth tokens.
	Clear(ctx context.Context) error
}

// Store persists OAuth tokens and the PEM signing key.
type Store interface {
	TokenStore
	// SaveSigningKey stores PEM private-key bytes. Implementations do not parse PEM.
	SaveSigningKey(pemData string) error
	// LoadSigningKey returns the stored PEM bytes. It does not parse or validate PEM.
	LoadSigningKey() (string, error)
}

// TokensFrom copies tok into Tokens, including id_token when present.
func TokensFrom(tok *oauth2.Token) Tokens {
	tokens := Tokens{
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		TokenType:    tok.TokenType,
		Expiry:       tok.Expiry,
	}
	if idTok, ok := tok.Extra("id_token").(string); ok {
		tokens.IDToken = idTok
	}
	return tokens
}

// RefreshIfNeeded refreshes the token using cfg when fewer than 30 seconds
// remain until expiry and a refresh token is present. Tokens with more than
// 30 seconds remaining, or with no refresh token, are returned unchanged.
// Returns true if a refresh was performed.
func RefreshIfNeeded(ctx context.Context, store TokenStore, cfg *oauth2.Config) (Tokens, bool, error) {
	tokens, err := store.Load(ctx)
	if err != nil {
		return Tokens{}, false, fmt.Errorf("load tokens: %w", err)
	}

	if time.Until(tokens.Expiry) > 30*time.Second {
		return tokens, false, nil
	}

	if tokens.RefreshToken == "" {
		return tokens, false, nil
	}

	src := cfg.TokenSource(ctx, &oauth2.Token{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		TokenType:    tokens.TokenType,
		Expiry:       tokens.Expiry,
	})

	newTok, err := src.Token()
	if err != nil {
		return Tokens{}, false, fmt.Errorf("refresh token: %w", err)
	}

	refreshed := TokensFrom(newTok)

	if err := store.Save(ctx, refreshed); err != nil {
		return Tokens{}, false, fmt.Errorf("save refreshed tokens: %w", err)
	}

	return refreshed, true, nil
}
