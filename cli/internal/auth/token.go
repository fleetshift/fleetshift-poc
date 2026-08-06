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
	Save(ctx context.Context, tokens Tokens) error
	Load(ctx context.Context) (Tokens, error)
	Clear(ctx context.Context) error
}

// RefreshIfNeeded refreshes the token using the given OAuth2 config if it
// is expired and a refresh token is available. Returns true if a refresh
// was performed.
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

	refreshed := Tokens{
		AccessToken:  newTok.AccessToken,
		RefreshToken: newTok.RefreshToken,
		TokenType:    newTok.TokenType,
		Expiry:       newTok.Expiry,
	}
	if idTok, ok := newTok.Extra("id_token").(string); ok {
		refreshed.IDToken = idTok
	}

	if err := store.Save(ctx, refreshed); err != nil {
		return Tokens{}, false, fmt.Errorf("save refreshed tokens: %w", err)
	}

	return refreshed, true, nil
}
