package auth

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryTokenStore is a [TokenStore] backed by an in-memory map.
// Intended for use in tests.
type InMemoryTokenStore struct {
	mu     sync.RWMutex
	tokens *Tokens
}

func (s *InMemoryTokenStore) Save(_ context.Context, tokens Tokens) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = &tokens
	return nil
}

func (s *InMemoryTokenStore) Load(_ context.Context) (Tokens, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.tokens == nil {
		return Tokens{}, fmt.Errorf("no tokens stored")
	}
	return *s.tokens, nil
}

func (s *InMemoryTokenStore) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = nil
	return nil
}
