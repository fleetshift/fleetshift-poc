package domain

import (
	"context"
	"fmt"
)

// StaticPlacement selects an explicit set of targets by ID.
type StaticPlacement struct {
	Targets []TargetID
}

func (s *StaticPlacement) Resolve(_ context.Context, pool []TargetInfo) ([]TargetInfo, error) {
	index := make(map[TargetID]TargetInfo, len(pool))
	for _, t := range pool {
		index[t.ID] = t
	}

	result := make([]TargetInfo, 0, len(s.Targets))
	for _, id := range s.Targets {
		t, ok := index[id]
		if !ok {
			return nil, fmt.Errorf("%w: target %q not found in pool", ErrNotFound, id)
		}
		result = append(result, t)
	}
	return result, nil
}

// AllPlacement selects every target in the pool.
type AllPlacement struct{}

func (a *AllPlacement) Resolve(_ context.Context, pool []TargetInfo) ([]TargetInfo, error) {
	result := make([]TargetInfo, len(pool))
	copy(result, pool)
	return result, nil
}

// SelectorPlacement filters the pool by label matching. All labels in the
// selector must be present and equal on the target.
type SelectorPlacement struct {
	Selector TargetSelector
}

func (s *SelectorPlacement) Resolve(_ context.Context, pool []TargetInfo) ([]TargetInfo, error) {
	var result []TargetInfo
	for _, t := range pool {
		if matchLabels(t.Labels, s.Selector.MatchLabels) {
			result = append(result, t)
		}
	}
	return result, nil
}

func matchLabels(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}
