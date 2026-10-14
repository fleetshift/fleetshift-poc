package kind

import (
	"context"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestMemoryGenerationStore_CheckAndAdvance(t *testing.T) {
	s := NewMemoryGenerationStore()
	ctx := context.Background()

	d, g, err := s.CheckAndAdvance(ctx, "fs--demo", nil, 1)
	if err != nil || d != GenerationCreated || g != 1 {
		t.Fatalf("first advance: disp=%v gen=%d err=%v", d, g, err)
	}
	d, g, err = s.CheckAndAdvance(ctx, "fs--demo", nil, 1)
	if err != nil || d != GenerationSame || g != 1 {
		t.Fatalf("same: disp=%v gen=%d err=%v", d, g, err)
	}
	d, g, err = s.CheckAndAdvance(ctx, "fs--demo", nil, 3)
	if err != nil || d != GenerationAdvanced || g != 3 {
		t.Fatalf("advance: disp=%v gen=%d err=%v", d, g, err)
	}
	d, g, err = s.CheckAndAdvance(ctx, "fs--demo", nil, 2)
	if err != nil || d != GenerationStale || g != 3 {
		t.Fatalf("stale: disp=%v gen=%d err=%v", d, g, err)
	}
}

func TestMemoryGenerationStore_ForgetClears(t *testing.T) {
	s := NewMemoryGenerationStore()
	ctx := context.Background()
	_, _, _ = s.CheckAndAdvance(ctx, "fs--demo", nil, 2)
	s.Forget("fs--demo")
	_, found, err := s.Get(ctx, "fs--demo", nil)
	if err != nil || found {
		t.Fatalf("after Forget: found=%v err=%v", found, err)
	}
}

func TestMemoryGenerationStore_NeverMovesBackwardConcurrent(t *testing.T) {
	s := NewMemoryGenerationStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for gen := domain.Generation(1); gen <= 20; gen++ {
		wg.Add(1)
		go func(g domain.Generation) {
			defer wg.Done()
			_, _, _ = s.CheckAndAdvance(ctx, "fs--demo", nil, g)
		}(gen)
	}
	wg.Wait()
	recorded, found, err := s.Get(ctx, "fs--demo", nil)
	if err != nil || !found || recorded != 20 {
		t.Fatalf("recorded=%d found=%v err=%v, want 20", recorded, found, err)
	}
}
