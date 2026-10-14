package observability_test

import (
	"context"
	"log/slog"
	"sync"
)

// recordingHandler is a [slog.Handler] that collects log records
// in memory for test assertions. It respects the configured level.
// All child handlers created via WithAttrs/WithGroup share the same
// record store.
type recordingHandler struct {
	opts  slog.HandlerOptions
	attrs []slog.Attr
	group string
	store *recordStore
}

type recordStore struct {
	mu      sync.Mutex
	records []slog.Record
}

func newRecordingHandler(opts *slog.HandlerOptions) *recordingHandler {
	h := &recordingHandler{store: &recordStore{}}
	if opts != nil {
		h.opts = *opts
	}
	return h
}

func (h *recordingHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	h.store.records = append(h.store.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &recordingHandler{
		opts:  h.opts,
		attrs: append(h.attrs, attrs...),
		group: h.group,
		store: h.store,
	}
}

func (h *recordingHandler) WithGroup(name string) slog.Handler {
	return &recordingHandler{
		opts:  h.opts,
		attrs: h.attrs,
		group: name,
		store: h.store,
	}
}

func (h *recordingHandler) Records() []slog.Record {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	out := make([]slog.Record, len(h.store.records))
	copy(out, h.store.records)
	return out
}
