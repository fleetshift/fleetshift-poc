// Package slogutil provides [slog] handler utilities.
package slogutil

import (
	"context"
	"log/slog"
)

// ComponentName identifies a logical component for per-component log level
// overrides. It corresponds to the value of the "component" attribute
// attached by observer constructors (e.g. "deployment", "authn").
type ComponentName string

// LevelOverrideHandler wraps an [slog.Handler] and applies per-component log
// level overrides. When a child logger is created with a "component"
// attribute (via [slog.Logger.With]) whose value matches an entry in the
// override map, that child's effective level is set to the override value.
//
// Loggers without a matching component attribute use the base level.
//
// The inner handler's own level should be set to the minimum of the base
// level and all override levels so it never prematurely rejects records.
type LevelOverrideHandler struct {
	inner     slog.Handler
	base      slog.Level
	overrides map[ComponentName]slog.Level

	// effective is non-nil when a WithAttrs call matched a component override.
	effective *slog.Level
}

// NewLevelOverrideHandler returns a handler that delegates to inner but
// applies per-component level overrides. If overrides is empty, the handler
// still functions correctly (every logger uses base).
func NewLevelOverrideHandler(inner slog.Handler, base slog.Level, overrides map[ComponentName]slog.Level) *LevelOverrideHandler {
	return &LevelOverrideHandler{
		inner:     inner,
		base:      base,
		overrides: overrides,
	}
}

// Enabled reports whether the handler is willing to accept a record at the
// given level. If a component override is active, it uses that level;
// otherwise it falls back to the base level.
func (h *LevelOverrideHandler) Enabled(_ context.Context, level slog.Level) bool {
	if h.effective != nil {
		return level >= *h.effective
	}
	return level >= h.base
}

// Handle delegates to the inner handler.
func (h *LevelOverrideHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

// WithAttrs returns a new handler with the given attributes. If any attribute
// has key "component" and its string value matches an override, the returned
// handler uses that override as its effective level.
func (h *LevelOverrideHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	child := &LevelOverrideHandler{
		inner:     h.inner.WithAttrs(attrs),
		base:      h.base,
		overrides: h.overrides,
		effective: h.effective,
	}
	for _, a := range attrs {
		if a.Key == "component" {
			if lvl, ok := h.overrides[ComponentName(a.Value.String())]; ok {
				child.effective = &lvl
			}
		}
	}
	return child
}

// WithGroup returns a new handler with the given group name, preserving the
// current override state.
func (h *LevelOverrideHandler) WithGroup(name string) slog.Handler {
	return &LevelOverrideHandler{
		inner:     h.inner.WithGroup(name),
		base:      h.base,
		overrides: h.overrides,
		effective: h.effective,
	}
}
