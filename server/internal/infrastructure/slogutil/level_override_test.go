package slogutil_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/slogutil"
)

func TestLevelOverrideHandler_BaseLevel(t *testing.T) {
	h := slogutil.NewLevelOverrideHandler(
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.LevelInfo,
		nil,
	)

	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("base=info: debug should be disabled")
	}
	if !h.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("base=info: info should be enabled")
	}
	if !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("base=info: warn should be enabled")
	}
}

func TestLevelOverrideHandler_ComponentOverrideLowersLevel(t *testing.T) {
	h := slogutil.NewLevelOverrideHandler(
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.LevelInfo,
		map[slogutil.ComponentName]slog.Level{
			"deployment": slog.LevelDebug,
		},
	)

	// Before WithAttrs, base level applies.
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("before WithAttrs: debug should be disabled at base level")
	}

	child := h.WithAttrs([]slog.Attr{slog.String("component", "deployment")})

	if !child.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("after WithAttrs(component=deployment): debug should be enabled")
	}
	if !child.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("after WithAttrs(component=deployment): info should be enabled")
	}
}

func TestLevelOverrideHandler_ComponentOverrideRaisesLevel(t *testing.T) {
	h := slogutil.NewLevelOverrideHandler(
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.LevelInfo,
		map[slogutil.ComponentName]slog.Level{
			"delivery": slog.LevelWarn,
		},
	)

	child := h.WithAttrs([]slog.Attr{slog.String("component", "delivery")})

	if child.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("delivery override=warn: info should be disabled")
	}
	if !child.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("delivery override=warn: warn should be enabled")
	}
}

func TestLevelOverrideHandler_UnknownComponentUsesBase(t *testing.T) {
	h := slogutil.NewLevelOverrideHandler(
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.LevelInfo,
		map[slogutil.ComponentName]slog.Level{
			"deployment": slog.LevelDebug,
		},
	)

	child := h.WithAttrs([]slog.Attr{slog.String("component", "unknown")})

	if child.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("unknown component should use base level; debug should be disabled")
	}
	if !child.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("unknown component should use base level; info should be enabled")
	}
}

func TestLevelOverrideHandler_RecordsFlowToInnerHandler(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := slogutil.NewLevelOverrideHandler(inner, slog.LevelInfo,
		map[slogutil.ComponentName]slog.Level{
			"deployment": slog.LevelDebug,
		},
	)

	logger := slog.New(h).With("component", "deployment")
	logger.Debug("test message", "key", "value")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log entry: %v (raw: %s)", err, buf.String())
	}
	if got := entry["msg"]; got != "test message" {
		t.Errorf("msg = %v, want %q", got, "test message")
	}
	if got := entry["component"]; got != "deployment" {
		t.Errorf("component = %v, want %q", got, "deployment")
	}
	if got := entry["key"]; got != "value" {
		t.Errorf("key = %v, want %q", got, "value")
	}
}

func TestLevelOverrideHandler_BaseBlocksRecordAtInnerLevel(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := slogutil.NewLevelOverrideHandler(inner, slog.LevelInfo, nil)

	logger := slog.New(h)
	logger.Debug("should not appear")

	if buf.Len() != 0 {
		t.Errorf("debug record should have been blocked, got: %s", buf.String())
	}
}

func TestLevelOverrideHandler_WithGroupPreservesOverride(t *testing.T) {
	h := slogutil.NewLevelOverrideHandler(
		slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.LevelInfo,
		map[slogutil.ComponentName]slog.Level{
			"deployment": slog.LevelDebug,
		},
	)

	child := h.WithAttrs([]slog.Attr{slog.String("component", "deployment")})
	grouped := child.WithGroup("sub")

	if !grouped.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("WithGroup should preserve the component override")
	}
}

func TestLevelOverrideHandler_NonComponentAttrsPassThrough(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := slogutil.NewLevelOverrideHandler(inner, slog.LevelDebug, nil)

	logger := slog.New(h).With("request_id", "abc")
	logger.Info("hello")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := entry["request_id"]; got != "abc" {
		t.Errorf("request_id = %v, want %q", got, "abc")
	}
}
