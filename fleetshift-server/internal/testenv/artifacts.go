package testenv

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ArtifactBundle is the allow-listed evidence root for environment runs.
// It only writes evidence files under Root; callers choose that path.
type ArtifactBundle struct {
	mu   sync.Mutex
	Root string
}

// newArtifactBundle returns a bundle that writes under root.
func newArtifactBundle(root string) *ArtifactBundle {
	return &ArtifactBundle{Root: root}
}

// recordEvent appends a named JSONL event to environment-events.jsonl.
// A nil receiver is a no-op.
func (b *ArtifactBundle) recordEvent(name string, fields map[string]any) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ev := map[string]any{
		"event": name,
		"at":    time.Now().UTC().Format(time.RFC3339Nano),
	}
	maps.Copy(ev, fields)
	path := filepath.Join(b.Root, "environment-events.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	_ = enc.Encode(ev)
}

// RecordTestResult writes first-attempt test status into the bundle.
func (b *ArtifactBundle) RecordTestResult(name string, passed bool, err error, duration time.Duration) {
	if b == nil {
		return
	}
	fields := map[string]any{
		"test":     name,
		"passed":   passed,
		"duration": duration.String(),
		"attempt":  1,
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	b.recordEvent("test_result", fields)
}

// writeSummary writes summary.json for the current environment status.
// A nil receiver is a no-op.
func (b *ArtifactBundle) writeSummary(env *Env, status string, closeErr error) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	summary := map[string]any{
		"status":       status,
		"profile":      "",
		"capabilities": []string{},
		"timings": map[string]any{
			"uptime": "",
		},
		"first_attempt": true,
		"retries":       0,
	}
	if env != nil {
		summary["profile"] = env.Profile
		summary["capabilities"] = env.Capabilities
		summary["timings"] = map[string]any{
			"uptime": time.Since(env.startedAt).String(),
		}
		if env.Endpoints.GRPC.Dial != "" {
			summary["endpoints"] = map[string]string{
				"grpc": env.Endpoints.GRPC.Dial,
				"http": env.Endpoints.HTTP.Dial,
			}
		}
	}
	if closeErr != nil {
		summary["cleanup_error"] = closeErr.Error()
		summary["leak_or_cleanup"] = "error"
	} else if status == "closed" || status == "started" {
		summary["leak_or_cleanup"] = "clean"
	}
	raw, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(b.Root, "summary.json"), raw, 0o644)
}
