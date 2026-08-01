package testenv

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// AssertAllowListedArtifacts checks the evidence bundle written under
// env.Artifacts.Root: summary.json, environment-events.jsonl, and that
// private runtime paths stay outside the upload root.
func AssertAllowListedArtifacts(t *testing.T, env *Env) {
	t.Helper()
	if env == nil || env.Artifacts == nil {
		t.Fatal("expected artifact bundle")
	}
	root := env.Artifacts.Root
	if filepath.Base(root) != "artifacts" {
		t.Fatalf("artifact root base = %q, want artifacts", filepath.Base(root))
	}

	raw, err := os.ReadFile(filepath.Join(root, "summary.json"))
	if err != nil {
		t.Fatalf("read summary.json: %v", err)
	}
	var summary map[string]any
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatalf("summary.json: %v", err)
	}
	if summary["profile"] != ProfileHermeticAPI {
		t.Fatalf("summary.profile = %v, want %q", summary["profile"], ProfileHermeticAPI)
	}
	caps, ok := summary["capabilities"].([]any)
	if !ok || len(caps) != len(HermeticCapabilities) {
		t.Fatalf("summary.capabilities = %v, want %v", summary["capabilities"], HermeticCapabilities)
	}
	for i, want := range HermeticCapabilities {
		if caps[i] != want {
			t.Fatalf("summary.capabilities[%d] = %v, want %q", i, caps[i], want)
		}
	}
	if summary["first_attempt"] != true {
		t.Fatalf("summary.first_attempt = %v, want true", summary["first_attempt"])
	}
	if summary["retries"] != float64(0) {
		t.Fatalf("summary.retries = %v, want 0", summary["retries"])
	}

	events := readArtifactJSONL(t, filepath.Join(root, "environment-events.jsonl"))
	if !artifactJSONLHasEvent(events, "environment_ready") {
		t.Fatalf("environment-events.jsonl missing environment_ready; events=%v", artifactEventNames(events))
	}

	workDir := filepath.Dir(root)
	for _, private := range []string{
		filepath.Join(workDir, DBFile),
		filepath.Join(workDir, ServerLogFile),
	} {
		rel, err := filepath.Rel(root, private)
		if err != nil {
			t.Fatalf("rel %s: %v", private, err)
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatalf("private path %q is inside artifact root %q (rel=%q)", private, root, rel)
		}
	}
}

// AssertArtifactTestResult checks that a first-attempt test_result event was
// recorded for testName in the allow-listed event journal.
func AssertArtifactTestResult(t *testing.T, env *Env, testName string) {
	t.Helper()
	if env == nil || env.Artifacts == nil {
		t.Fatal("expected artifact bundle")
	}
	events := readArtifactJSONL(t, filepath.Join(env.Artifacts.Root, "environment-events.jsonl"))
	for _, ev := range events {
		if ev["event"] != "test_result" {
			continue
		}
		if ev["test"] != testName {
			continue
		}
		if ev["attempt"] != float64(1) {
			t.Fatalf("test_result.attempt = %v, want 1", ev["attempt"])
		}
		if _, ok := ev["passed"].(bool); !ok {
			t.Fatalf("test_result.passed missing or not bool: %v", ev["passed"])
		}
		if _, ok := ev["duration"].(string); !ok || ev["duration"] == "" {
			t.Fatalf("test_result.duration missing: %v", ev["duration"])
		}
		return
	}
	t.Fatalf("environment-events.jsonl missing test_result for %q; events=%v", testName, artifactEventNames(events))
}

// readArtifactJSONL parses a JSONL artifact file into event maps.
func readArtifactJSONL(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("jsonl %s: %v", path, err)
		}
		out = append(out, ev)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}

// artifactJSONLHasEvent reports whether any event has the given name.
func artifactJSONLHasEvent(events []map[string]any, name string) bool {
	for _, ev := range events {
		if ev["event"] == name {
			return true
		}
	}
	return false
}

// artifactEventNames returns event name strings for assertion failure output.
func artifactEventNames(events []map[string]any) []string {
	names := make([]string, 0, len(events))
	for _, ev := range events {
		if s, ok := ev["event"].(string); ok {
			names = append(names, s)
		}
	}
	return names
}
