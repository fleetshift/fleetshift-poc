package logpipeline

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ocp-engine/internal/output"
)

func TestPipeline_StreamsAndParses(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, ".openshift_install.log")
	os.WriteFile(logFile, []byte(""), 0644)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	p := NewPipeline(logFile, &stdout, &stderr, 1)
	p.Start()

	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("level=info msg=\"Creating VPC\"\n")
	f.WriteString("level=info msg=\"It is now safe to remove the bootstrap resources\"\n")
	f.Close()

	time.Sleep(100 * time.Millisecond)
	p.Stop()

	stderrOut := stderr.String()
	if !strings.Contains(stderrOut, "Creating VPC") {
		t.Errorf("stderr missing 'Creating VPC': %s", stderrOut)
	}

	stdoutOut := stdout.String()
	if !strings.Contains(stdoutOut, "bootstrap_complete") {
		t.Errorf("stdout missing bootstrap_complete event: %s", stdoutOut)
	}

	for _, line := range strings.Split(strings.TrimSpace(stdoutOut), "\n") {
		if line == "" {
			continue
		}
		var event output.MilestoneEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Errorf("invalid JSON in stdout: %s", line)
		}
	}

	if !p.BootstrapComplete() {
		t.Error("BootstrapComplete() = false after bootstrap log line")
	}
}
