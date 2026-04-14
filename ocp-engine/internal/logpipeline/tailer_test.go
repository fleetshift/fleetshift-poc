package logpipeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTailer_ReadsExistingLines(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	os.WriteFile(logFile, []byte("line1\nline2\nline3\n"), 0644)

	done := make(chan struct{})
	lines := make(chan string, 10)

	go Tail(logFile, lines, done)

	time.Sleep(50 * time.Millisecond)
	close(done)

	var got []string
	for l := range lines {
		got = append(got, l)
	}

	if len(got) != 3 {
		t.Fatalf("got %d lines, want 3: %v", len(got), got)
	}
	if got[0] != "line1" || got[1] != "line2" || got[2] != "line3" {
		t.Errorf("lines = %v", got)
	}
}

func TestTailer_FollowsNewLines(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	os.WriteFile(logFile, []byte(""), 0644)

	done := make(chan struct{})
	lines := make(chan string, 10)

	go Tail(logFile, lines, done)

	time.Sleep(20 * time.Millisecond)
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("appended line\n")
	f.Close()

	select {
	case line := <-lines:
		if line != "appended line" {
			t.Errorf("got %q, want %q", line, "appended line")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("tailer didn't pick up appended line within 500ms")
	}

	close(done)
}

func TestTailer_StopsOnDone(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	os.WriteFile(logFile, []byte(""), 0644)

	done := make(chan struct{})
	lines := make(chan string, 10)

	go Tail(logFile, lines, done)
	time.Sleep(20 * time.Millisecond)
	close(done)

	select {
	case _, ok := <-lines:
		if ok {
			// got a line, drain
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("lines channel not closed within 500ms after done")
	}
}
