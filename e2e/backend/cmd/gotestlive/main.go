// Command gotestlive renders go test -json as live testdox-style progress.
// It reads events from stdin and writes human-readable output to stdout.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bitfield/gotestdox"
)

const (
	iconRun   = "⏳"
	iconPass  = "✅"
	iconFail  = "❌"
	iconSkip  = "➖"
	iconSuite = "❯"
	maxLog    = 100 // live t.Log line cap in runes
)

// testEvent is the subset of go test -json we render live.
type testEvent struct {
	Action  string  `json:"Action"`
	Package string  `json:"Package"`
	Test    string  `json:"Test"`
	Output  string  `json:"Output"`
	Elapsed float64 `json:"Elapsed"`
}

var tLogPrefix = regexp.MustCompile(`^\s+\S+\.go:\d+: (.*)$`)

// main reads go test -json from stdin and writes live output to stdout.
func main() {
	if err := render(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "gotestlive: %v\n", err)
		os.Exit(1)
	}
}

// render reads go test -json lines from in and writes live testdox-style
// output to out. Empty lines are dropped; other non-JSON lines pass through.
func render(in io.Reader, out io.Writer) error {
	w := bufio.NewWriter(out)
	defer w.Flush()
	r := &renderer{wroteHeader: map[string]bool{}}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			if err := writeLine(w, line); err != nil {
				return err
			}
			continue
		}
		if err := r.writeEvent(w, ev); err != nil {
			return err
		}
	}
	return sc.Err()
}

// renderer holds live-render state for one go test -json stream.
type renderer struct {
	wroteHeader map[string]bool
}

// writeEvent writes live output for one go test JSON event.
func (r *renderer) writeEvent(w *bufio.Writer, ev testEvent) error {
	if ev.Test == "" {
		if ev.Action == "output" {
			msg := strings.TrimRight(ev.Output, "\n")
			if msg == "" {
				return nil
			}
			return writeLine(w, msg)
		}
		return nil
	}
	if omitRootEvent(ev, r.wroteHeader[ev.Test]) {
		return nil
	}
	if err := r.writeHeader(w, ev.Test); err != nil {
		return err
	}
	indent := ""
	if _, ok := parentName(ev.Test); ok {
		indent = "  "
	}
	name := displayName(ev.Test)
	switch ev.Action {
	case "run":
		return writeLine(w, indent+iconRun+" "+name)
	case "output":
		if isFraming(ev.Output) {
			return nil
		}
		msg := logMessage(ev.Output)
		if msg == "" {
			return nil
		}
		return writeLine(w, indent+"  "+clip(msg, maxLog))
	case "pass", "fail", "skip":
		return writeResult(w, indent, ev.Action, name, ev.Elapsed)
	}
	return nil
}

// writeHeader writes a parent-test header on the first subtest event.
func (r *renderer) writeHeader(w *bufio.Writer, test string) error {
	parent, ok := parentName(test)
	if !ok || r.wroteHeader[parent] {
		return nil
	}
	if len(r.wroteHeader) > 0 {
		if err := writeLine(w, ""); err != nil {
			return err
		}
	}
	r.wroteHeader[parent] = true
	return writeLine(w, iconSuite+" "+gotestdox.Prettify(parent))
}

// displayName is the testdox sentence for a test, without repeating the parent
// title already printed as the suite header.
func displayName(test string) string {
	full := gotestdox.Prettify(test)
	parent, ok := parentName(test)
	if !ok {
		return full
	}
	prefix := gotestdox.Prettify(parent) + " "
	if len(full) > len(prefix) && strings.EqualFold(full[:len(prefix)], prefix) {
		return capitalize(full[len(prefix):])
	}
	leaf := test[strings.LastIndex(test, "/")+1:]
	return capitalize(strings.ReplaceAll(leaf, "_", " "))
}

// capitalize returns s with its first letter uppercased. Empty s is unchanged.
func capitalize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToUpper(r)) + s[n:]
}

// omitRootEvent reports whether a root-test event should be hidden. Run and
// output are always omitted; pass and skip are omitted when a suite header was
// already written (the root has subtests). Fail is never omitted.
func omitRootEvent(ev testEvent, wroteHeader bool) bool {
	if _, ok := parentName(ev.Test); ok {
		return false
	}
	switch ev.Action {
	case "fail":
		return false
	case "run", "output":
		return true
	case "pass", "skip":
		return wroteHeader
	}
	return false
}

// parentName returns the parent of a slash-separated subtest name.
func parentName(test string) (string, bool) {
	i := strings.LastIndex(test, "/")
	if i <= 0 {
		return "", false
	}
	return test[:i], true
}

func writeResult(w *bufio.Writer, indent, action, name string, elapsed float64) error {
	icon := iconPass
	switch action {
	case "fail":
		icon = iconFail
	case "skip":
		icon = iconSkip
	}
	return writeLine(w, fmt.Sprintf("%s%s %s (%.2fs)", indent, icon, name, elapsed))
}

// writeLine writes s and a newline, then flushes.
func writeLine(w *bufio.Writer, s string) error {
	if _, err := fmt.Fprintln(w, s); err != nil {
		return err
	}
	return w.Flush()
}

// isFraming reports whether s is a go test === / --- status framing line.
func isFraming(s string) bool {
	t := strings.TrimSpace(s)
	for _, p := range []string{"=== RUN", "=== PAUSE", "=== CONT", "=== NAME", "--- PASS", "--- FAIL", "--- SKIP"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// logMessage strips the file:line prefix from a t.Log line.
func logMessage(s string) string {
	s = strings.TrimRight(s, "\n")
	if m := tLogPrefix.FindStringSubmatch(s); len(m) == 2 {
		return m[1]
	}
	return strings.TrimSpace(s)
}

// clip truncates s to at most n runes, appending an ellipsis when truncated.
func clip(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n]) + "…"
}
