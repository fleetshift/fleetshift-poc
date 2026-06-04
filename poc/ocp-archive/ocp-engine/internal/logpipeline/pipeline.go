package logpipeline

import (
	"fmt"
	"io"
	"time"

	"github.com/ocp-engine/internal/output"
)

// Pipeline connects the tailer, scrubber, and parser into a processing pipeline.
type Pipeline struct {
	logFile string
	stdout  io.Writer // JSON milestone events
	stderr  io.Writer // scrubbed log lines
	attempt int
	parser  *Parser
	done    chan struct{}
	start   time.Time
}

// NewPipeline creates a new log pipeline.
func NewPipeline(logFile string, stdout, stderr io.Writer, attempt int) *Pipeline {
	return &Pipeline{
		logFile: logFile,
		stdout:  stdout,
		stderr:  stderr,
		attempt: attempt,
		parser:  NewParser(),
		done:    make(chan struct{}),
	}
}

// Start begins tailing the log file in a background goroutine.
func (p *Pipeline) Start() {
	p.start = time.Now()
	lines := make(chan string, 100)

	go Tail(p.logFile, lines, p.done)

	go func() {
		for line := range lines {
			scrubbed := Scrub(line)

			fmt.Fprintln(p.stderr, scrubbed)

			if event := p.parser.Parse(scrubbed); event != "" {
				elapsed := int(time.Since(p.start).Seconds())
				output.WriteMilestoneEvent(p.stdout, output.MilestoneEvent{
					Event:          event,
					ElapsedSeconds: elapsed,
					Attempt:        p.attempt,
				})
			}
		}
	}()
}

// Stop signals the pipeline to stop and waits for cleanup.
func (p *Pipeline) Stop() {
	close(p.done)
	time.Sleep(50 * time.Millisecond)
}

// BootstrapComplete returns whether the bootstrap_complete milestone was detected.
func (p *Pipeline) BootstrapComplete() bool {
	return p.parser.BootstrapComplete()
}
