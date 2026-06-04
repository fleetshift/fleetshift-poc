package logpipeline

import (
	"regexp"
	"sync"
)

type milestone struct {
	re    *regexp.Regexp
	event string
}

var milestones = []milestone{
	{regexp.MustCompile(`(?i)bootstrap.*complete|safe to remove the bootstrap`), "bootstrap_complete"},
	{regexp.MustCompile(`(?i)install complete`), "install_complete"},
	{regexp.MustCompile(`(?i)cluster operators.*available|all cluster operators.*available`), "cluster_operators_ready"},
}

// Parser detects milestone events from log lines and tracks bootstrap state.
type Parser struct {
	mu                sync.Mutex
	bootstrapComplete bool
	seen              map[string]bool
}

func NewParser() *Parser {
	return &Parser{seen: make(map[string]bool)}
}

// Parse checks a log line against milestone patterns.
// Returns the event name if a new milestone is detected, empty string otherwise.
// Each milestone fires only once.
func (p *Parser) Parse(line string) string {
	for _, m := range milestones {
		if m.re.MatchString(line) {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.seen[m.event] {
				return ""
			}
			p.seen[m.event] = true
			if m.event == "bootstrap_complete" {
				p.bootstrapComplete = true
			}
			return m.event
		}
	}
	return ""
}

// BootstrapComplete returns whether the bootstrap_complete milestone has been seen.
func (p *Parser) BootstrapComplete() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bootstrapComplete
}
