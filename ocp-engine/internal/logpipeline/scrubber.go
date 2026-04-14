package logpipeline

import "regexp"

var scrubPatterns = []struct {
	re          *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`AKIA[A-Z0-9]{16}`), "[REDACTED_AWS_KEY]"},
	{regexp.MustCompile(`(?i)(secret_?access_?key|secretkey)\s*[=:]\s*"?[A-Za-z0-9/+=]{20,}"?`), "${1}=[REDACTED]"},
	{regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9\-._~+/]+=*`), "${1}[REDACTED]"},
	{regexp.MustCompile(`(?i)(password\s*[=:]\s*)\S+`), "${1}[REDACTED]"},
	{regexp.MustCompile(`\{"auths":\{[^}]*\}[^}]*\}`), `{"auths":"[REDACTED]"}`},
}

// Scrub redacts known secret patterns from a log line.
func Scrub(line string) string {
	for _, p := range scrubPatterns {
		line = p.re.ReplaceAllString(line, p.replacement)
	}
	return line
}
