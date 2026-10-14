package aioinit

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// EnsureHostsAlias appends `ip hostname # marker` to path when that exact
// mapping is missing. It returns if the exact line already exists or hostname
// already maps to ip. A different IP for hostname is an error. path must
// already exist; the file is opened for append and is never replaced.
func EnsureHostsAlias(path, ip, hostname, marker string) error {
	if ip == "" || hostname == "" || marker == "" {
		return fmt.Errorf("hosts alias: ip, hostname, and marker are required")
	}
	wantLine := ip + " " + hostname + " # " + marker

	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read hosts file: %w", err)
	}
	content := string(raw)
	sc := bufio.NewScanner(strings.NewReader(content))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == wantLine {
			return nil
		}
		mappedIP, hosts := parseHostsLine(line)
		if mappedIP == "" {
			continue
		}
		for _, h := range hosts {
			if h != hostname {
				continue
			}
			if mappedIP != ip {
				return fmt.Errorf("conflicting hosts mapping for %s: %s", hostname, line)
			}
			return nil
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan hosts file: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("open hosts file: %w", err)
	}
	prefix := ""
	if len(content) > 0 && !strings.HasSuffix(content, "\n") {
		prefix = "\n"
	}
	if _, err := f.WriteString(prefix + wantLine + "\n"); err != nil {
		_ = f.Close()
		return fmt.Errorf("append hosts alias: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close hosts file: %w", err)
	}
	return nil
}

// parseHostsLine returns the IP and hostnames from a hosts(5) line.
func parseHostsLine(line string) (ip string, hostnames []string) {
	if line == "" || strings.HasPrefix(line, "#") {
		return "", nil
	}
	if i := strings.Index(line, "#"); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", nil
	}
	return fields[0], fields[1:]
}
