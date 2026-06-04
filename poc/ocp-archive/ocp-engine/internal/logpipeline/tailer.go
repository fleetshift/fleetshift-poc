package logpipeline

import (
	"bufio"
	"os"
	"time"
)

// Tail reads lines from a file continuously (like tail -f).
// New lines are sent to the lines channel. Stops when done is closed.
// Closes the lines channel on exit.
func Tail(filePath string, lines chan<- string, done <-chan struct{}) {
	defer close(lines)

	f, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for {
		for scanner.Scan() {
			select {
			case <-done:
				return
			case lines <- scanner.Text():
			}
		}
		// EOF — wait and retry
		select {
		case <-done:
			return
		case <-time.After(5 * time.Millisecond):
		}
		scanner = bufio.NewScanner(f)
	}
}
