package gcphcp

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestReadCACert_RespectsContextDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		<-release
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = readCACert(ctx, "https://"+listener.Addr().String())
	if err == nil {
		t.Fatal("expected context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("readCACert took too long to fail: %v", elapsed)
	}

	select {
	case <-accepted:
	default:
		t.Fatal("expected listener to accept a connection")
	}
}
