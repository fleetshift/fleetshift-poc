package loopbackforward

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestListenAndServe_EmptyListen(t *testing.T) {
	t.Parallel()
	err := ListenAndServe(context.Background(), "", "127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListenAndServe_EmptyDest(t *testing.T) {
	t.Parallel()
	err := ListenAndServe(context.Background(), "127.0.0.1:0", "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestListenAndServe_ForwardsBytes(t *testing.T) {
	t.Parallel()
	backend := echoListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- Serve(ctx, ln, backend) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	want := []byte("hello-dex")
	if _, err := c.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestServe_DialsDestinationPerConnection(t *testing.T) {
	backend := echoListener(t)
	orig := dial
	t.Cleanup(func() { dial = orig })
	var mu sync.Mutex
	var addrs []string
	dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		mu.Lock()
		addrs = append(addrs, network+" "+address)
		mu.Unlock()
		return orig(ctx, network, address)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() { errc <- Serve(ctx, ln, backend) }()

	for range 2 {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial proxy: %v", err)
		}
		if _, err := c.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 1)
		if _, err := io.ReadFull(c, got); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(addrs) != 2 {
		t.Fatalf("dials = %v, want 2", addrs)
	}
	want := "tcp " + backend
	for _, a := range addrs {
		if a != want {
			t.Fatalf("dial %q, want %q", a, want)
		}
	}
}

func TestServe_DialHonorsTimeout(t *testing.T) {
	orig := dial
	t.Cleanup(func() { dial = orig })
	dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errc := make(chan error, 1)
	go func() { errc <- Serve(ctx, ln, "127.0.0.1:1") }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(dialTimeout + time.Second)
	if err := c.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	n, err := c.Read(buf)
	if n != 0 {
		t.Fatalf("unexpected data: %q", buf[:n])
	}
	if err == nil {
		t.Fatal("expected client close after dial timeout")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("read waited until deadline; dial timeout did not close client")
	}

	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func echoListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	return ln.Addr().String()
}
