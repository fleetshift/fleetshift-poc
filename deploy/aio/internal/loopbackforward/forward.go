// Package loopbackforward is a TCP proxy: each accepted connection is dialed
// to dest so DNS names are looked up per connection.
package loopbackforward

import (
	"context"
	"fmt"
	"io"
	"net"
)

// dial is net.Dial; tests replace it to observe per-connection lookups.
var dial = net.Dial

// ListenAndServe accepts TCP connections on listen and proxies each to dest.
// dest is resolved on every connection so DNS names (e.g. a container alias)
// pick up address changes. It returns when ctx is cancelled or listen fails.
func ListenAndServe(ctx context.Context, listen, dest string) error {
	if listen == "" {
		return fmt.Errorf("listen address is empty")
	}
	if dest == "" {
		return fmt.Errorf("destination is empty")
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	return Serve(ctx, ln, dest)
}

// Serve proxies connections accepted on ln to dest until ctx is cancelled
// or Accept fails.
func Serve(ctx context.Context, ln net.Listener, dest string) error {
	if dest == "" {
		_ = ln.Close()
		return fmt.Errorf("destination is empty")
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go proxyConn(c, dest)
	}
}

// proxyConn dials dest and copies bytes until either side closes.
func proxyConn(client net.Conn, dest string) {
	defer client.Close()
	backend, err := dial("tcp", dest)
	if err != nil {
		return
	}
	defer backend.Close()
	go func() {
		_, _ = io.Copy(backend, client)
		closeWrite(backend)
	}()
	_, _ = io.Copy(client, backend)
	closeWrite(client)
}

// closeWrite shuts down the write half of a TCP conn, or closes c otherwise.
func closeWrite(c net.Conn) {
	if t, ok := c.(*net.TCPConn); ok {
		_ = t.CloseWrite()
		return
	}
	_ = c.Close()
}
