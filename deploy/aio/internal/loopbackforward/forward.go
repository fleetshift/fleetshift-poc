// Package loopbackforward is a TCP proxy: each accepted connection is dialed
// to dest so DNS names are looked up per connection.
package loopbackforward

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"
)

const dialTimeout = 5 * time.Second

// dial is (*net.Dialer).DialContext; tests replace it to observe per-connection lookups.
var dial = new(net.Dialer).DialContext

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
		go proxyConn(ctx, c, dest)
	}
}

// proxyConn dials dest and copies bytes until either side closes.
func proxyConn(ctx context.Context, client net.Conn, dest string) {
	defer client.Close()
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	destConn, err := dial(dialCtx, "tcp", dest)
	if err != nil {
		return
	}
	defer destConn.Close()
	go func() {
		_, _ = io.Copy(destConn, client)
		closeWrite(destConn)
	}()
	_, _ = io.Copy(client, destConn)
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
