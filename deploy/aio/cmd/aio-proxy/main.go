// Command aio-proxy terminates the sandbox gateway certificate and
// reverse-proxies peer Dex and FleetShift on one origin.
package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioproxy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "aio-proxy: %v\n", err)
		os.Exit(1)
	}
}

// run loads AIO endpoint and PKI paths, constructs the TLS edge, and serves
// until SIGINT or SIGTERM.
func run() error {
	endpoints := aioinit.FixedEndpoints
	pki := aioinit.DefaultSandboxPKIPaths()
	dexURL, err := url.Parse("http://" + endpoints.DexListen)
	if err != nil {
		return fmt.Errorf("dex upstream: %w", err)
	}
	appURL, err := url.Parse("http://" + endpoints.HTTPListen)
	if err != nil {
		return fmt.Errorf("app upstream: %w", err)
	}
	proxy, err := aioproxy.New(aioproxy.Config{
		ListenAddr:   endpoints.GatewayListen,
		CertFile:     pki.LeafCert,
		KeyFile:      pki.LeafKey,
		PublicOrigin: endpoints.PublicOrigin,
		DexURL:       dexURL,
		AppURL:       appURL,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return proxy.ListenAndServe(ctx)
}
