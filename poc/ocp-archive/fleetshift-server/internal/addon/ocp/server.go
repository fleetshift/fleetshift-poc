package ocp

import (
	"context"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	ocpv1 "github.com/fleetshift/fleetshift-poc/gen/ocp/v1"
)

const defaultCallbackAddr = ":50052"

// Start launches the addon's internal callback gRPC server. The
// listen address is read from OCP_CALLBACK_ADDR env var, falling
// back to ":50052". Pass a non-empty addr to override (useful for
// tests with ":0" for a random port).
//
// After Start returns, CallbackAddr() returns the resolved address.
func (a *Agent) Start(addr ...string) error {
	callbackAddr := os.Getenv("OCP_CALLBACK_ADDR")
	if callbackAddr == "" {
		callbackAddr = defaultCallbackAddr
	}
	if len(addr) > 0 && addr[0] != "" {
		callbackAddr = addr[0]
	}

	lis, err := net.Listen("tcp", callbackAddr)
	if err != nil {
		return fmt.Errorf("ocp callback: listen %s: %w", callbackAddr, err)
	}

	a.callbackAddr = lis.Addr().String()

	srv := grpc.NewServer()
	ocpv1.RegisterCallbackServiceServer(srv, &callbackServer{
		provisions:    &a.provisions,
		tokenVerifier: a.tokenSigner,
	})

	a.grpcServer = srv

	go func() {
		_ = srv.Serve(lis)
	}()

	return nil
}

// Shutdown gracefully stops the callback gRPC server.
func (a *Agent) Shutdown(ctx context.Context) {
	if a.grpcServer != nil {
		a.grpcServer.GracefulStop()
	}
}

// CallbackAddr returns the resolved address the callback server is
// listening on. Only valid after Start() returns.
func (a *Agent) CallbackAddr() string {
	return a.callbackAddr
}
