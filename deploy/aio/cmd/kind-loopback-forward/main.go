// Command kind-loopback-forward is a TCP proxy installed on kind control-plane
// nodes so 127.0.0.1:<port> reaches KIND_NODE_ROUTE_BACKEND (AIO Dex).
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/loopbackforward"
)

func main() {
	listen := flag.String("listen", "", "local host:port to accept (127.0.0.1:5556)")
	to := flag.String("to", "", "backend host:port (fleetshift:5556)")
	flag.Parse()
	if *listen == "" || *to == "" {
		log.Fatal("-listen and -to are required")
	}
	if err := loopbackforward.ListenAndServe(context.Background(), *listen, *to); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
