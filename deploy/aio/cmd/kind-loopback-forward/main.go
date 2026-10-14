// Command kind-loopback-forward is a TCP proxy installed on kind control-plane
// nodes so 127.0.0.1:<port> reaches KIND_LOOPBACK_FORWARD_TO (AIO TLS edge).
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/loopbackforward"
)

func main() {
	listen := flag.String("listen", "", "local host:port to accept (127.0.0.1:8085)")
	to := flag.String("to", "", "backend host:port (fleetshift:8085)")
	flag.Parse()
	if *listen == "" || *to == "" {
		log.Fatal("-listen and -to are required")
	}
	if err := loopbackforward.ListenAndServe(context.Background(), *listen, *to); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
