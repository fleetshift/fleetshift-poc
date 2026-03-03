package main

import (
	"os"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/cli"
)

func main() {
	if err := cli.New().Execute(); err != nil {
		os.Exit(1)
	}
}
