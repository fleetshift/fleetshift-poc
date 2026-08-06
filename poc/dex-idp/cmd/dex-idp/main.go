package main

import (
	"os"

	dexidp "github.com/fleetshift/fleetshift-poc/poc/dex-idp"
)

func main() {
	if err := dexidp.New().Execute(); err != nil {
		os.Exit(1)
	}
}
