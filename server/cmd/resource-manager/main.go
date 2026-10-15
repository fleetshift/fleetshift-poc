package main

import (
	"os"

	"github.com/stolostron/ocm-resource-manager/server/internal/cli"
)

func main() {
	if err := cli.New().Execute(); err != nil {
		os.Exit(1)
	}
}
