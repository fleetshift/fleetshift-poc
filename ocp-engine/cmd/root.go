package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "ocp-engine",
	Short: "Standalone OCP cluster provisioning CLI",
	Long:  "A stateless CLI tool that wraps openshift-install for provisioning and deprovisioning OpenShift 4.20 clusters on AWS.",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
