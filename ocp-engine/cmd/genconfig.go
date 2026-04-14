package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var genConfigCmd = &cobra.Command{
	Use:   "gen-config",
	Short: "Generate install-config.yaml without provisioning",
	Long:  "Dry-run mode that generates and writes the install-config.yaml to the cluster directory without executing any installation phases",
	RunE:  runGenConfig,
}

var genConfigConfigPath string

func init() {
	genConfigCmd.Flags().StringVar(&genConfigConfigPath, "config", "", "Path to cluster.yaml (required). Parent directory is used as work directory.")
	genConfigCmd.MarkFlagRequired("config")
	rootCmd.AddCommand(genConfigCmd)
}

func runGenConfig(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig(genConfigConfigPath)
	if err != nil {
		return output.WriteError(os.Stdout, "config_error", err, false)
	}

	wd, err := workdir.Open(filepath.Dir(genConfigConfigPath))
	if err != nil {
		return output.WriteError(os.Stdout, "workdir_error", err, false)
	}

	installConfigData, err := config.GenerateInstallConfig(cfg)
	if err != nil {
		return output.WriteError(os.Stdout, "config_error", fmt.Errorf("failed to generate install-config: %w", err), false)
	}

	if err := os.WriteFile(wd.InstallConfigPath(), installConfigData, 0600); err != nil {
		return output.WriteError(os.Stdout, "workdir_error", fmt.Errorf("failed to write install-config.yaml: %w", err), false)
	}

	output.WritePhaseResult(os.Stdout, output.PhaseResult{
		Phase:  "gen-config",
		Status: "complete",
	})

	return nil
}
