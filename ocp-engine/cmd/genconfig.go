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
	Long:  "Dry-run mode that generates and writes the install-config.yaml to the work directory without executing any installation phases",
	RunE:  runGenConfig,
}

var (
	genConfigConfigPath string
	genConfigWorkDir    string
)

func init() {
	genConfigCmd.Flags().StringVar(&genConfigConfigPath, "config", "", "Path to cluster configuration file (required)")
	genConfigCmd.Flags().StringVar(&genConfigWorkDir, "work-dir", "", "Path to work directory (required)")
	genConfigCmd.MarkFlagRequired("config")
	genConfigCmd.MarkFlagRequired("work-dir")
	rootCmd.AddCommand(genConfigCmd)
}

func runGenConfig(cmd *cobra.Command, args []string) error {
	// Step 1: Load config
	cfg, err := config.LoadConfig(genConfigConfigPath)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "config_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 2: Init work directory
	wd, err := workdir.Init(genConfigWorkDir)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         err.Error(),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 3: Generate install-config
	installConfigData, err := config.GenerateInstallConfig(cfg)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "config_error",
			Message:         fmt.Sprintf("failed to generate install-config: %v", err),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 4: Write install-config.yaml to work-dir
	if err := os.WriteFile(wd.InstallConfigPath(), installConfigData, 0600); err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         fmt.Sprintf("failed to write install-config.yaml: %v", err),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 5: Copy cluster.yaml to work-dir
	clusterYAMLPath := filepath.Join(wd.Path, "cluster.yaml")
	clusterYAMLData, err := os.ReadFile(genConfigConfigPath)
	if err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "config_error",
			Message:         fmt.Sprintf("failed to read config file for copying: %v", err),
			RequiresDestroy: false,
		})
		return err
	}
	if err := os.WriteFile(clusterYAMLPath, clusterYAMLData, 0644); err != nil {
		output.WriteErrorResult(os.Stdout, output.ErrorResult{
			Category:        "workdir_error",
			Message:         fmt.Sprintf("failed to copy cluster.yaml to work-dir: %v", err),
			RequiresDestroy: false,
		})
		return err
	}

	// Step 6: Write PhaseResult
	output.WritePhaseResult(os.Stdout, output.PhaseResult{
		Phase:  "gen-config",
		Status: "complete",
	})

	return nil
}
