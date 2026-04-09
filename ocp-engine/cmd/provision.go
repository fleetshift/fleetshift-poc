package cmd

import (
	"fmt"
	"os"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/credentials"
	"github.com/ocp-engine/internal/installer"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/phase"
	"github.com/ocp-engine/internal/prereq"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var provisionCmd = &cobra.Command{
	Use:   "provision",
	Short: "Provision a new OpenShift cluster",
	Long:  "Executes the complete cluster provisioning workflow through all phases (extract, install-config, manifests, ignition, cluster)",
	RunE:  runProvision,
}

var (
	provisionConfigPath string
	provisionWorkDir    string
)

func init() {
	provisionCmd.Flags().StringVar(&provisionConfigPath, "config", "", "Path to cluster configuration file (required)")
	provisionCmd.Flags().StringVar(&provisionWorkDir, "work-dir", "", "Path to work directory (required)")
	provisionCmd.MarkFlagRequired("config")
	provisionCmd.MarkFlagRequired("work-dir")
	rootCmd.AddCommand(provisionCmd)
}

func runProvision(cmd *cobra.Command, args []string) error {
	if err := prereq.Validate(); err != nil {
		return output.WriteError(os.Stdout,"prereq_error", err, false)
	}

	cfg, err := config.LoadConfig(provisionConfigPath)
	if err != nil {
		return output.WriteError(os.Stdout,"config_error", err, false)
	}

	wd, err := workdir.Init(provisionWorkDir)
	if err != nil {
		return output.WriteError(os.Stdout,"workdir_error", err, false)
	}

	if err := wd.Lock(); err != nil {
		return output.WriteError(os.Stdout,"already_running", err, false)
	}
	defer wd.Unlock()

	if err := wd.CopyClusterConfig(provisionConfigPath); err != nil {
		return output.WriteError(os.Stdout,"workdir_error", err, false)
	}

	awsEnv, err := credentials.ResolveFromConfig(&cfg.Platform.AWS.Credentials)
	if err != nil {
		return output.WriteError(os.Stdout,"config_error", fmt.Errorf("failed to resolve AWS credentials: %w", err), false)
	}

	releaseImage := cfg.ReleaseImage
	if releaseImage == "" {
		releaseImage = "quay.io/openshift-release-dev/ocp-release:4.20.18-multi"
	}

	inst := &installer.Installer{
		WorkDir:        wd.Path,
		InstallerPath:  wd.InstallerPath(),
		ReleaseImage:   releaseImage,
		PullSecretFile: cfg.PullSecretFile,
		AWSEnv:         awsEnv,
	}

	logPath := wd.LogPath()
	phases := phase.AllPhases()

	phaseFns := map[string]func() error{
		"extract": func() error {
			return inst.Extract(logPath)
		},
		"install-config": func() error {
			installConfigData, err := config.GenerateInstallConfig(cfg)
			if err != nil {
				return fmt.Errorf("generate install-config: %w", err)
			}
			return os.WriteFile(wd.InstallConfigPath(), installConfigData, 0600)
		},
		"manifests": func() error {
			return inst.CreateManifests(logPath)
		},
		"ignition": func() error {
			return inst.CreateIgnitionConfigs(logPath)
		},
		"cluster": func() error {
			return inst.CreateCluster(logPath)
		},
	}

	for _, p := range phases {
		if wd.IsPhaseComplete(p.Name) {
			continue
		}
		if err := phase.RunPhase(p, phaseFns[p.Name], os.Stdout); err != nil {
			output.WriteErrorResult(os.Stdout, output.ErrorResult{
				Category:        "phase_error",
				Phase:           p.Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				HasMetadata:     wd.HasMetadata(),
				RequiresDestroy: p.RequiresDestroyOnFailure,
			})
			return err
		}
		wd.MarkPhaseComplete(p.Name)
	}

	infraID, _ := wd.InfraID()
	output.WriteProvisionResult(os.Stdout, output.ProvisionResult{
		Status:  "succeeded",
		InfraID: infraID,
	})

	return nil
}
