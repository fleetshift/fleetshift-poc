package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ocp-engine/internal/config"
	"github.com/ocp-engine/internal/installer"
	"github.com/ocp-engine/internal/logpipeline"
	"github.com/ocp-engine/internal/output"
	"github.com/ocp-engine/internal/phase"
	"github.com/ocp-engine/internal/preflight"
	"github.com/ocp-engine/internal/workdir"
	"github.com/spf13/cobra"
)

var provisionCmd = &cobra.Command{
	Use:   "provision",
	Short: "Provision a new OpenShift cluster",
	Long:  "Executes the complete cluster provisioning workflow through all phases (extract, install-config, manifests, ignition, cluster). The directory containing the config file is used as the work directory.",
	RunE:  runProvision,
}

var provisionConfigPath string
var provisionAttempt int

func init() {
	provisionCmd.Flags().StringVar(&provisionConfigPath, "config", "", "Path to cluster.yaml (required). Parent directory is used as work directory.")
	provisionCmd.MarkFlagRequired("config")
	provisionCmd.Flags().IntVar(&provisionAttempt, "attempt", 1, "Attempt number for retry tracking (metadata only, no engine behavior change)")
	rootCmd.AddCommand(provisionCmd)
}

func runProvision(cmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfig(provisionConfigPath)
	if err != nil {
		return output.WriteError(os.Stdout, "config_error", err, false)
	}

	wd, err := workdir.Open(filepath.Dir(provisionConfigPath))
	if err != nil {
		return output.WriteError(os.Stdout, "workdir_error", err, false)
	}

	if err := wd.Lock(); err != nil {
		return output.WriteError(os.Stdout, "already_running", err, false)
	}
	defer wd.Unlock()

	// Copy cluster.yaml to work-dir so destroy can find it later
	if err := wd.CopyConfig(provisionConfigPath); err != nil {
		return output.WriteError(os.Stdout, "workdir_error", err, false)
	}

	// Run preflight checks (validates config, files, credentials, DNS)
	awsEnv, err := preflight.RunPreflight(cfg, os.Stdout, provisionAttempt)
	if err != nil {
		return output.WriteError(os.Stdout, "prereq_error", err, false)
	}
	wd.MarkPhaseComplete("preflight")

	releaseImage := cfg.Engine.ReleaseImage
	if releaseImage == "" {
		releaseImage = "quay.io/openshift-release-dev/ocp-release:4.20.18-multi"
	}

	inst := &installer.Installer{
		WorkDir:        wd.Path,
		InstallerPath:  wd.InstallerPath(),
		ReleaseImage:   releaseImage,
		PullSecretFile: cfg.Engine.PullSecretFile,
		AWSEnv:         awsEnv,
	}

	logPath := wd.LogPath()

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
			if err := wd.BackupInstallConfig(); err != nil {
				return fmt.Errorf("backup install-config: %w", err)
			}
			return inst.CreateManifests(logPath)
		},
		"ignition": func() error {
			return inst.CreateIgnitionConfigs(logPath)
		},
		"cluster": func() error {
			pipeline := logpipeline.NewPipeline(logPath, os.Stdout, os.Stderr, provisionAttempt)
			pipeline.Start()
			defer pipeline.Stop()
			return inst.CreateClusterQuiet(logPath)
		},
	}

	for _, p := range phase.AllPhases() {
		if p.Name == "preflight" {
			continue // already ran above
		}
		if wd.IsPhaseComplete(p.Name) {
			continue
		}
		fn, ok := phaseFns[p.Name]
		if !ok {
			continue
		}
		if err := phase.RunPhase(p, fn, os.Stdout, provisionAttempt); err != nil {
			errResult := output.ErrorResult{
				Category:        "phase_error",
				Phase:           p.Name,
				Message:         err.Error(),
				LogTail:         wd.LogTail(50),
				HasMetadata:     wd.HasMetadata(),
				RequiresDestroy: p.RequiresDestroyOnFailure,
				Attempt:         provisionAttempt,
			}
			// For cluster phase, parse failure reason from logs
			if p.Name == "cluster" {
				fullLog := readFullLog(logPath)
				errResult.FailureReason, errResult.FailureMessage = logpipeline.ParseFailureReason(fullLog)
			}
			output.WriteErrorResult(os.Stdout, errResult)
			return err
		}
		wd.MarkPhaseComplete(p.Name)
	}

	infraID, _ := wd.InfraID()
	output.WriteProvisionResult(os.Stdout, output.ProvisionResult{
		Status:  "succeeded",
		InfraID: infraID,
		Attempt: provisionAttempt,
	})

	return nil
}

func readFullLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}
