package cli

import (
	"fmt"
	"path/filepath"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

// annotationSkipServer marks commands that do not dial the FleetShift gRPC
// server (for example local auth configuration).
const annotationSkipServer = "fleetctl.io/skip-server"

// globalFlags is the fleetctl persistent flag set.
type globalFlags struct {
	server          string
	outputFormat    string
	serverTLS       bool
	serverCAFile    string
	serverInsecure  bool
	configDir       string
	insecureStorage bool
}

// cmdContext is per-invocation CLI state shared by subcommands.
type cmdContext struct {
	flags   globalFlags
	conn    *grpc.ClientConn
	printer *output.Printer
}

// New returns the root command for the fleetctl CLI.
func New() *cobra.Command {
	ctx := &cmdContext{}

	root := &cobra.Command{
		Use:          "fleetctl",
		Short:        "FleetShift command-line client",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateConfigFlags(ctx.flags); err != nil {
				return err
			}

			format := output.Format(ctx.flags.outputFormat)
			if err := format.Validate(); err != nil {
				return err
			}
			ctx.printer = output.NewPrinter(cmd.OutOrStdout(), format)

			if cmd.Annotations[annotationSkipServer] == "true" {
				return nil
			}

			conn, err := dial(ctx.flags)
			if err != nil {
				return err
			}
			ctx.conn = conn
			return nil
		},
		PersistentPostRunE: func(_ *cobra.Command, _ []string) error {
			if ctx.conn != nil {
				return ctx.conn.Close()
			}
			return nil
		},
	}

	root.PersistentFlags().StringVarP(&ctx.flags.server, "server", "s", "localhost:50051", "gRPC server address")
	root.PersistentFlags().StringVarP(&ctx.flags.outputFormat, "output", "o", string(output.FormatTable), "output format (table, json)")
	root.PersistentFlags().BoolVar(&ctx.flags.serverTLS, "server-tls", false, "Use TLS for the gRPC connection")
	root.PersistentFlags().StringVar(&ctx.flags.serverCAFile, "server-ca-file", "", "PEM CA bundle to trust for the gRPC server certificate")
	root.PersistentFlags().BoolVar(&ctx.flags.serverInsecure, "server-insecure", false, "Skip TLS certificate verification (debugging only)")
	root.PersistentFlags().StringVar(&ctx.flags.configDir, "config-dir", "", "Absolute directory for Fleetctl auth.json (default ~/.config/fleetshift)")
	root.PersistentFlags().BoolVar(&ctx.flags.insecureStorage, "insecure-storage", false, "Store OAuth tokens and the signing private key as plaintext files under --config-dir. Default is the OS keyring. Not for production.")

	root.AddCommand(newDeploymentCmd(ctx))
	root.AddCommand(newAuthCmd(ctx))
	root.AddCommand(newResourceCmd(ctx))

	return root
}

// validateConfigFlags requires --config-dir to be absolute and
// --insecure-storage to be paired with --config-dir.
func validateConfigFlags(f globalFlags) error {
	if f.configDir != "" && !filepath.IsAbs(f.configDir) {
		return fmt.Errorf("--config-dir must be an absolute path")
	}
	if f.insecureStorage && f.configDir == "" {
		return fmt.Errorf("--insecure-storage requires --config-dir")
	}
	return nil
}

// store returns the file store when --insecure-storage is set, otherwise the OS keyring.
func (f globalFlags) store() auth.Store {
	if f.insecureStorage {
		return auth.FileStore{Dir: f.configDir}
	}
	return auth.KeyringStore{}
}

// loadConfig reads auth.json from --config-dir, or the default user config path.
func (f globalFlags) loadConfig() (auth.Config, error) {
	return auth.LoadConfigFrom(f.configDir)
}

// saveConfig writes auth.json under --config-dir, or the default user config path.
func (f globalFlags) saveConfig(cfg auth.Config) error {
	return auth.SaveConfigTo(f.configDir, cfg)
}
