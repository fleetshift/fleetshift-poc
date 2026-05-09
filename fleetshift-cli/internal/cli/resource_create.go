package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/dynamic"
	"github.com/spf13/cobra"
)

func newResourceCreateCmd(ctx *cmdContext) *cobra.Command {
	var (
		id       string
		specFile string
	)

	cmd := &cobra.Command{
		Use:   "create <type>",
		Short: "Create a managed resource",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			plural := args[0]

			specJSON, err := readSpecFile(specFile)
			if err != nil {
				return err
			}

			client := dynamic.NewClient(ctx.conn)
			rt, err := client.ResolveType(cmd.Context(), plural)
			if err != nil {
				return err
			}

			resp, err := client.Create(cmd.Context(), rt, id, specJSON)
			if err != nil {
				return fmt.Errorf("create %s: %w", plural, err)
			}

			return ctx.printer.PrintResource(resp, resourceColumns())
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "resource identifier (required)")
	cmd.Flags().StringVar(&specFile, "spec-file", "", "path to spec JSON file (use - for stdin)")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("spec-file")

	return cmd
}

func readSpecFile(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}
