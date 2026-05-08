package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/dynamic"
	"github.com/spf13/cobra"
)

func newResourceDescribeCmd(ctx *cmdContext) *cobra.Command {
	return &cobra.Command{
		Use:   "describe <type>",
		Short: "Show the schema of a managed resource type",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			plural := args[0]

			client := dynamic.NewClient(ctx.conn)
			rt, err := client.ResolveType(cmd.Context(), plural)
			if err != nil {
				return err
			}

			info, err := client.Describe(cmd.Context(), rt)
			if err != nil {
				return fmt.Errorf("describe %s: %w", plural, err)
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Type:     %s\n", rt.Plural)
			fmt.Fprintf(out, "Singular: %s\n", rt.Singular)
			fmt.Fprintf(out, "Service:  %s\n", rt.ServiceName)
			fmt.Fprintln(out)

			fmt.Fprintln(out, "Methods:")
			for _, m := range info.Methods {
				fmt.Fprintf(out, "  %s\n", m)
			}
			fmt.Fprintln(out)

			if info.Spec != nil && len(info.Spec.Fields) > 0 {
				fmt.Fprintf(out, "Spec (%s):\n", info.Spec.FullName)
				printFieldTree(out, info.Spec.Fields, 1)
			} else {
				fmt.Fprintln(out, "Spec: (empty)")
			}

			return nil
		},
	}
}

func printFieldTree(w io.Writer, fields []dynamic.FieldSchema, depth int) {
	indent := strings.Repeat("  ", depth)
	for _, f := range fields {
		label := fieldLabel(f)
		if len(f.Fields) > 0 {
			fmt.Fprintf(w, "%s%s {\n", indent, label)
			printFieldTree(w, f.Fields, depth+1)
			fmt.Fprintf(w, "%s}\n", indent)
		} else {
			fmt.Fprintf(w, "%s%s\n", indent, label)
		}
	}
}

func fieldLabel(f dynamic.FieldSchema) string {
	var b strings.Builder

	if f.Repeated {
		b.WriteString("repeated ")
	} else if f.Optional {
		b.WriteString("optional ")
	}

	b.WriteString(shortTypeName(f.Type))
	b.WriteString(" ")
	b.WriteString(f.Name)

	b.WriteString(fmt.Sprintf(" = %d", f.Number))
	return b.String()
}

func shortTypeName(fullName string) string {
	if idx := strings.LastIndex(fullName, "."); idx >= 0 {
		return fullName[idx+1:]
	}
	return fullName
}
