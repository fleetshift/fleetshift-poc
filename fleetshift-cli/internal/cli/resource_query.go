package cli

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
)

type queryResourcesFlags struct {
	scope     string
	filter    string
	pageSize  int32
	pageToken string
	orderBy   string
}

func newResourceQueryCmd(ctx *cmdContext) *cobra.Command {
	f := &queryResourcesFlags{}

	cmd := &cobra.Command{
		Use:     "query",
		Aliases: []string{"search"},
		Short:   "Query managed resources across types with a CEL filter",
		Long: `Query managed extension resources across the platform.

The filter is CEL (not AIP-160 list-filter syntax), evaluated by the
server against the query envelope and resource body. Empty filter
matches all resources in scope.

Examples:
  # All kind clusters in us-east-1 that are active
  fleetctl resource query \
    --filter 'resource_type == "kind.fleetshift.io/Cluster" && resource.spec.region == "us-east-1" && resource.state == "ACTIVE"'

  # Resources whose envelope name starts with a service prefix
  fleetctl resource query \
    --filter 'name.startsWith("//kind.fleetshift.io/")'

  # Stable type+name order, first page of 20
  fleetctl resource query --order-by resource_type,name --page-size 20

  # Continue from a previous page (token from -o json or the table hint)
  fleetctl resource query --page-token '<token>' --filter '...'

In v0, --scope must be "-" (whole platform).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := pb.NewResourceQueryServiceClient(ctx.conn)

			resp, err := client.QueryResources(cmd.Context(), &pb.QueryResourcesRequest{
				Scope:     f.scope,
				Filter:    f.filter,
				PageSize:  f.pageSize,
				PageToken: f.pageToken,
				OrderBy:   f.orderBy,
			})
			if err != nil {
				return fmt.Errorf("query resources: %w", err)
			}

			if output.Format(ctx.flags.outputFormat) == output.FormatJSON {
				// Full response so callers retain nextPageToken.
				return ctx.printer.PrintResource(resp, nil)
			}

			msgs := make([]proto.Message, len(resp.GetResources()))
			for i, r := range resp.GetResources() {
				msgs[i] = r
			}
			if err := ctx.printer.PrintResourceList(msgs, queryResultColumns()); err != nil {
				return err
			}

			if tok := resp.GetNextPageToken(); tok != "" {
				hint := fmt.Sprintf("\nMore results available. Continue with:\n  fleetctl resource query --page-token %s", shellQuote(tok))
				if f.filter != "" {
					hint += fmt.Sprintf(" --filter %s", shellQuote(f.filter))
				}
				if f.orderBy != "" {
					hint += fmt.Sprintf(" --order-by %s", shellQuote(f.orderBy))
				}
				if f.pageSize > 0 {
					hint += fmt.Sprintf(" --page-size %d", f.pageSize)
				}
				if f.scope != "" && f.scope != "-" {
					hint += fmt.Sprintf(" --scope %s", shellQuote(f.scope))
				}
				fmt.Fprintln(cmd.ErrOrStderr(), hint)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&f.scope, "scope", "-", "query scope (v0: only \"-\" for whole platform)")
	cmd.Flags().StringVar(&f.filter, "filter", "", "CEL filter expression (empty matches all)")
	cmd.Flags().Int32Var(&f.pageSize, "page-size", 0, "maximum number of results to return (0 = server default)")
	cmd.Flags().StringVar(&f.pageToken, "page-token", "", "opaque token from a previous query to fetch the next page")
	cmd.Flags().StringVar(&f.orderBy, "order-by", "", "ordering: empty (server default) or \"resource_type,name\"")

	return cmd
}

func queryResultColumns() []output.Column {
	return []output.Column{
		{Header: "Name", Value: func(m proto.Message) string {
			r := m.(*pb.ResourceResult)
			if n := structStringField(r.GetResource(), "name"); n != "" {
				return n
			}
			return r.GetName()
		}},
		{Header: "Type", Value: func(m proto.Message) string {
			return m.(*pb.ResourceResult).GetResourceType()
		}},
		{Header: "State", Value: func(m proto.Message) string {
			r := m.(*pb.ResourceResult)
			s := trimStatePrefix(structStringField(r.GetResource(), "state"))
			if pr := structStringField(r.GetResource(), "pauseReason"); pr != "" {
				s += " (Paused)"
			}
			if s == "" {
				return "-"
			}
			return s
		}},
		{Header: "UID", Value: func(m proto.Message) string {
			uid := structStringField(m.(*pb.ResourceResult).GetResource(), "uid")
			if uid == "" {
				return "-"
			}
			return uid
		}},
		{Header: "Age", Value: func(m proto.Message) string {
			raw := structStringField(m.(*pb.ResourceResult).GetResource(), "createTime")
			if raw == "" {
				return "-"
			}
			t, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				t, err = time.Parse(time.RFC3339, raw)
			}
			if err != nil {
				return "-"
			}
			return formatAge(t)
		}},
	}
}

func structStringField(s *structpb.Struct, key string) string {
	if s == nil || s.Fields == nil {
		return ""
	}
	v, ok := s.Fields[key]
	if !ok || v == nil {
		return ""
	}
	switch kind := v.Kind.(type) {
	case *structpb.Value_StringValue:
		return kind.StringValue
	default:
		return v.String()
	}
}

// shellQuote wraps s in single quotes for a copy-pasteable shell hint,
// escaping any embedded single quotes.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
