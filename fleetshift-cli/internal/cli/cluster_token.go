package cli

import (
	"encoding/json"
	"fmt"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

func newClusterTokenCmd(ctx *cmdContext) *cobra.Command {
	return &cobra.Command{
		Use:   "token <resource-id>",
		Short: "Get a credential for a provisioned cluster (exec plugin helper)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resourceID := args[0]

			client := pb.NewClusterServiceClient(ctx.conn)
			resp, err := client.GetClusterCredential(cmd.Context(), &pb.GetClusterCredentialRequest{
				ResourceId: resourceID,
			})
			if err != nil {
				return fmt.Errorf("credential exchange failed: %w", err)
			}

			execCred := &clientauthv1.ExecCredential{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "client.authentication.k8s.io/v1",
					Kind:       "ExecCredential",
				},
				Status: &clientauthv1.ExecCredentialStatus{
					Token:               resp.GetToken(),
					ExpirationTimestamp: &metav1.Time{Time: resp.GetExpiration().AsTime()},
				},
			}

			out, err := json.Marshal(execCred)
			if err != nil {
				return fmt.Errorf("marshal exec credential: %w", err)
			}

			_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return err
		},
	}
}
