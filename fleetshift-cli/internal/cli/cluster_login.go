package cli

import (
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var connectionFlags = []string{"server", "server-tls", "server-ca-file", "server-insecure"}

func newClusterLoginCmd(ctx *cmdContext) *cobra.Command {
	var kubeconfigPath string
	setCurrentContext := true

	cmd := &cobra.Command{
		Use:   "login <resource-id>",
		Short: "Log into a provisioned cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resourceID := args[0]
			contextName := "fleetshift/" + resourceID

			client := pb.NewClusterServiceClient(ctx.conn)
			resp, err := client.GetClusterConnectionInfo(cmd.Context(), &pb.GetClusterConnectionInfoRequest{
				ResourceId: resourceID,
			})
			if err != nil {
				return fmt.Errorf("failed to get cluster info: %w", err)
			}

			execArgs := []string{"cluster", "token", resourceID}
			root := cmd.Root()
			for _, name := range connectionFlags {
				f := root.PersistentFlags().Lookup(name)
				if f != nil && f.Changed {
					if f.Value.Type() == "bool" {
						execArgs = append(execArgs, "--"+f.Name)
					} else {
						execArgs = append(execArgs, "--"+f.Name, f.Value.String())
					}
				}
			}

			cluster := &clientcmdapi.Cluster{
				Server: resp.GetEndpoint(),
			}
			if caCert := resp.GetCaCert(); caCert != "" {
				cluster.CertificateAuthorityData = []byte(caCert)
			}

			authInfo := &clientcmdapi.AuthInfo{
				Exec: &clientcmdapi.ExecConfig{
					APIVersion:      "client.authentication.k8s.io/v1",
					Command:         "fleetctl",
					Args:            execArgs,
					InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
				},
			}

			newContext := &clientcmdapi.Context{
				Cluster:  contextName,
				AuthInfo: contextName,
			}

			kcPath := kubeconfigPath
			if kcPath == "" {
				kcPath = os.Getenv("KUBECONFIG")
			}
			if kcPath == "" {
				home, err := os.UserHomeDir()
				if err != nil {
					return fmt.Errorf("cannot determine home directory: %w", err)
				}
				kcPath = filepath.Join(home, ".kube", "config")
			}

			existingConfig, err := clientcmd.LoadFromFile(kcPath)
			if err != nil {
				if !os.IsNotExist(err) {
					return fmt.Errorf("cannot read kubeconfig: %w", err)
				}
				existingConfig = clientcmdapi.NewConfig()
			}

			existingConfig.Clusters[contextName] = cluster
			existingConfig.AuthInfos[contextName] = authInfo
			existingConfig.Contexts[contextName] = newContext
			if setCurrentContext {
				existingConfig.CurrentContext = contextName
			}

			if err := os.MkdirAll(filepath.Dir(kcPath), 0o755); err != nil {
				return fmt.Errorf("cannot create kubeconfig directory: %w", err)
			}

			if err := clientcmd.WriteToFile(*existingConfig, kcPath); err != nil {
				return fmt.Errorf("cannot write kubeconfig: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"Logged into cluster %q. Context %q set as current context.\n",
				resourceID, contextName)
			return nil
		},
	}

	cmd.Flags().StringVar(&kubeconfigPath, "kubeconfig", "", "path to kubeconfig file (default: $KUBECONFIG or ~/.kube/config)")
	cmd.Flags().BoolVar(&setCurrentContext, "set-current-context", true, "set as current context")

	return cmd
}
