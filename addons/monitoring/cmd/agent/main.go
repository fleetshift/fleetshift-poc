package main

import (
	"os"

	"github.com/hashicorp/go-plugin"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/fleetshift/fleetshift-poc/addons/monitoring/internal/collector"
	monitoringplugin "github.com/fleetshift/fleetshift-poc/addons/shared/monitoring"
)

func main() {
	clusterID := os.Getenv("CLUSTER_ID")
	if clusterID == "" {
		clusterID = "unknown"
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		panic("failed to get in-cluster config: " + err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic("failed to create kubernetes client: " + err.Error())
	}

	impl := &collector.Collector{
		Client:    clientset,
		ClusterID: clusterID,
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: monitoringplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"monitoring": &monitoringplugin.MonitoringGRPCPlugin{Impl: impl},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
