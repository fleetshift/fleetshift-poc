package main

import (
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	pb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
	"github.com/fleetshift/fleetshift-poc/addons/monitoring/internal/collector"
	monitoringplugin "github.com/fleetshift/fleetshift-poc/addons/shared/monitoring"
)

func main() {
	clusterID := os.Getenv("CLUSTER_ID")
	if clusterID == "" {
		clusterID = "unknown"
	}

	port := os.Getenv("LISTEN_PORT")
	if port == "" {
		port = "10000"
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

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		panic("failed to listen: " + err.Error())
	}

	s := grpc.NewServer()
	pb.RegisterMonitoringAddonServer(s, &monitoringplugin.GRPCServer{Impl: impl})

	// go-plugin client uses gRPC health check for Ping()
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)
	healthSrv.SetServingStatus("plugin", healthpb.HealthCheckResponse_SERVING)

	fmt.Fprintf(os.Stderr, "monitoring addon listening on :%s\n", port)
	if err := s.Serve(lis); err != nil {
		panic("serve failed: " + err.Error())
	}
}
