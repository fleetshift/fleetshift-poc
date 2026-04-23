package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"

	monitoringpb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
)

func main() {
	platformAddr := flag.String("platform-addr", "localhost:50051", "FleetShift platform gRPC address")
	targetID := flag.String("target-id", "fleetlet-kind-1", "Target ID to register")
	targetName := flag.String("target-name", "Kind Addon Spike", "Human-readable target name")
	monitoringAddr := flag.String("monitoring-addr", "", "Monitoring addon address (host:port); empty to skip")
	monitoringSocket := flag.String("monitoring-socket", "", "Monitoring addon Unix socket path; takes precedence over --monitoring-addr")
	metricsInterval := flag.Duration("metrics-interval", 30*time.Second, "Metrics collection interval")
	flag.Parse()

	effectiveMonitoringAddr := *monitoringAddr
	if *monitoringSocket != "" {
		effectiveMonitoringAddr = "unix:" + *monitoringSocket
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger, *platformAddr, *targetID, *targetName, effectiveMonitoringAddr, *metricsInterval); err != nil {
		logger.Error("fleetlet exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, platformAddr, targetID, targetName, monitoringAddr string, metricsInterval time.Duration) error {
	logger.Info("connecting to platform", "addr", platformAddr)

	conn, err := grpc.NewClient(platformAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial platform: %w", err)
	}
	defer conn.Close()

	client := pb.NewFleetletServiceClient(conn)

	// --- Control: register target ---

	controlStream, err := client.Control(ctx)
	if err != nil {
		return fmt.Errorf("open control stream: %w", err)
	}

	logger.Info("registering target", "target_id", targetID)
	if err := controlStream.Send(&pb.ControlEvent{
		Event: &pb.ControlEvent_RegisterTarget{
			RegisterTarget: &pb.RegisterTarget{
				TargetId:   targetID,
				TargetType: "fleetlet",
				Name:       targetName,
				Labels: map[string]string{
					"fleetshift.io/spike": "addon",
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("send register target: %w", err)
	}

	resp, err := controlStream.Recv()
	if err != nil {
		return fmt.Errorf("recv control response: %w", err)
	}

	if accepted := resp.GetTargetAccepted(); accepted != nil {
		logger.Info("target accepted", "target_id", accepted.TargetId)
	} else if errResp := resp.GetError(); errResp != nil {
		return fmt.Errorf("registration rejected: %s", errResp.Message)
	}

	// --- K8s client for SSA ---

	var dynClient dynamic.Interface
	if config, err := rest.InClusterConfig(); err == nil {
		dynClient, err = dynamic.NewForConfig(config)
		if err != nil {
			logger.Warn("failed to create dynamic k8s client", "error", err)
		}
	} else {
		logger.Warn("not running in-cluster, SSA disabled", "error", err)
	}

	// --- Monitoring addon via go-plugin ---

	var monitoringAddon *monitoringGRPCClient
	if monitoringAddr != "" {
		addon, cleanup, err := connectMonitoringAddon(logger, monitoringAddr)
		if err != nil {
			logger.Warn("failed to connect monitoring addon", "error", err)
		} else {
			monitoringAddon = addon
			defer cleanup()
			logger.Info("monitoring addon connected", "addr", monitoringAddr)
		}
	}

	// --- Deliver: handle delivery requests ---

	deliverCtx := metadata.AppendToOutgoingContext(ctx, "target-id", targetID)
	deliverStream, err := client.Deliver(deliverCtx)
	if err != nil {
		return fmt.Errorf("open deliver stream: %w", err)
	}

	logger.Info("deliver stream established, waiting for instructions")

	// Start metrics relay if monitoring addon is connected
	if monitoringAddon != nil {
		go metricsLoop(ctx, logger, deliverStream, monitoringAddon, targetID, metricsInterval)
	}

	for {
		instruction, err := deliverStream.Recv()
		if err == io.EOF {
			logger.Info("deliver stream closed by platform")
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv deliver instruction: %w", err)
		}

		switch inst := instruction.Instruction.(type) {
		case *pb.DeliverInstruction_DeliveryRequest:
			req := inst.DeliveryRequest
			logger.Info("received delivery request",
				"delivery_id", req.DeliveryId,
				"target_id", req.TargetId,
				"manifests", len(req.Manifests))

			if err := deliverStream.Send(&pb.DeliverEvent{
				Event: &pb.DeliverEvent_DeliveryAccepted{
					DeliveryAccepted: &pb.DeliveryAccepted{
						DeliveryId: req.DeliveryId,
					},
				},
			}); err != nil {
				return fmt.Errorf("send delivery accepted: %w", err)
			}

			applyErr := applyManifests(ctx, logger, dynClient, req.Manifests)

			state := pb.DeliveryCompleted_STATE_DELIVERED
			msg := "applied by fleetlet"
			if applyErr != nil {
				state = pb.DeliveryCompleted_STATE_FAILED
				msg = applyErr.Error()
			}

			if err := deliverStream.Send(&pb.DeliverEvent{
				Event: &pb.DeliverEvent_DeliveryCompleted{
					DeliveryCompleted: &pb.DeliveryCompleted{
						DeliveryId: req.DeliveryId,
						State:      state,
						Message:    msg,
					},
				},
			}); err != nil {
				return fmt.Errorf("send delivery completed: %w", err)
			}

			logger.Info("delivery completed", "delivery_id", req.DeliveryId, "state", state)

		case *pb.DeliverInstruction_RemoveRequest:
			req := inst.RemoveRequest
			logger.Info("received remove request",
				"delivery_id", req.DeliveryId,
				"target_id", req.TargetId)

			if err := deliverStream.Send(&pb.DeliverEvent{
				Event: &pb.DeliverEvent_RemoveCompleted{
					RemoveCompleted: &pb.RemoveCompleted{
						DeliveryId: req.DeliveryId,
						Success:    true,
						Message:    "removed by fleetlet",
					},
				},
			}); err != nil {
				return fmt.Errorf("send remove completed: %w", err)
			}
		}
	}
}

func connectMonitoringAddon(logger *slog.Logger, addr string) (*monitoringGRPCClient, func(), error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to monitoring addon: %w", err)
	}

	client := monitoringpb.NewMonitoringAddonClient(conn)
	addon := &monitoringGRPCClient{client: client}

	return addon, func() { conn.Close() }, nil
}

type monitoringGRPCClient struct {
	client monitoringpb.MonitoringAddonClient
}

func (c *monitoringGRPCClient) Collect(ctx context.Context) (*monitoringpb.CollectResponse, error) {
	return c.client.Collect(ctx, &monitoringpb.CollectRequest{})
}

func applyManifests(ctx context.Context, logger *slog.Logger, dynClient dynamic.Interface, manifests []*pb.Manifest) error {
	if dynClient == nil {
		for i, m := range manifests {
			logger.Info("manifest (SSA disabled, logging only)",
				"ordinal", i,
				"resource_type", m.ResourceType,
				"size", len(m.Raw))
		}
		return nil
	}

	for i, m := range manifests {
		var obj unstructured.Unstructured
		if err := json.Unmarshal(m.Raw, &obj.Object); err != nil {
			return fmt.Errorf("unmarshal manifest %d: %w", i, err)
		}

		gvr := obj.GroupVersionKind().GroupVersion().WithResource(guessResource(obj.GetKind()))
		ns := obj.GetNamespace()

		var resource dynamic.ResourceInterface
		if ns != "" {
			resource = dynClient.Resource(gvr).Namespace(ns)
		} else {
			resource = dynClient.Resource(gvr)
		}

		_, err := resource.Patch(ctx, obj.GetName(), types.ApplyPatchType, m.Raw, metav1.PatchOptions{
			FieldManager: "fleetlet",
		})
		if err != nil {
			return fmt.Errorf("apply manifest %d (%s/%s): %w", i, obj.GetKind(), obj.GetName(), err)
		}

		logger.Info("manifest applied via SSA",
			"ordinal", i,
			"kind", obj.GetKind(),
			"name", obj.GetName())
	}

	return nil
}

func guessResource(kind string) string {
	switch kind {
	case "MonitoringConfig":
		return "monitoringconfigs"
	case "Deployment":
		return "deployments"
	case "Service":
		return "services"
	case "ConfigMap":
		return "configmaps"
	default:
		return strings.ToLower(kind) + "s"
	}
}

func metricsLoop(ctx context.Context, logger *slog.Logger, stream grpc.BidiStreamingClient[pb.DeliverEvent, pb.DeliverInstruction], addon *monitoringGRPCClient, targetID string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := addon.Collect(ctx)
			if err != nil {
				logger.Warn("metrics collection failed", "error", err)
				continue
			}

			conditions := metricsToConditions(resp)

			if err := stream.Send(&pb.DeliverEvent{
				Event: &pb.DeliverEvent_ConditionReport{
					ConditionReport: &pb.DeliveryConditionReport{
						DeliveryId: "metrics-" + targetID,
						TargetId:   targetID,
						Conditions: conditions,
						ObservedAt: timestamppb.Now(),
					},
				},
			}); err != nil {
				logger.Warn("failed to send metrics report", "error", err)
				return
			}

			logger.Debug("metrics relayed",
				"nodes", len(resp.Nodes),
				"total_pods", resp.TotalPods)
		}
	}
}

type clusterMetricsJSON struct {
	Nodes   int    `json:"nodes"`
	Pods    int32  `json:"pods"`
	Cluster string `json:"cluster"`
}

type nodeMetricsJSON struct {
	Name         string `json:"name"`
	CPUCapacity  int64  `json:"cpuCapacity"`
	CPUUsage     int64  `json:"cpuUsage"`
	MemCapacity  int64  `json:"memCapacity"`
	MemUsage     int64  `json:"memUsage"`
	Pods         int32  `json:"pods"`
}

func metricsToConditions(resp *monitoringpb.CollectResponse) []*pb.Condition {
	var conditions []*pb.Condition

	clusterJSON, _ := json.Marshal(clusterMetricsJSON{
		Nodes:   len(resp.Nodes),
		Pods:    resp.TotalPods,
		Cluster: resp.ClusterId,
	})
	conditions = append(conditions, &pb.Condition{
		Type:    "ClusterMetrics",
		Status:  "True",
		Reason:  "Collected",
		Message: string(clusterJSON),
	})

	for _, n := range resp.Nodes {
		nodeJSON, _ := json.Marshal(nodeMetricsJSON{
			Name:        n.Name,
			CPUCapacity: n.CpuMillicores,
			CPUUsage:    n.CpuUsageMillicores,
			MemCapacity: n.MemoryBytes / (1024 * 1024),
			MemUsage:    n.MemoryUsageBytes / (1024 * 1024),
			Pods:        n.PodCount,
		})
		conditions = append(conditions, &pb.Condition{
			Type:    "NodeMetrics",
			Status:  "True",
			Reason:  n.Name,
			Message: string(nodeJSON),
		})
	}

	return conditions
}
