package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/go-plugin"
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
	monitoringplugin "github.com/fleetshift/fleetshift-poc/addons/shared/monitoring"
)

func main() {
	platformAddr := flag.String("platform-addr", "localhost:50051", "FleetShift platform gRPC address")
	targetID := flag.String("target-id", "fleetlet-kind-1", "Target ID to register")
	targetName := flag.String("target-name", "Kind Addon Spike", "Human-readable target name")
	pluginDir := flag.String("plugin-dir", "", "Directory containing go-plugin Unix sockets (shared emptyDir)")
	metricsInterval := flag.Duration("metrics-interval", 30*time.Second, "Metrics collection and heartbeat interval")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backoff := 1 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		err := run(ctx, logger, *platformAddr, *targetID, *targetName, *pluginDir, *metricsInterval)
		if ctx.Err() != nil {
			logger.Info("fleetlet shut down gracefully")
			return
		}

		logger.Warn("fleetlet disconnected, will reconnect",
			"error", err, "backoff", backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			logger.Info("fleetlet shut down during backoff")
			return
		}

		backoff = min(backoff*2, maxBackoff)
	}
}

func run(ctx context.Context, logger *slog.Logger, platformAddr, targetID, targetName, pluginDir string, metricsInterval time.Duration) error {
	// runCtx keeps streams alive until we explicitly cancel after sending Goodbye.
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

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

	controlStream, err := client.Control(runCtx)
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

	// --- Monitoring addon via go-plugin (sidecar) ---

	var monAddon monitoringplugin.MonitoringAddon
	var addons []*addonStatus
	if pluginDir != "" {
		addon, cleanup, err := connectMonitoringPlugin(runCtx, logger, pluginDir)
		if err != nil {
			logger.Warn("failed to connect monitoring plugin", "error", err)
			addons = append(addons, &addonStatus{name: "monitoring", connected: false, lastError: err.Error()})
		} else {
			monAddon = addon
			defer cleanup()
			logger.Info("monitoring plugin connected via go-plugin")
			addons = append(addons, &addonStatus{name: "monitoring", connected: true})
		}
	}

	// --- Deliver: handle delivery requests ---

	deliverCtx := metadata.AppendToOutgoingContext(runCtx, "target-id", targetID)
	deliverStream, err := client.Deliver(deliverCtx)
	if err != nil {
		return fmt.Errorf("open deliver stream: %w", err)
	}

	logger.Info("deliver stream established, waiting for instructions")

	// Start heartbeat loop (always, even without addons)
	go heartbeatLoop(runCtx, logger, deliverStream, targetID, metricsInterval, addons)

	if monAddon != nil {
		go metricsLoop(runCtx, logger, deliverStream, monAddon, targetID, metricsInterval, addons[0])
	}

	// Graceful shutdown: send Goodbye before tearing down streams.
	go func() {
		<-ctx.Done()
		logger.Info("shutdown signal received, sending goodbye")
		_ = controlStream.Send(&pb.ControlEvent{
			Event: &pb.ControlEvent_Goodbye{
				Goodbye: &pb.Goodbye{
					TargetId:  targetID,
					Reason:    "SIGTERM",
					Timestamp: timestamppb.Now(),
				},
			},
		})
		time.Sleep(500 * time.Millisecond)
		runCancel()
	}()

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

			applyErr := applyManifests(runCtx, logger, dynClient, req.Manifests)

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

// addonStatus tracks the health of a single go-plugin addon.
type addonStatus struct {
	mu               sync.Mutex
	name             string
	connected        bool
	lastCollectionAt time.Time
	lastError        string
}

func (s *addonStatus) setSuccess(t time.Time) {
	s.mu.Lock()
	s.connected = true
	s.lastCollectionAt = t
	s.lastError = ""
	s.mu.Unlock()
}

func (s *addonStatus) setError(err error) {
	s.mu.Lock()
	s.lastError = err.Error()
	s.mu.Unlock()
}

func (s *addonStatus) toProto() *pb.AddonHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	ah := &pb.AddonHealth{
		Name:      s.name,
		Connected: s.connected,
		Message:   s.lastError,
	}
	if !s.lastCollectionAt.IsZero() {
		ah.LastCollectionTime = timestamppb.New(s.lastCollectionAt)
	}
	return ah
}

// heartbeatLoop sends periodic Heartbeat messages on the deliver stream.
func heartbeatLoop(
	ctx context.Context,
	logger *slog.Logger,
	stream grpc.BidiStreamingClient[pb.DeliverEvent, pb.DeliverInstruction],
	targetID string,
	interval time.Duration,
	addons []*addonStatus,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var health []*pb.AddonHealth
			for _, a := range addons {
				health = append(health, a.toProto())
			}

			if err := stream.Send(&pb.DeliverEvent{
				Event: &pb.DeliverEvent_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						TargetId:    targetID,
						Timestamp:   timestamppb.Now(),
						AddonHealth: health,
					},
				},
			}); err != nil {
				logger.Warn("failed to send heartbeat", "error", err)
				return
			}
			logger.Debug("heartbeat sent", "addons", len(health))
		}
	}
}

// connectMonitoringPlugin discovers a go-plugin Unix socket in pluginDir and
// connects via ReattachConfig. The sidecar container creates the socket using
// plugin.Serve() with PLUGIN_UNIX_SOCKET_DIR; the name is random so we glob.
func connectMonitoringPlugin(ctx context.Context, logger *slog.Logger, pluginDir string) (monitoringplugin.MonitoringAddon, func(), error) {
	socketPath, err := waitForSocket(ctx, logger, pluginDir, 30*time.Second)
	if err != nil {
		return nil, nil, err
	}

	logger.Info("discovered plugin socket", "path", socketPath)

	c := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: monitoringplugin.Handshake,
		Plugins:         monitoringplugin.PluginMap,
		Reattach: &plugin.ReattachConfig{
			Protocol:        plugin.ProtocolGRPC,
			ProtocolVersion: 1,
			Addr:            &net.UnixAddr{Name: socketPath, Net: "unix"},
			Test:            true,
		},
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
	})

	rpcClient, err := c.Client()
	if err != nil {
		c.Kill()
		return nil, nil, fmt.Errorf("get rpc client: %w", err)
	}

	raw, err := rpcClient.Dispense("monitoring")
	if err != nil {
		c.Kill()
		return nil, nil, fmt.Errorf("dispense monitoring: %w", err)
	}

	addon := raw.(monitoringplugin.MonitoringAddon)
	return addon, func() { c.Kill() }, nil
}

func waitForSocket(ctx context.Context, logger *slog.Logger, dir string, timeout time.Duration) (string, error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timed out waiting for plugin socket in %s", dir)
		case <-ticker.C:
			matches, _ := filepath.Glob(filepath.Join(dir, "plugin*"))
			if len(matches) > 0 {
				return matches[0], nil
			}
			logger.Debug("waiting for plugin socket", "dir", dir)
		}
	}
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

func metricsLoop(ctx context.Context, logger *slog.Logger, stream grpc.BidiStreamingClient[pb.DeliverEvent, pb.DeliverInstruction], addon monitoringplugin.MonitoringAddon, targetID string, interval time.Duration, status *addonStatus) {
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
				status.setError(err)
				continue
			}

			status.setSuccess(time.Now())
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
