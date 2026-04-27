package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/fleetshift/fleetshift-poc/addons/gen/addon/monitoring/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
)

type Collector struct {
	Client    kubernetes.Interface
	ClusterID string
}

type nodeMetricsUsage struct {
	CPU    resource.Quantity `json:"cpu"`
	Memory resource.Quantity `json:"memory"`
}

type nodeMetricsItem struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Usage nodeMetricsUsage `json:"usage"`
}

type nodeMetricsList struct {
	Items []nodeMetricsItem `json:"items"`
}

func (c *Collector) Collect(ctx context.Context) (*pb.CollectResponse, error) {
	nodes, err := c.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	pods, err := c.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	podCountByNode := make(map[string]int32)
	for _, p := range pods.Items {
		if p.Spec.NodeName != "" {
			podCountByNode[p.Spec.NodeName]++
		}
	}

	usageByNode := c.fetchNodeUsage(ctx)

	var nodeMetrics []*pb.NodeMetrics
	for _, n := range nodes.Items {
		cpuCap := n.Status.Capacity.Cpu()
		memCap := n.Status.Capacity.Memory()

		nm := &pb.NodeMetrics{
			Name:          n.Name,
			CpuMillicores: cpuCap.MilliValue(),
			MemoryBytes:   memCap.Value(),
			PodCount:      podCountByNode[n.Name],
		}

		if u, ok := usageByNode[n.Name]; ok {
			nm.CpuUsageMillicores = u.CPU.MilliValue()
			nm.MemoryUsageBytes = u.Memory.Value()
		}

		nodeMetrics = append(nodeMetrics, nm)
	}

	return &pb.CollectResponse{
		Nodes:       nodeMetrics,
		TotalPods:   int32(len(pods.Items)),
		ClusterId:   c.ClusterID,
		CollectedAt: time.Now().Unix(),
	}, nil
}

func (c *Collector) GenerateManifests(_ context.Context, _ string) ([][]byte, error) {
	return nil, fmt.Errorf("GenerateManifests is not supported on the cluster side")
}

func (c *Collector) fetchNodeUsage(ctx context.Context) map[string]nodeMetricsUsage {
	result := make(map[string]nodeMetricsUsage)

	raw, err := c.Client.Discovery().RESTClient().
		Get().AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").
		DoRaw(ctx)
	if err != nil {
		fmt.Printf("metrics-server not available: %v\n", err)
		return result
	}

	var list nodeMetricsList
	if err := json.Unmarshal(raw, &list); err != nil {
		fmt.Printf("failed to parse node metrics: %v\n", err)
		return result
	}

	for _, item := range list.Items {
		result[item.Metadata.Name] = item.Usage
	}

	return result
}
