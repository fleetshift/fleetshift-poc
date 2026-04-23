package manifests

import (
	"encoding/json"
	"fmt"
)

type MonitoringConfig struct {
	APIVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Metadata   MonitoringMetadata   `json:"metadata"`
	Spec       MonitoringConfigSpec `json:"spec"`
}

type MonitoringMetadata struct {
	Name string `json:"name"`
}

type MonitoringConfigSpec struct {
	Interval     string `json:"interval"`
	CollectNodes bool   `json:"collectNodes"`
	CollectPods  bool   `json:"collectPods"`
}

func GenerateMonitoringConfig(name, interval string, collectNodes, collectPods bool) ([]byte, error) {
	cfg := MonitoringConfig{
		APIVersion: "monitoring.fleetshift.io/v1alpha1",
		Kind:       "MonitoringConfig",
		Metadata:   MonitoringMetadata{Name: name},
		Spec: MonitoringConfigSpec{
			Interval:     interval,
			CollectNodes: collectNodes,
			CollectPods:  collectPods,
		},
	}

	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal monitoring config: %w", err)
	}
	return raw, nil
}
