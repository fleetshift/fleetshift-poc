package gcphcp

import (
	"encoding/json"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterOutput captures what a successful GCP HCP cluster creation
// produces: connection info for the provisioned cluster and,
// optionally, a platform ServiceAccount token for attested delivery.
type ClusterOutput struct {
	TargetID  domain.TargetID
	Name      string
	APIServer string // e.g. "https://1.2.3.4:6443"
	CACert    []byte // PEM-encoded cluster CA certificate

	// SATokenRef and SAToken are set when platform SA bootstrapping
	// succeeds. SATokenRef is a vault key; SAToken is the raw bearer
	// token stored under that key.
	SATokenRef domain.SecretRef
	SAToken    []byte

	// TrustBundles are the IdP trust configs to embed in the
	// provisioned target's properties. Set by the GCP HCP agent from
	// its in-memory trust store at provisioning time.
	TrustBundles []domain.TrustBundleEntry
}

// Target returns a [domain.ProvisionedTarget] with connection info
// stored as properties. When a platform SA token was provisioned, the
// target includes a service_account_token_ref pointing at the vault
// secret rather than embedding the raw credential.
func (o *ClusterOutput) Target() domain.ProvisionedTarget {
	props := map[string]string{
		"api_server": o.APIServer,
	}
	if len(o.CACert) > 0 {
		props["ca_cert"] = string(o.CACert)
	}
	if o.SATokenRef != "" && len(o.SAToken) > 0 {
		props["service_account_token_ref"] = string(o.SATokenRef)
	}
	if len(o.TrustBundles) > 0 {
		if data, err := json.Marshal(o.TrustBundles); err == nil {
			props["trust_bundle"] = string(data)
		}
	}
	return domain.ProvisionedTarget{
		ID:                    o.TargetID,
		Type:                  KubernetesTargetType,
		Name:                  o.Name,
		Properties:            props,
		AcceptedManifestTypes: []domain.ManifestType{kubernetes.ManifestManifestType},
	}
}

// Secrets returns the [domain.ProducedSecret] entries that should be
// stored in the vault for this cluster. Returns nil when no platform
// SA was bootstrapped.
func (o *ClusterOutput) Secrets() []domain.ProducedSecret {
	if o.SATokenRef == "" || len(o.SAToken) == 0 {
		return nil
	}
	return []domain.ProducedSecret{{
		Ref:   o.SATokenRef,
		Value: o.SAToken,
	}}
}
