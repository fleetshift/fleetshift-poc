// Package hcp provides construction helpers for HyperShift HostedCluster
// and NodePool custom resources used by the HCP delivery agent.
package hcp

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	"github.com/openshift/hypershift/api/util/ipnet"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"golang.org/x/crypto/ssh"
)

const hostedClusterNamespace = "clusters"

// ClusterSpec describes the desired hosted cluster configuration.
type ClusterSpec struct {
	Name         string         `json:"name"`
	InfraID      string         `json:"infraID"`
	Region       string         `json:"region"`
	BaseDomain   string         `json:"baseDomain"`
	ReleaseImage string         `json:"releaseImage"`
	NodePools    []NodePoolSpec `json:"nodePools"`
}

// NodePoolSpec describes a single node pool within a hosted cluster.
type NodePoolSpec struct {
	Name         string `json:"name"`
	InstanceType string `json:"instanceType"`
	Replicas     int32  `json:"replicas"`
	Arch         string `json:"arch,omitempty"`
	Zone         string `json:"zone,omitempty"`
}

// InfraOutput holds AWS infrastructure IDs produced by the infra provisioner.
type InfraOutput struct {
	VPCID            string   `json:"vpcID"`
	PrivateSubnetIDs []string `json:"privateSubnetIDs"`
	PublicSubnetIDs  []string `json:"publicSubnetIDs"`
	Zones            []string `json:"zones"`
}

// IAMOutput holds IAM role ARNs produced by the IAM provisioner.
type IAMOutput struct {
	IngressARN              string `json:"ingressARN"`
	ImageRegistryARN        string `json:"imageRegistryARN"`
	StorageARN              string `json:"storageARN"`
	NetworkARN              string `json:"networkARN"`
	KubeCloudControllerARN  string `json:"kubeCloudControllerARN"`
	NodePoolManagementARN   string `json:"nodePoolManagementARN"`
	ControlPlaneOperatorARN string `json:"controlPlaneOperatorARN"`
}

// PlatformConfig holds secrets required for cluster provisioning.
type PlatformConfig struct {
	PullSecret []byte `json:"pullSecret"`
	SSHKey     []byte `json:"sshKey,omitempty"`
}

// BuildHostedCluster constructs a HyperShift HostedCluster object from
// the provided cluster spec, infrastructure output, IAM output, and
// platform configuration.
func BuildHostedCluster(spec ClusterSpec, infra InfraOutput, iam IAMOutput, _ PlatformConfig) hyperv1.HostedCluster {
	return hyperv1.HostedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: hostedClusterNamespace,
		},
		Spec: hyperv1.HostedClusterSpec{
			InfraID: spec.InfraID,
			Release: hyperv1.Release{
				Image: spec.ReleaseImage,
			},
			Platform: hyperv1.PlatformSpec{
				Type: hyperv1.AWSPlatform,
				AWS: &hyperv1.AWSPlatformSpec{
					Region: spec.Region,
					CloudProviderConfig: &hyperv1.AWSCloudProviderConfig{
						VPC: infra.VPCID,
					},
					RolesRef: hyperv1.AWSRolesRef{
						IngressARN:              iam.IngressARN,
						ImageRegistryARN:        iam.ImageRegistryARN,
						StorageARN:              iam.StorageARN,
						NetworkARN:              iam.NetworkARN,
						KubeCloudControllerARN:  iam.KubeCloudControllerARN,
						NodePoolManagementARN:   iam.NodePoolManagementARN,
						ControlPlaneOperatorARN: iam.ControlPlaneOperatorARN,
					},
				},
			},
			Networking: hyperv1.ClusterNetworking{
				NetworkType: hyperv1.OVNKubernetes,
				ClusterNetwork: []hyperv1.ClusterNetworkEntry{
					{CIDR: *ipnet.MustParseCIDR("10.132.0.0/14")},
				},
				ServiceNetwork: []hyperv1.ServiceNetworkEntry{
					{CIDR: *ipnet.MustParseCIDR("172.31.0.0/16")},
				},
			},
			PullSecret: corev1.LocalObjectReference{
				Name: spec.Name + "-pull-secret",
			},
			SSHKey: corev1.LocalObjectReference{
				Name: spec.Name + "-ssh-key",
			},
			Etcd: hyperv1.EtcdSpec{
				ManagementType: hyperv1.Managed,
			},
			SecretEncryption: &hyperv1.SecretEncryptionSpec{
				Type: hyperv1.AESCBC,
				AESCBC: &hyperv1.AESCBCSpec{
					ActiveKey: corev1.LocalObjectReference{
						Name: spec.Name + "-etcd-encryption-key",
					},
				},
			},
			Services: defaultServicePublishingStrategy(),
		},
	}
}

func defaultServicePublishingStrategy() []hyperv1.ServicePublishingStrategyMapping {
	return []hyperv1.ServicePublishingStrategyMapping{
		{
			Service:                   hyperv1.APIServer,
			ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.LoadBalancer},
		},
		{
			Service:                   hyperv1.OAuthServer,
			ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.Route},
		},
		{
			Service:                   hyperv1.Konnectivity,
			ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.Route},
		},
		{
			Service:                   hyperv1.Ignition,
			ServicePublishingStrategy: hyperv1.ServicePublishingStrategy{Type: hyperv1.Route},
		},
	}
}

// BuildNodePools constructs HyperShift NodePool objects, one per entry
// in the cluster spec. Subnets are assigned round-robin from the
// infrastructure's private subnet list.
func BuildNodePools(spec ClusterSpec, infra InfraOutput) []hyperv1.NodePool {
	pools := make([]hyperv1.NodePool, len(spec.NodePools))
	for i, np := range spec.NodePools {
		replicas := np.Replicas

		subnetID := pickSubnet(i, np.Zone, infra)

		pools[i] = hyperv1.NodePool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      spec.Name + "-" + np.Name,
				Namespace: hostedClusterNamespace,
			},
			Spec: hyperv1.NodePoolSpec{
				ClusterName: spec.Name,
				Release: hyperv1.Release{
					Image: spec.ReleaseImage,
				},
				Replicas: ptr.To(replicas),
				Platform: hyperv1.NodePoolPlatform{
					Type: hyperv1.AWSPlatform,
					AWS: &hyperv1.AWSNodePoolPlatform{
						InstanceType: np.InstanceType,
						Subnet: hyperv1.AWSResourceReference{
							ID: ptr.To(subnetID),
						},
					},
				},
				Management: hyperv1.NodePoolManagement{
					UpgradeType: hyperv1.UpgradeTypeReplace,
					AutoRepair:  true,
				},
			},
		}
	}
	return pools
}

func pickSubnet(index int, zone string, infra InfraOutput) string {
	if len(infra.PrivateSubnetIDs) == 0 {
		return ""
	}
	// If a zone is specified, try to match by index in zones list.
	if zone != "" {
		for j, z := range infra.Zones {
			if z == zone && j < len(infra.PrivateSubnetIDs) {
				return infra.PrivateSubnetIDs[j]
			}
		}
	}
	return infra.PrivateSubnetIDs[index%len(infra.PrivateSubnetIDs)]
}

// BuildSecrets creates the Kubernetes Secrets needed for a HostedCluster:
// pull secret, SSH key, and etcd encryption key.
func BuildSecrets(spec ClusterSpec, platform PlatformConfig) []corev1.Secret {
	sshKeyData := platform.SSHKey
	if len(sshKeyData) == 0 {
		sshKeyData = generateSSHPublicKey()
	}

	encryptionKey := make([]byte, 32)
	if _, err := rand.Read(encryptionKey); err != nil {
		panic(fmt.Sprintf("generate etcd encryption key: %v", err))
	}

	return []corev1.Secret{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      spec.Name + "-pull-secret",
				Namespace: hostedClusterNamespace,
			},
			Data: map[string][]byte{
				".dockerconfigjson": platform.PullSecret,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      spec.Name + "-ssh-key",
				Namespace: hostedClusterNamespace,
			},
			Data: map[string][]byte{
				"id_rsa.pub": sshKeyData,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      spec.Name + "-etcd-encryption-key",
				Namespace: hostedClusterNamespace,
			},
			Data: map[string][]byte{
				"key": encryptionKey,
			},
		},
	}
}

func generateSSHPublicKey() []byte {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("generate ssh key: %v", err))
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		panic(fmt.Sprintf("convert ssh public key: %v", err))
	}
	return ssh.MarshalAuthorizedKey(sshPub)
}
