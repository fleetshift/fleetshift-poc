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
						IngressARN:              iam.IngressRoleArn,
						ImageRegistryARN:        iam.ImageRegistryRoleArn,
						StorageARN:              iam.EBSCSIDriverRoleArn,
						NetworkARN:              iam.CloudNetworkConfigControllerRoleArn,
						KubeCloudControllerARN:  iam.CloudControllerRoleArn,
						NodePoolManagementARN:   iam.NodePoolRoleArn,
						ControlPlaneOperatorARN: iam.ControlPlaneOperatorRoleArn,
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
			Services:                            defaultServicePublishingStrategy(),
			InfrastructureAvailabilityPolicy:     infraAvailabilityPolicy(spec.InfraAvailability),
		},
	}
}

func infraAvailabilityPolicy(s string) hyperv1.AvailabilityPolicy {
	if s == "SingleReplica" {
		return hyperv1.SingleReplica
	}
	return hyperv1.HighlyAvailable
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

		zone := ""
		if len(np.Zones) > 0 {
			zone = np.Zones[0]
		}
		subnetID := pickSubnet(i, zone, infra)

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
func BuildSecrets(spec ClusterSpec, platform PlatformConfig) ([]corev1.Secret, error) {
	sshKeyData := platform.SSHKey
	if len(sshKeyData) == 0 {
		var err error
		sshKeyData, err = generateSSHPublicKey()
		if err != nil {
			return nil, fmt.Errorf("generate SSH key: %w", err)
		}
	}

	encryptionKey := make([]byte, 32)
	if _, err := rand.Read(encryptionKey); err != nil {
		return nil, fmt.Errorf("generate etcd encryption key: %w", err)
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
	}, nil
}

func generateSSHPublicKey() ([]byte, error) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ssh key: %w", err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("convert ssh public key: %w", err)
	}
	return ssh.MarshalAuthorizedKey(sshPub), nil
}
