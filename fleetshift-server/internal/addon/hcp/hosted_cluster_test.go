package hcp

import (
	"testing"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
)

func defaultClusterSpec() ClusterSpec {
	return ClusterSpec{
		Name:         "my-cluster",
		InfraID:      "my-cluster-abc123",
		Region:       "us-east-1",
		BaseDomain:   "example.com",
		ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
		NodePools: []NodePoolSpec{
			{
				Name:         "workers",
				InstanceType: "m5.xlarge",
				Replicas:     3,
			},
			{
				Name:         "infra",
				InstanceType: "m5.2xlarge",
				Replicas:     2,
				Arch:         "amd64",
			},
		},
	}
}

func defaultInfraOutput() InfraOutput {
	return InfraOutput{
		VPCID:            "vpc-0123456789abcdef0",
		PrivateSubnetIDs: []string{"subnet-priv-1", "subnet-priv-2"},
		PublicSubnetIDs:  []string{"subnet-pub-1", "subnet-pub-2"},
		Zones:            []string{"us-east-1a", "us-east-1b"},
	}
}

func defaultIAMOutput() IAMOutput {
	return IAMOutput{
		IngressRoleArn:                     "arn:aws:iam::123456789012:role/my-cluster-ingress",
		ImageRegistryRoleArn:               "arn:aws:iam::123456789012:role/my-cluster-image-registry",
		EBSCSIDriverRoleArn:                "arn:aws:iam::123456789012:role/my-cluster-storage",
		CloudNetworkConfigControllerRoleArn: "arn:aws:iam::123456789012:role/my-cluster-network",
		CloudControllerRoleArn:             "arn:aws:iam::123456789012:role/my-cluster-kube-cloud-controller",
		NodePoolRoleArn:                    "arn:aws:iam::123456789012:role/my-cluster-nodepool-mgmt",
		ControlPlaneOperatorRoleArn:        "arn:aws:iam::123456789012:role/my-cluster-cpo",
	}
}

func defaultPlatformConfig() PlatformConfig {
	return PlatformConfig{
		PullSecret: []byte(`{"auths":{"quay.io":{"auth":"dGVzdDp0ZXN0"}}}`),
		SSHKey:     []byte("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI test@example.com"),
	}
}

func TestBuildHostedCluster(t *testing.T) {
	spec := defaultClusterSpec()
	infra := defaultInfraOutput()
	iam := defaultIAMOutput()
	platform := defaultPlatformConfig()

	hc := BuildHostedCluster(spec, infra, iam, platform)

	t.Run("metadata", func(t *testing.T) {
		if hc.Name != "my-cluster" {
			t.Errorf("name = %q, want %q", hc.Name, "my-cluster")
		}
		if hc.Namespace != "clusters" {
			t.Errorf("namespace = %q, want %q", hc.Namespace, "clusters")
		}
	})

	t.Run("infraID", func(t *testing.T) {
		if hc.Spec.InfraID != "my-cluster-abc123" {
			t.Errorf("infraID = %q, want %q", hc.Spec.InfraID, "my-cluster-abc123")
		}
	})

	t.Run("platform_type", func(t *testing.T) {
		if hc.Spec.Platform.Type != hyperv1.AWSPlatform {
			t.Errorf("platform type = %q, want %q", hc.Spec.Platform.Type, hyperv1.AWSPlatform)
		}
	})

	t.Run("aws_region", func(t *testing.T) {
		if hc.Spec.Platform.AWS == nil {
			t.Fatal("platform.AWS is nil")
		}
		if hc.Spec.Platform.AWS.Region != "us-east-1" {
			t.Errorf("region = %q, want %q", hc.Spec.Platform.AWS.Region, "us-east-1")
		}
	})

	t.Run("aws_vpc", func(t *testing.T) {
		if hc.Spec.Platform.AWS.CloudProviderConfig == nil {
			t.Fatal("cloudProviderConfig is nil")
		}
		if hc.Spec.Platform.AWS.CloudProviderConfig.VPC != "vpc-0123456789abcdef0" {
			t.Errorf("VPC = %q, want %q", hc.Spec.Platform.AWS.CloudProviderConfig.VPC, "vpc-0123456789abcdef0")
		}
	})

	t.Run("aws_roles", func(t *testing.T) {
		roles := hc.Spec.Platform.AWS.RolesRef
		if roles.IngressARN != iam.IngressRoleArn {
			t.Errorf("IngressARN = %q, want %q", roles.IngressARN, iam.IngressRoleArn)
		}
		if roles.ControlPlaneOperatorARN != iam.ControlPlaneOperatorRoleArn {
			t.Errorf("ControlPlaneOperatorARN = %q, want %q", roles.ControlPlaneOperatorARN, iam.ControlPlaneOperatorRoleArn)
		}
	})

	t.Run("networking", func(t *testing.T) {
		if len(hc.Spec.Networking.ClusterNetwork) != 1 {
			t.Fatalf("clusterNetwork count = %d, want 1", len(hc.Spec.Networking.ClusterNetwork))
		}
		if hc.Spec.Networking.ClusterNetwork[0].CIDR.String() != "10.132.0.0/14" {
			t.Errorf("clusterNetwork CIDR = %q, want %q", hc.Spec.Networking.ClusterNetwork[0].CIDR.String(), "10.132.0.0/14")
		}
		if len(hc.Spec.Networking.ServiceNetwork) != 1 {
			t.Fatalf("serviceNetwork count = %d, want 1", len(hc.Spec.Networking.ServiceNetwork))
		}
		if hc.Spec.Networking.ServiceNetwork[0].CIDR.String() != "172.31.0.0/16" {
			t.Errorf("serviceNetwork CIDR = %q, want %q", hc.Spec.Networking.ServiceNetwork[0].CIDR.String(), "172.31.0.0/16")
		}
	})

	t.Run("release_image", func(t *testing.T) {
		if hc.Spec.Release.Image != spec.ReleaseImage {
			t.Errorf("release image = %q, want %q", hc.Spec.Release.Image, spec.ReleaseImage)
		}
	})

	t.Run("pull_secret_ref", func(t *testing.T) {
		if hc.Spec.PullSecret.Name != "my-cluster-pull-secret" {
			t.Errorf("pullSecret name = %q, want %q", hc.Spec.PullSecret.Name, "my-cluster-pull-secret")
		}
	})

	t.Run("ssh_key_ref", func(t *testing.T) {
		if hc.Spec.SSHKey.Name != "my-cluster-ssh-key" {
			t.Errorf("sshKey name = %q, want %q", hc.Spec.SSHKey.Name, "my-cluster-ssh-key")
		}
	})

	t.Run("etcd_managed", func(t *testing.T) {
		if hc.Spec.Etcd.ManagementType != hyperv1.Managed {
			t.Errorf("etcd management = %q, want %q", hc.Spec.Etcd.ManagementType, hyperv1.Managed)
		}
	})

	t.Run("secret_encryption", func(t *testing.T) {
		if hc.Spec.SecretEncryption == nil {
			t.Fatal("secretEncryption is nil")
		}
		if hc.Spec.SecretEncryption.Type != hyperv1.AESCBC {
			t.Errorf("encryption type = %q, want %q", hc.Spec.SecretEncryption.Type, hyperv1.AESCBC)
		}
		if hc.Spec.SecretEncryption.AESCBC == nil {
			t.Fatal("aescbc is nil")
		}
		if hc.Spec.SecretEncryption.AESCBC.ActiveKey.Name != "my-cluster-etcd-encryption-key" {
			t.Errorf("aescbc activeKey = %q, want %q", hc.Spec.SecretEncryption.AESCBC.ActiveKey.Name, "my-cluster-etcd-encryption-key")
		}
	})

	t.Run("services", func(t *testing.T) {
		if len(hc.Spec.Services) < 3 {
			t.Fatalf("services count = %d, want >= 3", len(hc.Spec.Services))
		}
	})
}

func TestBuildNodePools(t *testing.T) {
	spec := defaultClusterSpec()
	infra := defaultInfraOutput()

	pools := BuildNodePools(spec, infra)

	t.Run("count_matches_spec", func(t *testing.T) {
		if len(pools) != len(spec.NodePools) {
			t.Fatalf("pool count = %d, want %d", len(pools), len(spec.NodePools))
		}
	})

	t.Run("first_pool", func(t *testing.T) {
		np := pools[0]
		if np.Name != "my-cluster-workers" {
			t.Errorf("name = %q, want %q", np.Name, "my-cluster-workers")
		}
		if np.Namespace != "clusters" {
			t.Errorf("namespace = %q, want %q", np.Namespace, "clusters")
		}
		if np.Spec.ClusterName != "my-cluster" {
			t.Errorf("clusterName = %q, want %q", np.Spec.ClusterName, "my-cluster")
		}
		if np.Spec.Platform.Type != hyperv1.AWSPlatform {
			t.Errorf("platform type = %q, want %q", np.Spec.Platform.Type, hyperv1.AWSPlatform)
		}
		if np.Spec.Platform.AWS == nil {
			t.Fatal("platform.AWS is nil")
		}
		if np.Spec.Platform.AWS.InstanceType != "m5.xlarge" {
			t.Errorf("instance type = %q, want %q", np.Spec.Platform.AWS.InstanceType, "m5.xlarge")
		}
		if np.Spec.Replicas == nil || *np.Spec.Replicas != 3 {
			t.Errorf("replicas = %v, want 3", np.Spec.Replicas)
		}
		if np.Spec.Release.Image != spec.ReleaseImage {
			t.Errorf("release image = %q, want %q", np.Spec.Release.Image, spec.ReleaseImage)
		}
	})

	t.Run("second_pool", func(t *testing.T) {
		np := pools[1]
		if np.Name != "my-cluster-infra" {
			t.Errorf("name = %q, want %q", np.Name, "my-cluster-infra")
		}
		if np.Spec.Platform.AWS.InstanceType != "m5.2xlarge" {
			t.Errorf("instance type = %q, want %q", np.Spec.Platform.AWS.InstanceType, "m5.2xlarge")
		}
		if np.Spec.Replicas == nil || *np.Spec.Replicas != 2 {
			t.Errorf("replicas = %v, want 2", np.Spec.Replicas)
		}
	})

	t.Run("subnet_from_infra", func(t *testing.T) {
		np := pools[0]
		if np.Spec.Platform.AWS.Subnet.ID == nil || *np.Spec.Platform.AWS.Subnet.ID == "" {
			t.Error("subnet ID should be set from infra private subnets")
		}
	})
}

func TestBuildSecrets(t *testing.T) {
	spec := defaultClusterSpec()
	platform := defaultPlatformConfig()

	secrets := BuildSecrets(spec, platform)

	t.Run("count", func(t *testing.T) {
		if len(secrets) != 3 {
			t.Fatalf("secret count = %d, want 3", len(secrets))
		}
	})

	t.Run("pull_secret", func(t *testing.T) {
		s := secrets[0]
		if s.Name != "my-cluster-pull-secret" {
			t.Errorf("name = %q, want %q", s.Name, "my-cluster-pull-secret")
		}
		if s.Namespace != "clusters" {
			t.Errorf("namespace = %q, want %q", s.Namespace, "clusters")
		}
		if string(s.Data[".dockerconfigjson"]) != string(platform.PullSecret) {
			t.Error("pull secret data mismatch")
		}
	})

	t.Run("ssh_key", func(t *testing.T) {
		s := secrets[1]
		if s.Name != "my-cluster-ssh-key" {
			t.Errorf("name = %q, want %q", s.Name, "my-cluster-ssh-key")
		}
		if len(s.Data["id_rsa.pub"]) == 0 {
			t.Error("ssh key data is empty")
		}
	})

	t.Run("etcd_encryption_key", func(t *testing.T) {
		s := secrets[2]
		if s.Name != "my-cluster-etcd-encryption-key" {
			t.Errorf("name = %q, want %q", s.Name, "my-cluster-etcd-encryption-key")
		}
		key := s.Data["key"]
		if len(key) == 0 {
			t.Error("etcd encryption key data is empty")
		}
	})

	t.Run("ssh_key_generated_when_empty", func(t *testing.T) {
		noSSH := PlatformConfig{
			PullSecret: platform.PullSecret,
		}
		secrets := BuildSecrets(spec, noSSH)
		sshSecret := secrets[1]
		if len(sshSecret.Data["id_rsa.pub"]) == 0 {
			t.Error("ssh key should be auto-generated when not provided")
		}
	})
}
