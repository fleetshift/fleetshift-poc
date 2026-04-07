package hcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// IAMAPI is the subset of the IAM client used by the HCP agent.
type IAMAPI interface {
	CreateOpenIDConnectProvider(ctx context.Context, input *iam.CreateOpenIDConnectProviderInput, optFns ...func(*iam.Options)) (*iam.CreateOpenIDConnectProviderOutput, error)
	DeleteOpenIDConnectProvider(ctx context.Context, input *iam.DeleteOpenIDConnectProviderInput, optFns ...func(*iam.Options)) (*iam.DeleteOpenIDConnectProviderOutput, error)
	CreateRole(ctx context.Context, input *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error)
	DeleteRole(ctx context.Context, input *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error)
	PutRolePolicy(ctx context.Context, input *iam.PutRolePolicyInput, optFns ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error)
	DeleteRolePolicy(ctx context.Context, input *iam.DeleteRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error)
	CreateInstanceProfile(ctx context.Context, input *iam.CreateInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.CreateInstanceProfileOutput, error)
	DeleteInstanceProfile(ctx context.Context, input *iam.DeleteInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.DeleteInstanceProfileOutput, error)
	AddRoleToInstanceProfile(ctx context.Context, input *iam.AddRoleToInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.AddRoleToInstanceProfileOutput, error)
	RemoveRoleFromInstanceProfile(ctx context.Context, input *iam.RemoveRoleFromInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.RemoveRoleFromInstanceProfileOutput, error)
	ListRolePolicies(ctx context.Context, input *iam.ListRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error)
	ListInstanceProfilesForRole(ctx context.Context, input *iam.ListInstanceProfilesForRoleInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error)
}

// IAMOutput captures all IAM resource identifiers created for an HCP cluster.
type IAMOutput struct {
	OIDCProviderArn                       string
	CloudControllerRoleArn                string
	NodePoolRoleArn                       string
	ControlPlaneOperatorRoleArn           string
	CloudNetworkConfigControllerRoleArn   string
	IngressRoleArn                        string
	ImageRegistryRoleArn                  string
	EBSCSIDriverRoleArn                   string
	WorkerRoleArn                         string
	WorkerInstanceProfileName             string
}

// IAMParams holds the parameters needed for CreateIAM.
type IAMParams struct {
	InfraID  string
	Region   string
	S3Bucket string
}

// roleSpec defines a single IAM role to create.
type roleSpec struct {
	suffix     string
	policyName string
	policy     map[string]any
}

// oidcIssuerURL returns the S3-backed OIDC issuer URL for the given infra.
func oidcIssuerURL(s3Bucket, region, infraID string) string {
	return fmt.Sprintf("%s.s3.%s.amazonaws.com/%s", s3Bucket, region, infraID)
}

// trustPolicy builds an IAM trust policy that allows the OIDC provider to
// assume the role for the given service account.
func trustPolicy(oidcProviderArn, oidcIssuer, namespace, saName string) string {
	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect": "Allow",
				"Principal": map[string]any{
					"Federated": oidcProviderArn,
				},
				"Action": "sts:AssumeRoleWithWebIdentity",
				"Condition": map[string]any{
					"StringEquals": map[string]string{
						oidcIssuer + ":sub": "system:serviceaccount:" + namespace + ":" + saName,
					},
				},
			},
		},
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

// inlinePolicy marshals a policy document to JSON.
func inlinePolicy(doc map[string]any) string {
	b, _ := json.Marshal(doc)
	return string(b)
}

// roleDefinitions returns the 8 IAM role definitions for an HCP cluster.
func roleDefinitions() []roleSpec {
	return []roleSpec{
		{
			suffix:     "cloud-controller",
			policyName: "cloud-controller-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"ec2:*", "elasticloadbalancing:*"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "node-pool",
			policyName: "node-pool-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"ec2:*"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "control-plane-operator",
			policyName: "control-plane-operator-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"ec2:*", "route53:*", "s3:*"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "cloud-network-config-controller",
			policyName: "cloud-network-config-controller-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"ec2:*"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "openshift-ingress",
			policyName: "openshift-ingress-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"elasticloadbalancing:*", "route53:*", "tag:GetResources"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "openshift-image-registry",
			policyName: "openshift-image-registry-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"s3:*"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "aws-ebs-csi-driver-controller",
			policyName: "aws-ebs-csi-driver-controller-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"ec2:CreateVolume", "ec2:DeleteVolume", "ec2:AttachVolume", "ec2:DetachVolume", "ec2:CreateSnapshot", "ec2:DeleteSnapshot", "ec2:CreateTags", "ec2:DeleteTags", "ec2:DescribeVolumes", "ec2:DescribeSnapshots", "ec2:DescribeInstances", "ec2:DescribeAvailabilityZones", "ec2:DescribeTags"},
						"Resource": "*",
					},
				},
			},
		},
		{
			suffix:     "worker-role",
			policyName: "worker-role-policy",
			policy: map[string]any{
				"Version": "2012-10-17",
				"Statement": []map[string]any{
					{
						"Effect":   "Allow",
						"Action":   []string{"ec2:DescribeInstances", "ec2:DescribeRegions"},
						"Resource": "*",
					},
				},
			},
		},
	}
}

// serviceAccountForRole maps role suffix to the namespace and SA name used
// in the OIDC trust policy.
var serviceAccountForRole = map[string][2]string{
	"cloud-controller":                {"kube-system", "aws-cloud-controller-manager"},
	"node-pool":                       {"kube-system", "capa-controller-manager"},
	"control-plane-operator":          {"kube-system", "control-plane-operator"},
	"cloud-network-config-controller": {"openshift-cloud-network-config-controller", "cloud-network-config-controller"},
	"openshift-ingress":               {"openshift-ingress-operator", "ingress-operator"},
	"openshift-image-registry":        {"openshift-image-registry", "cluster-image-registry-operator"},
	"aws-ebs-csi-driver-controller":   {"openshift-cluster-csi-drivers", "aws-ebs-csi-driver-controller-sa"},
	"worker-role":                     {"kube-system", "worker"},
}

// CreateIAM creates the OIDC provider, 8 IAM roles with trust policies and
// inline permissions, and a worker instance profile for an HCP cluster.
func CreateIAM(ctx context.Context, iamClient IAMAPI, params IAMParams) (*IAMOutput, error) {
	out := &IAMOutput{}

	oidcIssuer := oidcIssuerURL(params.S3Bucket, params.Region, params.InfraID)

	// 1. Create OIDC provider
	oidcOut, err := iamClient.CreateOpenIDConnectProvider(ctx, &iam.CreateOpenIDConnectProviderInput{
		Url:            aws.String("https://" + oidcIssuer),
		ClientIDList:   []string{"openshift", "sts.amazonaws.com"},
		ThumbprintList: []string{"A9D53002E97E00E043244F3D170D6F4C414104FD"}, // S3 root CA thumbprint
	})
	if err != nil {
		return out, fmt.Errorf("create OIDC provider: %w", err)
	}
	out.OIDCProviderArn = *oidcOut.OpenIDConnectProviderArn

	// 2. Create 8 IAM roles with trust policies and inline permissions
	roles := roleDefinitions()
	roleArnSetters := []func(string){
		func(arn string) { out.CloudControllerRoleArn = arn },
		func(arn string) { out.NodePoolRoleArn = arn },
		func(arn string) { out.ControlPlaneOperatorRoleArn = arn },
		func(arn string) { out.CloudNetworkConfigControllerRoleArn = arn },
		func(arn string) { out.IngressRoleArn = arn },
		func(arn string) { out.ImageRegistryRoleArn = arn },
		func(arn string) { out.EBSCSIDriverRoleArn = arn },
		func(arn string) { out.WorkerRoleArn = arn },
	}

	for i, role := range roles {
		roleName := params.InfraID + "-" + role.suffix
		sa := serviceAccountForRole[role.suffix]

		roleOut, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 aws.String(roleName),
			AssumeRolePolicyDocument: aws.String(trustPolicy(out.OIDCProviderArn, oidcIssuer, sa[0], sa[1])),
			Tags: []iamtypes.Tag{
				{Key: aws.String("kubernetes.io/cluster/" + params.InfraID), Value: aws.String("owned")},
			},
		})
		if err != nil {
			return out, fmt.Errorf("create role %s: %w", roleName, err)
		}
		roleArnSetters[i](*roleOut.Role.Arn)

		_, err = iamClient.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
			RoleName:       aws.String(roleName),
			PolicyName:     aws.String(role.policyName),
			PolicyDocument: aws.String(inlinePolicy(role.policy)),
		})
		if err != nil {
			return out, fmt.Errorf("put policy on role %s: %w", roleName, err)
		}
	}

	// 3. Create worker instance profile and add the worker role
	profileName := params.InfraID + "-worker"
	_, err = iamClient.CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		Tags: []iamtypes.Tag{
			{Key: aws.String("kubernetes.io/cluster/" + params.InfraID), Value: aws.String("owned")},
		},
	})
	if err != nil {
		return out, fmt.Errorf("create instance profile: %w", err)
	}
	out.WorkerInstanceProfileName = profileName

	_, err = iamClient.AddRoleToInstanceProfile(ctx, &iam.AddRoleToInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		RoleName:            aws.String(params.InfraID + "-worker-role"),
	})
	if err != nil {
		return out, fmt.Errorf("add worker role to instance profile: %w", err)
	}

	return out, nil
}

// DestroyIAM removes all IAM resources created for an HCP cluster:
// instance profile, 8 roles (with their inline policies), and OIDC provider.
func DestroyIAM(ctx context.Context, iamClient IAMAPI, infraID string, out *IAMOutput) error {
	profileName := infraID + "-worker"

	// 1. Remove worker role from instance profile
	if _, err := iamClient.RemoveRoleFromInstanceProfile(ctx, &iam.RemoveRoleFromInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
		RoleName:            aws.String(infraID + "-worker-role"),
	}); err != nil {
		return fmt.Errorf("remove role from instance profile: %w", err)
	}

	// 2. Delete instance profile
	if _, err := iamClient.DeleteInstanceProfile(ctx, &iam.DeleteInstanceProfileInput{
		InstanceProfileName: aws.String(profileName),
	}); err != nil {
		return fmt.Errorf("delete instance profile: %w", err)
	}

	// 3. Delete all 8 roles (delete inline policies first, then delete role)
	for _, role := range roleDefinitions() {
		roleName := infraID + "-" + role.suffix

		// List and delete all inline policies
		policiesOut, err := iamClient.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{
			RoleName: aws.String(roleName),
		})
		if err != nil {
			return fmt.Errorf("list policies for role %s: %w", roleName, err)
		}
		for _, policyName := range policiesOut.PolicyNames {
			if _, err := iamClient.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{
				RoleName:   aws.String(roleName),
				PolicyName: aws.String(policyName),
			}); err != nil {
				return fmt.Errorf("delete policy %s from role %s: %w", policyName, roleName, err)
			}
		}

		// Delete the role
		if _, err := iamClient.DeleteRole(ctx, &iam.DeleteRoleInput{
			RoleName: aws.String(roleName),
		}); err != nil {
			return fmt.Errorf("delete role %s: %w", roleName, err)
		}
	}

	// 4. Delete OIDC provider
	if out != nil && out.OIDCProviderArn != "" {
		if _, err := iamClient.DeleteOpenIDConnectProvider(ctx, &iam.DeleteOpenIDConnectProviderInput{
			OpenIDConnectProviderArn: aws.String(out.OIDCProviderArn),
		}); err != nil {
			return fmt.Errorf("delete OIDC provider: %w", err)
		}
	}

	return nil
}
