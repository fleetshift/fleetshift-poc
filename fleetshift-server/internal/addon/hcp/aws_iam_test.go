package hcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// mockIAM implements IAMAPI for testing.
type mockIAM struct {
	rolesCreated           []string
	policiesPut            []string
	instanceProfileCreated string
	roleAddedToProfile     string
	oidcProviderURL        string
	calls                  []string
}

func (m *mockIAM) CreateOpenIDConnectProvider(ctx context.Context, input *iam.CreateOpenIDConnectProviderInput, optFns ...func(*iam.Options)) (*iam.CreateOpenIDConnectProviderOutput, error) {
	m.calls = append(m.calls, "CreateOpenIDConnectProvider")
	m.oidcProviderURL = *input.Url
	return &iam.CreateOpenIDConnectProviderOutput{
		OpenIDConnectProviderArn: aws.String("arn:aws:iam::123456789012:oidc-provider/" + *input.Url),
	}, nil
}

func (m *mockIAM) DeleteOpenIDConnectProvider(ctx context.Context, input *iam.DeleteOpenIDConnectProviderInput, optFns ...func(*iam.Options)) (*iam.DeleteOpenIDConnectProviderOutput, error) {
	m.calls = append(m.calls, "DeleteOpenIDConnectProvider")
	return &iam.DeleteOpenIDConnectProviderOutput{}, nil
}

func (m *mockIAM) CreateRole(ctx context.Context, input *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	m.calls = append(m.calls, "CreateRole:"+*input.RoleName)
	m.rolesCreated = append(m.rolesCreated, *input.RoleName)
	return &iam.CreateRoleOutput{
		Role: &iamtypes.Role{
			Arn:      aws.String(fmt.Sprintf("arn:aws:iam::123456789012:role/%s", *input.RoleName)),
			RoleName: input.RoleName,
		},
	}, nil
}

func (m *mockIAM) DeleteRole(ctx context.Context, input *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	m.calls = append(m.calls, "DeleteRole:"+*input.RoleName)
	return &iam.DeleteRoleOutput{}, nil
}

func (m *mockIAM) PutRolePolicy(ctx context.Context, input *iam.PutRolePolicyInput, optFns ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	m.calls = append(m.calls, "PutRolePolicy:"+*input.RoleName)
	m.policiesPut = append(m.policiesPut, *input.RoleName+"/"+*input.PolicyName)
	return &iam.PutRolePolicyOutput{}, nil
}

func (m *mockIAM) DeleteRolePolicy(ctx context.Context, input *iam.DeleteRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	m.calls = append(m.calls, "DeleteRolePolicy:"+*input.RoleName)
	return &iam.DeleteRolePolicyOutput{}, nil
}

func (m *mockIAM) CreateInstanceProfile(ctx context.Context, input *iam.CreateInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.CreateInstanceProfileOutput, error) {
	m.calls = append(m.calls, "CreateInstanceProfile:"+*input.InstanceProfileName)
	m.instanceProfileCreated = *input.InstanceProfileName
	return &iam.CreateInstanceProfileOutput{
		InstanceProfile: &iamtypes.InstanceProfile{
			InstanceProfileName: input.InstanceProfileName,
			Arn:                 aws.String(fmt.Sprintf("arn:aws:iam::123456789012:instance-profile/%s", *input.InstanceProfileName)),
		},
	}, nil
}

func (m *mockIAM) DeleteInstanceProfile(ctx context.Context, input *iam.DeleteInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.DeleteInstanceProfileOutput, error) {
	m.calls = append(m.calls, "DeleteInstanceProfile")
	return &iam.DeleteInstanceProfileOutput{}, nil
}

func (m *mockIAM) AddRoleToInstanceProfile(ctx context.Context, input *iam.AddRoleToInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.AddRoleToInstanceProfileOutput, error) {
	m.calls = append(m.calls, "AddRoleToInstanceProfile")
	m.roleAddedToProfile = *input.RoleName
	return &iam.AddRoleToInstanceProfileOutput{}, nil
}

func (m *mockIAM) RemoveRoleFromInstanceProfile(ctx context.Context, input *iam.RemoveRoleFromInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.RemoveRoleFromInstanceProfileOutput, error) {
	m.calls = append(m.calls, "RemoveRoleFromInstanceProfile")
	return &iam.RemoveRoleFromInstanceProfileOutput{}, nil
}

func (m *mockIAM) ListRolePolicies(ctx context.Context, input *iam.ListRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error) {
	m.calls = append(m.calls, "ListRolePolicies:"+*input.RoleName)
	// Return the policy that was put for this role (lookup from policiesPut)
	var policyNames []string
	for _, p := range m.policiesPut {
		parts := strings.SplitN(p, "/", 2)
		if parts[0] == *input.RoleName && len(parts) == 2 {
			policyNames = append(policyNames, parts[1])
		}
	}
	return &iam.ListRolePoliciesOutput{PolicyNames: policyNames}, nil
}

func (m *mockIAM) ListInstanceProfilesForRole(ctx context.Context, input *iam.ListInstanceProfilesForRoleInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error) {
	return &iam.ListInstanceProfilesForRoleOutput{}, nil
}

func (m *mockIAM) ListOpenIDConnectProviders(ctx context.Context, input *iam.ListOpenIDConnectProvidersInput, optFns ...func(*iam.Options)) (*iam.ListOpenIDConnectProvidersOutput, error) {
	m.calls = append(m.calls, "ListOpenIDConnectProviders")
	// Return the OIDC provider that was created, if any.
	var providers []iamtypes.OpenIDConnectProviderListEntry
	if m.oidcProviderURL != "" {
		arn := "arn:aws:iam::123456789012:oidc-provider/" + m.oidcProviderURL
		providers = append(providers, iamtypes.OpenIDConnectProviderListEntry{
			Arn: aws.String(arn),
		})
	}
	return &iam.ListOpenIDConnectProvidersOutput{
		OpenIDConnectProviderList: providers,
	}, nil
}

func TestCreateIAM_OIDCProviderCreated(t *testing.T) {
	mock := &mockIAM{}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	out, err := CreateIAM(context.Background(), mock, params)
	if err != nil {
		t.Fatalf("CreateIAM returned error: %v", err)
	}

	// Verify OIDC provider was created
	if mock.oidcProviderURL == "" {
		t.Fatal("OIDC provider was not created")
	}
	expectedURL := "https://my-oidc-bucket.s3.us-east-1.amazonaws.com/test-infra"
	if mock.oidcProviderURL != expectedURL {
		t.Errorf("OIDC provider URL = %q, want %q", mock.oidcProviderURL, expectedURL)
	}
	if out.OIDCProviderArn == "" {
		t.Error("OIDCProviderArn is empty")
	}
}

func TestCreateIAM_AllRolesCreated(t *testing.T) {
	mock := &mockIAM{}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	out, err := CreateIAM(context.Background(), mock, params)
	if err != nil {
		t.Fatalf("CreateIAM returned error: %v", err)
	}

	expectedRoles := []string{
		"test-infra-cloud-controller",
		"test-infra-node-pool",
		"test-infra-control-plane-operator",
		"test-infra-cloud-network-config-controller",
		"test-infra-openshift-ingress",
		"test-infra-openshift-image-registry",
		"test-infra-aws-ebs-csi-driver-controller",
		"test-infra-worker-role",
	}

	if len(mock.rolesCreated) != 8 {
		t.Fatalf("expected 8 roles, got %d: %v", len(mock.rolesCreated), mock.rolesCreated)
	}

	for i, expected := range expectedRoles {
		if mock.rolesCreated[i] != expected {
			t.Errorf("role[%d] = %q, want %q", i, mock.rolesCreated[i], expected)
		}
	}

	// Verify all ARNs are set in output
	arns := map[string]string{
		"CloudControllerRoleArn":              out.CloudControllerRoleArn,
		"NodePoolRoleArn":                     out.NodePoolRoleArn,
		"ControlPlaneOperatorRoleArn":         out.ControlPlaneOperatorRoleArn,
		"CloudNetworkConfigControllerRoleArn": out.CloudNetworkConfigControllerRoleArn,
		"IngressRoleArn":                      out.IngressRoleArn,
		"ImageRegistryRoleArn":                out.ImageRegistryRoleArn,
		"EBSCSIDriverRoleArn":                 out.EBSCSIDriverRoleArn,
		"WorkerRoleArn":                       out.WorkerRoleArn,
	}
	for name, arn := range arns {
		if arn == "" {
			t.Errorf("%s is empty", name)
		}
	}
}

func TestCreateIAM_RolePoliciesAttached(t *testing.T) {
	mock := &mockIAM{}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	_, err := CreateIAM(context.Background(), mock, params)
	if err != nil {
		t.Fatalf("CreateIAM returned error: %v", err)
	}

	if len(mock.policiesPut) != 8 {
		t.Fatalf("expected 8 policies, got %d: %v", len(mock.policiesPut), mock.policiesPut)
	}

	// Each role should have a policy attached
	for _, p := range mock.policiesPut {
		if !strings.Contains(p, "test-infra-") {
			t.Errorf("policy %q does not contain infra ID prefix", p)
		}
	}
}

func TestCreateIAM_InstanceProfileCreated(t *testing.T) {
	mock := &mockIAM{}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	out, err := CreateIAM(context.Background(), mock, params)
	if err != nil {
		t.Fatalf("CreateIAM returned error: %v", err)
	}

	if mock.instanceProfileCreated != "test-infra-worker" {
		t.Errorf("instance profile = %q, want %q", mock.instanceProfileCreated, "test-infra-worker")
	}
	if mock.roleAddedToProfile != "test-infra-worker-role" {
		t.Errorf("role added to profile = %q, want %q", mock.roleAddedToProfile, "test-infra-worker-role")
	}
	if out.WorkerInstanceProfileName != "test-infra-worker" {
		t.Errorf("WorkerInstanceProfileName = %q, want %q", out.WorkerInstanceProfileName, "test-infra-worker")
	}
}

func TestCreateIAM_TrustPolicyReferencesOIDC(t *testing.T) {
	// Verify trust policy structure
	oidcArn := "arn:aws:iam::123456789012:oidc-provider/test"
	issuer := "my-bucket.s3.us-east-1.amazonaws.com/test-infra"
	tp := trustPolicy(oidcArn, issuer, "kube-system", "test-sa")

	var doc map[string]any
	if err := json.Unmarshal([]byte(tp), &doc); err != nil {
		t.Fatalf("trust policy is not valid JSON: %v", err)
	}

	stmts, ok := doc["Statement"].([]any)
	if !ok || len(stmts) != 1 {
		t.Fatal("expected exactly 1 statement in trust policy")
	}

	stmt := stmts[0].(map[string]any)
	principal := stmt["Principal"].(map[string]any)
	if principal["Federated"] != oidcArn {
		t.Errorf("Federated principal = %v, want %v", principal["Federated"], oidcArn)
	}

	condition := stmt["Condition"].(map[string]any)
	strEquals := condition["StringEquals"].(map[string]any)
	expectedKey := issuer + ":sub"
	if strEquals[expectedKey] != "system:serviceaccount:kube-system:test-sa" {
		t.Errorf("condition %q = %v, want system:serviceaccount:kube-system:test-sa", expectedKey, strEquals[expectedKey])
	}
}

func TestDestroyIAM_RemovesAllRolesAndOIDC(t *testing.T) {
	mock := &mockIAM{}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	// First create IAM resources so the mock has policy data
	out, err := CreateIAM(context.Background(), mock, params)
	if err != nil {
		t.Fatalf("CreateIAM failed: %v", err)
	}

	// Reset call tracking
	mock.calls = nil

	// Now destroy
	err = DestroyIAM(context.Background(), mock, "test-infra", out)
	if err != nil {
		t.Fatalf("DestroyIAM failed: %v", err)
	}

	// Verify instance profile cleanup comes first
	if len(mock.calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(mock.calls))
	}
	if mock.calls[0] != "RemoveRoleFromInstanceProfile" {
		t.Errorf("first call = %q, want RemoveRoleFromInstanceProfile", mock.calls[0])
	}
	if mock.calls[1] != "DeleteInstanceProfile" {
		t.Errorf("second call = %q, want DeleteInstanceProfile", mock.calls[1])
	}

	// Count role deletions
	deleteRoleCount := 0
	deletePolicyCount := 0
	for _, call := range mock.calls {
		if strings.HasPrefix(call, "DeleteRole:") {
			deleteRoleCount++
		}
		if strings.HasPrefix(call, "DeleteRolePolicy:") {
			deletePolicyCount++
		}
	}
	if deleteRoleCount != 8 {
		t.Errorf("expected 8 DeleteRole calls, got %d", deleteRoleCount)
	}
	if deletePolicyCount != 8 {
		t.Errorf("expected 8 DeleteRolePolicy calls, got %d", deletePolicyCount)
	}

	// Verify OIDC provider deleted last
	lastCall := mock.calls[len(mock.calls)-1]
	if lastCall != "DeleteOpenIDConnectProvider" {
		t.Errorf("last call = %q, want DeleteOpenIDConnectProvider", lastCall)
	}
}

func TestDestroyIAM_PolicyDeletedBeforeRole(t *testing.T) {
	mock := &mockIAM{}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	out, err := CreateIAM(context.Background(), mock, params)
	if err != nil {
		t.Fatalf("CreateIAM failed: %v", err)
	}
	mock.calls = nil

	err = DestroyIAM(context.Background(), mock, "test-infra", out)
	if err != nil {
		t.Fatalf("DestroyIAM failed: %v", err)
	}

	// For each role, DeleteRolePolicy must come before DeleteRole
	for _, role := range roleDefinitions() {
		roleName := "test-infra-" + role.suffix
		policyIdx := -1
		roleIdx := -1
		for i, call := range mock.calls {
			if call == "DeleteRolePolicy:"+roleName {
				policyIdx = i
			}
			if call == "DeleteRole:"+roleName {
				roleIdx = i
			}
		}
		if policyIdx < 0 {
			t.Errorf("DeleteRolePolicy not called for %s", roleName)
			continue
		}
		if roleIdx < 0 {
			t.Errorf("DeleteRole not called for %s", roleName)
			continue
		}
		if policyIdx > roleIdx {
			t.Errorf("DeleteRolePolicy (idx=%d) should come before DeleteRole (idx=%d) for %s", policyIdx, roleIdx, roleName)
		}
	}
}

func TestDestroyIAM_ErrorReturned(t *testing.T) {
	mock := &mockIAMWithError{
		mockIAM:  &mockIAM{},
		failOn:   "DeleteRole",
		failErr:  fmt.Errorf("role in use"),
	}
	params := IAMParams{
		InfraID:  "test-infra",
		Region:   "us-east-1",
		S3Bucket: "my-oidc-bucket",
	}

	// Create first so mock has policy data
	out, err := CreateIAM(context.Background(), mock.mockIAM, params)
	if err != nil {
		t.Fatalf("CreateIAM failed: %v", err)
	}

	err = DestroyIAM(context.Background(), mock, "test-infra", out)
	if err == nil {
		t.Fatal("expected error from DestroyIAM, got nil")
	}
	if !strings.Contains(err.Error(), "role in use") {
		t.Errorf("error = %q, want it to contain 'role in use'", err.Error())
	}
}

// mockIAMWithError wraps mockIAM and returns an error for a specific operation.
type mockIAMWithError struct {
	*mockIAM
	failOn  string
	failErr error
}

func (m *mockIAMWithError) DeleteRole(ctx context.Context, input *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	if m.failOn == "DeleteRole" {
		return nil, m.failErr
	}
	return m.mockIAM.DeleteRole(ctx, input, optFns...)
}

func (m *mockIAMWithError) ListRolePolicies(ctx context.Context, input *iam.ListRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error) {
	return m.mockIAM.ListRolePolicies(ctx, input, optFns...)
}
