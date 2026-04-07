package hcp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
)

// mockEC2 implements EC2API for testing.
type mockEC2 struct {
	vpcCount    int
	subnetCount int
	eipCount    int
	natCount    int
	rtCount     int
	epCount     int

	createVpcCalls              []ec2.CreateVpcInput
	modifyVpcAttrCalls          []ec2.ModifyVpcAttributeInput
	createDhcpCalls             []ec2.CreateDhcpOptionsInput
	assocDhcpCalls              []ec2.AssociateDhcpOptionsInput
	createIGWCalls              []ec2.CreateInternetGatewayInput
	attachIGWCalls              []ec2.AttachInternetGatewayInput
	createSubnetCalls           []ec2.CreateSubnetInput
	allocateAddrCalls           []ec2.AllocateAddressInput
	createNatCalls              []ec2.CreateNatGatewayInput
	createRTCalls               []ec2.CreateRouteTableInput
	createRouteCalls            []ec2.CreateRouteInput
	assocRTCalls                []ec2.AssociateRouteTableInput
	createVpcEndpointCalls      []ec2.CreateVpcEndpointInput
	createTagsCalls             []ec2.CreateTagsInput

	// Destroy tracking
	operations                  []string
	describeRTResult            *ec2.DescribeRouteTablesOutput
	describeNatResult           *ec2.DescribeNatGatewaysOutput
	deleteErr                   error // if set, next delete call returns this error
	deleteErrOn                 string // operation name to fail on
}

func (m *mockEC2) CreateVpc(_ context.Context, in *ec2.CreateVpcInput, _ ...func(*ec2.Options)) (*ec2.CreateVpcOutput, error) {
	m.createVpcCalls = append(m.createVpcCalls, *in)
	m.vpcCount++
	return &ec2.CreateVpcOutput{
		Vpc: &ec2types.Vpc{
			VpcId:     aws.String(fmt.Sprintf("vpc-%04d", m.vpcCount)),
			CidrBlock: in.CidrBlock,
		},
	}, nil
}

func (m *mockEC2) ModifyVpcAttribute(_ context.Context, in *ec2.ModifyVpcAttributeInput, _ ...func(*ec2.Options)) (*ec2.ModifyVpcAttributeOutput, error) {
	m.modifyVpcAttrCalls = append(m.modifyVpcAttrCalls, *in)
	return &ec2.ModifyVpcAttributeOutput{}, nil
}

func (m *mockEC2) CreateDhcpOptions(_ context.Context, in *ec2.CreateDhcpOptionsInput, _ ...func(*ec2.Options)) (*ec2.CreateDhcpOptionsOutput, error) {
	m.createDhcpCalls = append(m.createDhcpCalls, *in)
	return &ec2.CreateDhcpOptionsOutput{
		DhcpOptions: &ec2types.DhcpOptions{
			DhcpOptionsId: aws.String("dopt-0001"),
		},
	}, nil
}

func (m *mockEC2) AssociateDhcpOptions(_ context.Context, in *ec2.AssociateDhcpOptionsInput, _ ...func(*ec2.Options)) (*ec2.AssociateDhcpOptionsOutput, error) {
	m.assocDhcpCalls = append(m.assocDhcpCalls, *in)
	return &ec2.AssociateDhcpOptionsOutput{}, nil
}

func (m *mockEC2) CreateInternetGateway(_ context.Context, in *ec2.CreateInternetGatewayInput, _ ...func(*ec2.Options)) (*ec2.CreateInternetGatewayOutput, error) {
	m.createIGWCalls = append(m.createIGWCalls, *in)
	return &ec2.CreateInternetGatewayOutput{
		InternetGateway: &ec2types.InternetGateway{
			InternetGatewayId: aws.String("igw-0001"),
		},
	}, nil
}

func (m *mockEC2) AttachInternetGateway(_ context.Context, in *ec2.AttachInternetGatewayInput, _ ...func(*ec2.Options)) (*ec2.AttachInternetGatewayOutput, error) {
	m.attachIGWCalls = append(m.attachIGWCalls, *in)
	return &ec2.AttachInternetGatewayOutput{}, nil
}

func (m *mockEC2) CreateSubnet(_ context.Context, in *ec2.CreateSubnetInput, _ ...func(*ec2.Options)) (*ec2.CreateSubnetOutput, error) {
	m.createSubnetCalls = append(m.createSubnetCalls, *in)
	m.subnetCount++
	return &ec2.CreateSubnetOutput{
		Subnet: &ec2types.Subnet{
			SubnetId:         aws.String(fmt.Sprintf("subnet-%04d", m.subnetCount)),
			AvailabilityZone: in.AvailabilityZone,
			CidrBlock:        in.CidrBlock,
		},
	}, nil
}

func (m *mockEC2) AllocateAddress(_ context.Context, in *ec2.AllocateAddressInput, _ ...func(*ec2.Options)) (*ec2.AllocateAddressOutput, error) {
	m.allocateAddrCalls = append(m.allocateAddrCalls, *in)
	m.eipCount++
	return &ec2.AllocateAddressOutput{
		AllocationId: aws.String(fmt.Sprintf("eipalloc-%04d", m.eipCount)),
	}, nil
}

func (m *mockEC2) CreateNatGateway(_ context.Context, in *ec2.CreateNatGatewayInput, _ ...func(*ec2.Options)) (*ec2.CreateNatGatewayOutput, error) {
	m.createNatCalls = append(m.createNatCalls, *in)
	m.natCount++
	return &ec2.CreateNatGatewayOutput{
		NatGateway: &ec2types.NatGateway{
			NatGatewayId: aws.String(fmt.Sprintf("nat-%04d", m.natCount)),
		},
	}, nil
}

func (m *mockEC2) CreateRouteTable(_ context.Context, in *ec2.CreateRouteTableInput, _ ...func(*ec2.Options)) (*ec2.CreateRouteTableOutput, error) {
	m.createRTCalls = append(m.createRTCalls, *in)
	m.rtCount++
	return &ec2.CreateRouteTableOutput{
		RouteTable: &ec2types.RouteTable{
			RouteTableId: aws.String(fmt.Sprintf("rtb-%04d", m.rtCount)),
		},
	}, nil
}

func (m *mockEC2) CreateRoute(_ context.Context, in *ec2.CreateRouteInput, _ ...func(*ec2.Options)) (*ec2.CreateRouteOutput, error) {
	m.createRouteCalls = append(m.createRouteCalls, *in)
	return &ec2.CreateRouteOutput{Return: aws.Bool(true)}, nil
}

func (m *mockEC2) AssociateRouteTable(_ context.Context, in *ec2.AssociateRouteTableInput, _ ...func(*ec2.Options)) (*ec2.AssociateRouteTableOutput, error) {
	m.assocRTCalls = append(m.assocRTCalls, *in)
	return &ec2.AssociateRouteTableOutput{
		AssociationId: aws.String("rtbassoc-0001"),
	}, nil
}

func (m *mockEC2) CreateVpcEndpoint(_ context.Context, in *ec2.CreateVpcEndpointInput, _ ...func(*ec2.Options)) (*ec2.CreateVpcEndpointOutput, error) {
	m.createVpcEndpointCalls = append(m.createVpcEndpointCalls, *in)
	m.epCount++
	return &ec2.CreateVpcEndpointOutput{
		VpcEndpoint: &ec2types.VpcEndpoint{
			VpcEndpointId: aws.String(fmt.Sprintf("vpce-%04d", m.epCount)),
		},
	}, nil
}

func (m *mockEC2) CreateTags(_ context.Context, in *ec2.CreateTagsInput, _ ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	m.createTagsCalls = append(m.createTagsCalls, *in)
	return &ec2.CreateTagsOutput{}, nil
}

// checkErr returns deleteErr if the operation matches deleteErrOn.
func (m *mockEC2) checkErr(op string) error {
	if m.deleteErr != nil && m.deleteErrOn == op {
		return m.deleteErr
	}
	return nil
}

func (m *mockEC2) DeleteVpc(_ context.Context, in *ec2.DeleteVpcInput, _ ...func(*ec2.Options)) (*ec2.DeleteVpcOutput, error) {
	m.operations = append(m.operations, "DeleteVpc")
	if err := m.checkErr("DeleteVpc"); err != nil {
		return nil, err
	}
	return &ec2.DeleteVpcOutput{}, nil
}
func (m *mockEC2) DeleteSubnet(_ context.Context, in *ec2.DeleteSubnetInput, _ ...func(*ec2.Options)) (*ec2.DeleteSubnetOutput, error) {
	m.operations = append(m.operations, "DeleteSubnet:"+aws.ToString(in.SubnetId))
	if err := m.checkErr("DeleteSubnet"); err != nil {
		return nil, err
	}
	return &ec2.DeleteSubnetOutput{}, nil
}
func (m *mockEC2) DeleteInternetGateway(_ context.Context, _ *ec2.DeleteInternetGatewayInput, _ ...func(*ec2.Options)) (*ec2.DeleteInternetGatewayOutput, error) {
	m.operations = append(m.operations, "DeleteInternetGateway")
	if err := m.checkErr("DeleteInternetGateway"); err != nil {
		return nil, err
	}
	return &ec2.DeleteInternetGatewayOutput{}, nil
}
func (m *mockEC2) DetachInternetGateway(_ context.Context, _ *ec2.DetachInternetGatewayInput, _ ...func(*ec2.Options)) (*ec2.DetachInternetGatewayOutput, error) {
	m.operations = append(m.operations, "DetachInternetGateway")
	if err := m.checkErr("DetachInternetGateway"); err != nil {
		return nil, err
	}
	return &ec2.DetachInternetGatewayOutput{}, nil
}
func (m *mockEC2) DeleteNatGateway(_ context.Context, _ *ec2.DeleteNatGatewayInput, _ ...func(*ec2.Options)) (*ec2.DeleteNatGatewayOutput, error) {
	m.operations = append(m.operations, "DeleteNatGateway")
	if err := m.checkErr("DeleteNatGateway"); err != nil {
		return nil, err
	}
	return &ec2.DeleteNatGatewayOutput{}, nil
}
func (m *mockEC2) ReleaseAddress(_ context.Context, _ *ec2.ReleaseAddressInput, _ ...func(*ec2.Options)) (*ec2.ReleaseAddressOutput, error) {
	m.operations = append(m.operations, "ReleaseAddress")
	if err := m.checkErr("ReleaseAddress"); err != nil {
		return nil, err
	}
	return &ec2.ReleaseAddressOutput{}, nil
}
func (m *mockEC2) DeleteRouteTable(_ context.Context, in *ec2.DeleteRouteTableInput, _ ...func(*ec2.Options)) (*ec2.DeleteRouteTableOutput, error) {
	m.operations = append(m.operations, "DeleteRouteTable:"+aws.ToString(in.RouteTableId))
	if err := m.checkErr("DeleteRouteTable"); err != nil {
		return nil, err
	}
	return &ec2.DeleteRouteTableOutput{}, nil
}
func (m *mockEC2) DeleteVpcEndpoints(_ context.Context, _ *ec2.DeleteVpcEndpointsInput, _ ...func(*ec2.Options)) (*ec2.DeleteVpcEndpointsOutput, error) {
	m.operations = append(m.operations, "DeleteVpcEndpoints")
	if err := m.checkErr("DeleteVpcEndpoints"); err != nil {
		return nil, err
	}
	return &ec2.DeleteVpcEndpointsOutput{}, nil
}
func (m *mockEC2) DeleteDhcpOptions(_ context.Context, _ *ec2.DeleteDhcpOptionsInput, _ ...func(*ec2.Options)) (*ec2.DeleteDhcpOptionsOutput, error) {
	m.operations = append(m.operations, "DeleteDhcpOptions")
	if err := m.checkErr("DeleteDhcpOptions"); err != nil {
		return nil, err
	}
	return &ec2.DeleteDhcpOptionsOutput{}, nil
}
func (m *mockEC2) DescribeVpcs(context.Context, *ec2.DescribeVpcsInput, ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	return &ec2.DescribeVpcsOutput{}, nil
}
func (m *mockEC2) DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	return &ec2.DescribeSubnetsOutput{}, nil
}
func (m *mockEC2) DescribeNatGateways(_ context.Context, _ *ec2.DescribeNatGatewaysInput, _ ...func(*ec2.Options)) (*ec2.DescribeNatGatewaysOutput, error) {
	m.operations = append(m.operations, "DescribeNatGateways")
	if m.describeNatResult != nil {
		return m.describeNatResult, nil
	}
	// Default: return deleted state so the wait loop exits
	return &ec2.DescribeNatGatewaysOutput{
		NatGateways: []ec2types.NatGateway{{State: ec2types.NatGatewayStateDeleted}},
	}, nil
}
func (m *mockEC2) DescribeRouteTables(_ context.Context, _ *ec2.DescribeRouteTablesInput, _ ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error) {
	m.operations = append(m.operations, "DescribeRouteTables")
	if m.describeRTResult != nil {
		return m.describeRTResult, nil
	}
	// Default: return a main association so ReplaceRouteTableAssociation works.
	return &ec2.DescribeRouteTablesOutput{
		RouteTables: []ec2types.RouteTable{{
			RouteTableId: aws.String("rtb-main"),
			Associations: []ec2types.RouteTableAssociation{{
				Main:                    aws.Bool(true),
				RouteTableAssociationId: aws.String("rtbassoc-main"),
			}},
		}},
	}, nil
}
func (m *mockEC2) DescribeInternetGateways(_ context.Context, _ *ec2.DescribeInternetGatewaysInput, _ ...func(*ec2.Options)) (*ec2.DescribeInternetGatewaysOutput, error) {
	return &ec2.DescribeInternetGatewaysOutput{}, nil
}
func (m *mockEC2) DescribeVpcEndpoints(_ context.Context, _ *ec2.DescribeVpcEndpointsInput, _ ...func(*ec2.Options)) (*ec2.DescribeVpcEndpointsOutput, error) {
	return &ec2.DescribeVpcEndpointsOutput{}, nil
}
func (m *mockEC2) DescribeAddresses(_ context.Context, _ *ec2.DescribeAddressesInput, _ ...func(*ec2.Options)) (*ec2.DescribeAddressesOutput, error) {
	return &ec2.DescribeAddressesOutput{}, nil
}
func (m *mockEC2) DisassociateRouteTable(_ context.Context, in *ec2.DisassociateRouteTableInput, _ ...func(*ec2.Options)) (*ec2.DisassociateRouteTableOutput, error) {
	m.operations = append(m.operations, "DisassociateRouteTable:"+aws.ToString(in.AssociationId))
	return &ec2.DisassociateRouteTableOutput{}, nil
}

func (m *mockEC2) ReplaceRouteTableAssociation(_ context.Context, _ *ec2.ReplaceRouteTableAssociationInput, _ ...func(*ec2.Options)) (*ec2.ReplaceRouteTableAssociationOutput, error) {
	m.operations = append(m.operations, "ReplaceRouteTableAssociation")
	return &ec2.ReplaceRouteTableAssociationOutput{}, nil
}

// mockRoute53 implements Route53API for testing.
type mockRoute53 struct {
	zoneCount              int
	createZoneCalls        []route53.CreateHostedZoneInput
	operations             []string
	listRecordSetsResult   *route53.ListResourceRecordSetsOutput
	deleteErr              error
	deleteErrOn            string
}

func (m *mockRoute53) CreateHostedZone(_ context.Context, in *route53.CreateHostedZoneInput, _ ...func(*route53.Options)) (*route53.CreateHostedZoneOutput, error) {
	m.createZoneCalls = append(m.createZoneCalls, *in)
	m.zoneCount++
	return &route53.CreateHostedZoneOutput{
		HostedZone: &r53types.HostedZone{
			Id:   aws.String(fmt.Sprintf("/hostedzone/Z%04d", m.zoneCount)),
			Name: in.Name,
		},
	}, nil
}

func (m *mockRoute53) DeleteHostedZone(_ context.Context, in *route53.DeleteHostedZoneInput, _ ...func(*route53.Options)) (*route53.DeleteHostedZoneOutput, error) {
	m.operations = append(m.operations, "DeleteHostedZone:"+aws.ToString(in.Id))
	if m.deleteErr != nil && m.deleteErrOn == "DeleteHostedZone" {
		return nil, m.deleteErr
	}
	return &route53.DeleteHostedZoneOutput{}, nil
}

func (m *mockRoute53) ListHostedZonesByName(context.Context, *route53.ListHostedZonesByNameInput, ...func(*route53.Options)) (*route53.ListHostedZonesByNameOutput, error) {
	return &route53.ListHostedZonesByNameOutput{}, nil
}

func (m *mockRoute53) ListResourceRecordSets(_ context.Context, _ *route53.ListResourceRecordSetsInput, _ ...func(*route53.Options)) (*route53.ListResourceRecordSetsOutput, error) {
	m.operations = append(m.operations, "ListResourceRecordSets")
	if m.listRecordSetsResult != nil {
		return m.listRecordSetsResult, nil
	}
	return &route53.ListResourceRecordSetsOutput{}, nil
}

func (m *mockRoute53) ChangeResourceRecordSets(_ context.Context, _ *route53.ChangeResourceRecordSetsInput, _ ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error) {
	m.operations = append(m.operations, "ChangeResourceRecordSets")
	return &route53.ChangeResourceRecordSetsOutput{}, nil
}

func TestCreateInfra_SingleZone(t *testing.T) {
	ec2Mock := &mockEC2{}
	r53Mock := &mockRoute53{}

	spec := InfraSpec{
		Name:       "test-cluster",
		InfraID:    "test-infra-123",
		Region:     "us-east-1",
		BaseDomain: "example.com",
		Zones:      []string{"us-east-1a"},
	}

	out, err := CreateInfra(context.Background(), ec2Mock, r53Mock, spec)
	if err != nil {
		t.Fatalf("CreateInfra failed: %v", err)
	}

	// VPC created
	if len(ec2Mock.createVpcCalls) != 1 {
		t.Fatalf("expected 1 CreateVpc call, got %d", len(ec2Mock.createVpcCalls))
	}
	if aws.ToString(ec2Mock.createVpcCalls[0].CidrBlock) != "10.0.0.0/16" {
		t.Errorf("VPC CIDR = %q, want 10.0.0.0/16", aws.ToString(ec2Mock.createVpcCalls[0].CidrBlock))
	}
	if out.VPCID != "vpc-0001" {
		t.Errorf("VPCID = %q, want vpc-0001", out.VPCID)
	}

	// VPC attributes modified (DNS support + hostnames)
	if len(ec2Mock.modifyVpcAttrCalls) != 2 {
		t.Errorf("expected 2 ModifyVpcAttribute calls, got %d", len(ec2Mock.modifyVpcAttrCalls))
	}

	// DHCP options
	if len(ec2Mock.createDhcpCalls) != 1 {
		t.Errorf("expected 1 CreateDhcpOptions call, got %d", len(ec2Mock.createDhcpCalls))
	}
	if out.DHCPOptionsID != "dopt-0001" {
		t.Errorf("DHCPOptionsID = %q, want dopt-0001", out.DHCPOptionsID)
	}

	// IGW
	if len(ec2Mock.createIGWCalls) != 1 {
		t.Errorf("expected 1 CreateInternetGateway call, got %d", len(ec2Mock.createIGWCalls))
	}
	if out.InternetGatewayID != "igw-0001" {
		t.Errorf("InternetGatewayID = %q, want igw-0001", out.InternetGatewayID)
	}

	// 1 private + 1 public subnet
	if len(ec2Mock.createSubnetCalls) != 2 {
		t.Fatalf("expected 2 CreateSubnet calls, got %d", len(ec2Mock.createSubnetCalls))
	}
	if aws.ToString(ec2Mock.createSubnetCalls[0].CidrBlock) != "10.0.128.0/20" {
		t.Errorf("private subnet CIDR = %q, want 10.0.128.0/20", aws.ToString(ec2Mock.createSubnetCalls[0].CidrBlock))
	}
	if aws.ToString(ec2Mock.createSubnetCalls[1].CidrBlock) != "10.0.0.0/20" {
		t.Errorf("public subnet CIDR = %q, want 10.0.0.0/20", aws.ToString(ec2Mock.createSubnetCalls[1].CidrBlock))
	}
	if len(out.PrivateSubnetIDs) != 1 || len(out.PublicSubnetIDs) != 1 {
		t.Errorf("expected 1 private + 1 public subnet, got %d + %d", len(out.PrivateSubnetIDs), len(out.PublicSubnetIDs))
	}

	// NAT gateway
	if len(ec2Mock.createNatCalls) != 1 {
		t.Errorf("expected 1 CreateNatGateway call, got %d", len(ec2Mock.createNatCalls))
	}
	if len(out.NATGatewayIDs) != 1 {
		t.Errorf("expected 1 NAT gateway ID, got %d", len(out.NATGatewayIDs))
	}

	// Route tables: 1 private + 1 public = 2
	if len(ec2Mock.createRTCalls) != 2 {
		t.Errorf("expected 2 CreateRouteTable calls, got %d", len(ec2Mock.createRTCalls))
	}
	if len(out.PrivateRouteTableIDs) != 1 {
		t.Errorf("expected 1 private route table, got %d", len(out.PrivateRouteTableIDs))
	}
	if out.PublicRouteTableID == "" {
		t.Error("expected non-empty public route table ID")
	}

	// Routes: 1 private (0.0.0.0/0 -> NAT) + 1 public (0.0.0.0/0 -> IGW) = 2
	if len(ec2Mock.createRouteCalls) != 2 {
		t.Errorf("expected 2 CreateRoute calls, got %d", len(ec2Mock.createRouteCalls))
	}

	// S3 endpoint
	if len(ec2Mock.createVpcEndpointCalls) != 1 {
		t.Errorf("expected 1 CreateVpcEndpoint call, got %d", len(ec2Mock.createVpcEndpointCalls))
	}
	if out.S3EndpointID != "vpce-0001" {
		t.Errorf("S3EndpointID = %q, want vpce-0001", out.S3EndpointID)
	}
	expectedService := "com.amazonaws.us-east-1.s3"
	if aws.ToString(ec2Mock.createVpcEndpointCalls[0].ServiceName) != expectedService {
		t.Errorf("S3 endpoint service = %q, want %q", aws.ToString(ec2Mock.createVpcEndpointCalls[0].ServiceName), expectedService)
	}

	// DNS zones: 2 (private + local)
	if len(r53Mock.createZoneCalls) != 2 {
		t.Fatalf("expected 2 CreateHostedZone calls, got %d", len(r53Mock.createZoneCalls))
	}
	if out.PrivateDNSZoneID == "" {
		t.Error("expected non-empty PrivateDNSZoneID")
	}
	if out.LocalDNSZoneID == "" {
		t.Error("expected non-empty LocalDNSZoneID")
	}
	if aws.ToString(r53Mock.createZoneCalls[0].Name) != "test-infra-123.example.com" {
		t.Errorf("private zone name = %q, want test-infra-123.example.com", aws.ToString(r53Mock.createZoneCalls[0].Name))
	}
	if aws.ToString(r53Mock.createZoneCalls[1].Name) != "test-infra-123.hypershift.local" {
		t.Errorf("local zone name = %q, want test-infra-123.hypershift.local", aws.ToString(r53Mock.createZoneCalls[1].Name))
	}

	// Zones preserved
	if len(out.Zones) != 1 || out.Zones[0] != "us-east-1a" {
		t.Errorf("Zones = %v, want [us-east-1a]", out.Zones)
	}

	// Verify tagging: VPC should have the cluster ownership tag
	vpcTags := ec2Mock.createVpcCalls[0].TagSpecifications[0].Tags
	foundClusterTag := false
	for _, tag := range vpcTags {
		if aws.ToString(tag.Key) == "kubernetes.io/cluster/test-infra-123" && aws.ToString(tag.Value) == "owned" {
			foundClusterTag = true
			break
		}
	}
	if !foundClusterTag {
		t.Error("VPC missing kubernetes.io/cluster/test-infra-123: owned tag")
	}
}

func testInfraOutput() *InfraOutput {
	return &InfraOutput{
		VPCID:                "vpc-0001",
		DHCPOptionsID:        "dopt-0001",
		InternetGatewayID:    "igw-0001",
		PrivateSubnetIDs:     []string{"subnet-0001"},
		PublicSubnetIDs:      []string{"subnet-0002"},
		ElasticIPAllocIDs:    []string{"eipalloc-0001"},
		NATGatewayIDs:        []string{"nat-0001"},
		PrivateRouteTableIDs: []string{"rtb-0001"},
		PublicRouteTableID:   "rtb-0002",
		S3EndpointID:         "vpce-0001",
		PrivateDNSZoneID:     "/hostedzone/Z0001",
		LocalDNSZoneID:       "/hostedzone/Z0002",
		Zones:                []string{"us-east-1a"},
	}
}

func TestDestroyInfra_CorrectOrder(t *testing.T) {
	ec2Mock := &mockEC2{}
	r53Mock := &mockRoute53{}

	err := DestroyInfra(context.Background(), ec2Mock, r53Mock, "test-infra", testInfraOutput())
	if err != nil {
		t.Fatalf("DestroyInfra failed: %v", err)
	}

	// Verify DNS zones deleted first
	if len(r53Mock.operations) < 4 {
		t.Fatalf("expected at least 4 Route53 operations, got %d: %v", len(r53Mock.operations), r53Mock.operations)
	}

	// Build ordered list of high-level operations from EC2 mock
	// opIndex finds the first operation that exactly equals name or starts with name + ":"
	opIndex := func(name string) int {
		for i, op := range ec2Mock.operations {
			if op == name || strings.HasPrefix(op, name+":") {
				return i
			}
		}
		return -1
	}

	// Verify reverse dependency order:
	// VPC endpoints before route tables
	epIdx := opIndex("DeleteVpcEndpoints")
	rtIdx := opIndex("DeleteRouteTable")
	if epIdx < 0 || rtIdx < 0 {
		t.Fatalf("missing DeleteVpcEndpoints or DeleteRouteTable in operations: %v", ec2Mock.operations)
	}
	if epIdx > rtIdx {
		t.Errorf("DeleteVpcEndpoints (idx=%d) should come before DeleteRouteTable (idx=%d)", epIdx, rtIdx)
	}

	// Route tables before NAT gateways
	natIdx := opIndex("DeleteNatGateway")
	if natIdx < 0 {
		t.Fatalf("missing DeleteNatGateway in operations: %v", ec2Mock.operations)
	}
	if rtIdx > natIdx {
		t.Errorf("DeleteRouteTable (idx=%d) should come before DeleteNatGateway (idx=%d)", rtIdx, natIdx)
	}

	// NAT gateways before EIPs
	eipIdx := opIndex("ReleaseAddress")
	if eipIdx < 0 {
		t.Fatalf("missing ReleaseAddress in operations: %v", ec2Mock.operations)
	}
	if natIdx > eipIdx {
		t.Errorf("DeleteNatGateway (idx=%d) should come before ReleaseAddress (idx=%d)", natIdx, eipIdx)
	}

	// EIPs before subnets
	subIdx := opIndex("DeleteSubnet")
	if subIdx < 0 {
		t.Fatalf("missing DeleteSubnet in operations: %v", ec2Mock.operations)
	}
	if eipIdx > subIdx {
		t.Errorf("ReleaseAddress (idx=%d) should come before DeleteSubnet (idx=%d)", eipIdx, subIdx)
	}

	// Subnets before IGW
	igwDetachIdx := opIndex("DetachInternetGateway")
	if igwDetachIdx < 0 {
		t.Fatalf("missing DetachInternetGateway in operations: %v", ec2Mock.operations)
	}
	if subIdx > igwDetachIdx {
		t.Errorf("DeleteSubnet (idx=%d) should come before DetachInternetGateway (idx=%d)", subIdx, igwDetachIdx)
	}

	// IGW detach before IGW delete
	igwDeleteIdx := opIndex("DeleteInternetGateway")
	if igwDeleteIdx < 0 {
		t.Fatalf("missing DeleteInternetGateway in operations: %v", ec2Mock.operations)
	}
	if igwDetachIdx > igwDeleteIdx {
		t.Errorf("DetachInternetGateway (idx=%d) should come before DeleteInternetGateway (idx=%d)", igwDetachIdx, igwDeleteIdx)
	}

	// IGW before DHCP
	dhcpIdx := opIndex("DeleteDhcpOptions")
	if dhcpIdx < 0 {
		t.Fatalf("missing DeleteDhcpOptions in operations: %v", ec2Mock.operations)
	}
	if igwDeleteIdx > dhcpIdx {
		t.Errorf("DeleteInternetGateway (idx=%d) should come before DeleteDhcpOptions (idx=%d)", igwDeleteIdx, dhcpIdx)
	}

	// DHCP before VPC
	vpcIdx := opIndex("DeleteVpc")
	if vpcIdx < 0 {
		t.Fatalf("missing DeleteVpc in operations: %v", ec2Mock.operations)
	}
	if dhcpIdx > vpcIdx {
		t.Errorf("DeleteDhcpOptions (idx=%d) should come before DeleteVpc (idx=%d)", dhcpIdx, vpcIdx)
	}
}

func TestDestroyInfra_DeletesAllSubnets(t *testing.T) {
	ec2Mock := &mockEC2{}
	r53Mock := &mockRoute53{}

	out := testInfraOutput()
	out.PrivateSubnetIDs = []string{"subnet-priv-1", "subnet-priv-2"}
	out.PublicSubnetIDs = []string{"subnet-pub-1", "subnet-pub-2"}

	err := DestroyInfra(context.Background(), ec2Mock, r53Mock, "test-infra", out)
	if err != nil {
		t.Fatalf("DestroyInfra failed: %v", err)
	}

	deleteSubnetCount := 0
	for _, op := range ec2Mock.operations {
		if len(op) >= 12 && op[:12] == "DeleteSubnet" {
			deleteSubnetCount++
		}
	}
	if deleteSubnetCount != 4 {
		t.Errorf("expected 4 DeleteSubnet calls, got %d", deleteSubnetCount)
	}
}

func TestDestroyInfra_ErrorReturned(t *testing.T) {
	ec2Mock := &mockEC2{
		deleteErr:   fmt.Errorf("access denied"),
		deleteErrOn: "DeleteNatGateway",
	}
	r53Mock := &mockRoute53{}

	err := DestroyInfra(context.Background(), ec2Mock, r53Mock, "test-infra", testInfraOutput())
	if err == nil {
		t.Fatal("expected error from DestroyInfra, got nil")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Errorf("error = %q, want it to contain 'access denied'", err.Error())
	}
}

func TestDestroyInfra_DNSRecordCleanup(t *testing.T) {
	ec2Mock := &mockEC2{}
	r53Mock := &mockRoute53{
		listRecordSetsResult: &route53.ListResourceRecordSetsOutput{
			ResourceRecordSets: []r53types.ResourceRecordSet{
				{Type: r53types.RRTypeNs, Name: aws.String("example.com.")},
				{Type: r53types.RRTypeSoa, Name: aws.String("example.com.")},
				{Type: r53types.RRTypeA, Name: aws.String("api.example.com.")},
			},
		},
	}

	err := DestroyInfra(context.Background(), ec2Mock, r53Mock, "test-infra", testInfraOutput())
	if err != nil {
		t.Fatalf("DestroyInfra failed: %v", err)
	}

	// Should list records, change (delete non-default), then delete zone — for each zone
	hasChange := false
	for _, op := range r53Mock.operations {
		if op == "ChangeResourceRecordSets" {
			hasChange = true
		}
	}
	if !hasChange {
		t.Error("expected ChangeResourceRecordSets to delete non-default records")
	}
}

func TestDestroyInfra_NilOutput_NoResources(t *testing.T) {
	// When no VPCs are found by tag, discovery returns empty output and
	// DestroyInfra completes as a no-op.
	ec2Mock := &mockEC2{}
	r53Mock := &mockRoute53{}

	err := DestroyInfra(context.Background(), ec2Mock, r53Mock, "test-infra", nil)
	if err != nil {
		t.Fatalf("expected no error when no resources found, got: %v", err)
	}
}
