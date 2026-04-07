package hcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
)

// EC2API is the subset of the EC2 client needed for infrastructure creation.
type EC2API interface {
	CreateVpc(ctx context.Context, params *ec2.CreateVpcInput, optFns ...func(*ec2.Options)) (*ec2.CreateVpcOutput, error)
	ModifyVpcAttribute(ctx context.Context, params *ec2.ModifyVpcAttributeInput, optFns ...func(*ec2.Options)) (*ec2.ModifyVpcAttributeOutput, error)
	CreateDhcpOptions(ctx context.Context, params *ec2.CreateDhcpOptionsInput, optFns ...func(*ec2.Options)) (*ec2.CreateDhcpOptionsOutput, error)
	AssociateDhcpOptions(ctx context.Context, params *ec2.AssociateDhcpOptionsInput, optFns ...func(*ec2.Options)) (*ec2.AssociateDhcpOptionsOutput, error)
	CreateInternetGateway(ctx context.Context, params *ec2.CreateInternetGatewayInput, optFns ...func(*ec2.Options)) (*ec2.CreateInternetGatewayOutput, error)
	AttachInternetGateway(ctx context.Context, params *ec2.AttachInternetGatewayInput, optFns ...func(*ec2.Options)) (*ec2.AttachInternetGatewayOutput, error)
	CreateSubnet(ctx context.Context, params *ec2.CreateSubnetInput, optFns ...func(*ec2.Options)) (*ec2.CreateSubnetOutput, error)
	AllocateAddress(ctx context.Context, params *ec2.AllocateAddressInput, optFns ...func(*ec2.Options)) (*ec2.AllocateAddressOutput, error)
	CreateNatGateway(ctx context.Context, params *ec2.CreateNatGatewayInput, optFns ...func(*ec2.Options)) (*ec2.CreateNatGatewayOutput, error)
	CreateRouteTable(ctx context.Context, params *ec2.CreateRouteTableInput, optFns ...func(*ec2.Options)) (*ec2.CreateRouteTableOutput, error)
	CreateRoute(ctx context.Context, params *ec2.CreateRouteInput, optFns ...func(*ec2.Options)) (*ec2.CreateRouteOutput, error)
	AssociateRouteTable(ctx context.Context, params *ec2.AssociateRouteTableInput, optFns ...func(*ec2.Options)) (*ec2.AssociateRouteTableOutput, error)
	CreateVpcEndpoint(ctx context.Context, params *ec2.CreateVpcEndpointInput, optFns ...func(*ec2.Options)) (*ec2.CreateVpcEndpointOutput, error)
	CreateTags(ctx context.Context, params *ec2.CreateTagsInput, optFns ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error)

	// Cleanup / describe methods
	DeleteVpc(ctx context.Context, params *ec2.DeleteVpcInput, optFns ...func(*ec2.Options)) (*ec2.DeleteVpcOutput, error)
	DeleteSubnet(ctx context.Context, params *ec2.DeleteSubnetInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSubnetOutput, error)
	DeleteInternetGateway(ctx context.Context, params *ec2.DeleteInternetGatewayInput, optFns ...func(*ec2.Options)) (*ec2.DeleteInternetGatewayOutput, error)
	DetachInternetGateway(ctx context.Context, params *ec2.DetachInternetGatewayInput, optFns ...func(*ec2.Options)) (*ec2.DetachInternetGatewayOutput, error)
	DeleteNatGateway(ctx context.Context, params *ec2.DeleteNatGatewayInput, optFns ...func(*ec2.Options)) (*ec2.DeleteNatGatewayOutput, error)
	ReleaseAddress(ctx context.Context, params *ec2.ReleaseAddressInput, optFns ...func(*ec2.Options)) (*ec2.ReleaseAddressOutput, error)
	DeleteRouteTable(ctx context.Context, params *ec2.DeleteRouteTableInput, optFns ...func(*ec2.Options)) (*ec2.DeleteRouteTableOutput, error)
	DeleteVpcEndpoints(ctx context.Context, params *ec2.DeleteVpcEndpointsInput, optFns ...func(*ec2.Options)) (*ec2.DeleteVpcEndpointsOutput, error)
	DeleteDhcpOptions(ctx context.Context, params *ec2.DeleteDhcpOptionsInput, optFns ...func(*ec2.Options)) (*ec2.DeleteDhcpOptionsOutput, error)
	DescribeVpcs(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error)
	DescribeSubnets(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	DescribeNatGateways(ctx context.Context, params *ec2.DescribeNatGatewaysInput, optFns ...func(*ec2.Options)) (*ec2.DescribeNatGatewaysOutput, error)
	DescribeRouteTables(ctx context.Context, params *ec2.DescribeRouteTablesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeRouteTablesOutput, error)
	DescribeInternetGateways(ctx context.Context, params *ec2.DescribeInternetGatewaysInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInternetGatewaysOutput, error)
	DescribeVpcEndpoints(ctx context.Context, params *ec2.DescribeVpcEndpointsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcEndpointsOutput, error)
	DescribeAddresses(ctx context.Context, params *ec2.DescribeAddressesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeAddressesOutput, error)
	DisassociateRouteTable(ctx context.Context, params *ec2.DisassociateRouteTableInput, optFns ...func(*ec2.Options)) (*ec2.DisassociateRouteTableOutput, error)
	ReplaceRouteTableAssociation(ctx context.Context, params *ec2.ReplaceRouteTableAssociationInput, optFns ...func(*ec2.Options)) (*ec2.ReplaceRouteTableAssociationOutput, error)
}

// Route53API is the subset of the Route53 client needed for DNS zone management.
type Route53API interface {
	CreateHostedZone(ctx context.Context, params *route53.CreateHostedZoneInput, optFns ...func(*route53.Options)) (*route53.CreateHostedZoneOutput, error)
	DeleteHostedZone(ctx context.Context, params *route53.DeleteHostedZoneInput, optFns ...func(*route53.Options)) (*route53.DeleteHostedZoneOutput, error)
	ListHostedZonesByName(ctx context.Context, params *route53.ListHostedZonesByNameInput, optFns ...func(*route53.Options)) (*route53.ListHostedZonesByNameOutput, error)
	ListResourceRecordSets(ctx context.Context, params *route53.ListResourceRecordSetsInput, optFns ...func(*route53.Options)) (*route53.ListResourceRecordSetsOutput, error)
	ChangeResourceRecordSets(ctx context.Context, params *route53.ChangeResourceRecordSetsInput, optFns ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error)
}

// InfraOutput captures all AWS resource IDs created during infrastructure setup.
type InfraOutput struct {
	VPCID              string
	DHCPOptionsID      string
	InternetGatewayID  string
	PrivateSubnetIDs   []string
	PublicSubnetIDs    []string
	ElasticIPAllocIDs  []string
	NATGatewayIDs      []string
	PrivateRouteTableIDs []string
	PublicRouteTableID string
	S3EndpointID       string
	PrivateDNSZoneID   string
	LocalDNSZoneID     string
	Zones              []string
}

// InfraSpec holds the parameters for infrastructure creation.
// If ClusterSpec from agent.go is available, use it instead.
type InfraSpec struct {
	Name       string
	InfraID    string
	Region     string
	BaseDomain string
	Zones      []string
}

const (
	// Subnet CIDRs use /20 to match HyperShift CLI — gives 4094 IPs per subnet.
	basePublicSubnetCIDR  = "10.0.0.0/20"
	basePrivateSubnetCIDR = "10.0.128.0/20"
)

func clusterTag(infraID string) ec2types.Tag {
	return ec2types.Tag{
		Key:   aws.String(fmt.Sprintf("kubernetes.io/cluster/%s", infraID)),
		Value: aws.String("owned"),
	}
}

func nameTag(name string) ec2types.Tag {
	return ec2types.Tag{
		Key:   aws.String("Name"),
		Value: aws.String(name),
	}
}

func elbTag(key string) ec2types.Tag {
	return ec2types.Tag{
		Key:   aws.String(key),
		Value: aws.String("1"),
	}
}

// dhcpDomainName returns the correct DHCP domain name for a region.
// us-east-1 uses "ec2.internal"; all other regions use "{region}.compute.internal".
func dhcpDomainName(region string) string {
	if region == "us-east-1" {
		return "ec2.internal"
	}
	return region + ".compute.internal"
}

// CreateInfra provisions the full VPC + networking stack for an HCP cluster.
func CreateInfra(ctx context.Context, ec2Client EC2API, r53Client Route53API, spec InfraSpec) (*InfraOutput, error) {
	out := &InfraOutput{Zones: spec.Zones}
	tag := clusterTag(spec.InfraID)

	// 1. Create VPC
	vpcOut, err := ec2Client.CreateVpc(ctx, &ec2.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeVpc,
			Tags:         []ec2types.Tag{tag, nameTag(spec.InfraID + "-vpc")},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("create VPC: %w", err)
	}
	out.VPCID = aws.ToString(vpcOut.Vpc.VpcId)

	// Enable DNS support and hostnames
	for _, attr := range []struct {
		value *ec2.ModifyVpcAttributeInput
	}{
		{&ec2.ModifyVpcAttributeInput{VpcId: vpcOut.Vpc.VpcId, EnableDnsSupport: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)}}},
		{&ec2.ModifyVpcAttributeInput{VpcId: vpcOut.Vpc.VpcId, EnableDnsHostnames: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)}}},
	} {
		if _, err := ec2Client.ModifyVpcAttribute(ctx, attr.value); err != nil {
			return nil, fmt.Errorf("modify VPC attribute: %w", err)
		}
	}

	// 2. DHCP Options
	dhcpOut, err := ec2Client.CreateDhcpOptions(ctx, &ec2.CreateDhcpOptionsInput{
		DhcpConfigurations: []ec2types.NewDhcpConfiguration{
			{Key: aws.String("domain-name"), Values: []string{dhcpDomainName(spec.Region)}},
			{Key: aws.String("domain-name-servers"), Values: []string{"AmazonProvidedDNS"}},
		},
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeDhcpOptions,
			Tags:         []ec2types.Tag{tag},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("create DHCP options: %w", err)
	}
	out.DHCPOptionsID = aws.ToString(dhcpOut.DhcpOptions.DhcpOptionsId)

	if _, err := ec2Client.AssociateDhcpOptions(ctx, &ec2.AssociateDhcpOptionsInput{
		DhcpOptionsId: dhcpOut.DhcpOptions.DhcpOptionsId,
		VpcId:         vpcOut.Vpc.VpcId,
	}); err != nil {
		return nil, fmt.Errorf("associate DHCP options: %w", err)
	}

	// 3. Internet Gateway
	igwOut, err := ec2Client.CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeInternetGateway,
			Tags:         []ec2types.Tag{tag, nameTag(spec.InfraID + "-igw")},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("create internet gateway: %w", err)
	}
	out.InternetGatewayID = aws.ToString(igwOut.InternetGateway.InternetGatewayId)

	if _, err := ec2Client.AttachInternetGateway(ctx, &ec2.AttachInternetGatewayInput{
		InternetGatewayId: igwOut.InternetGateway.InternetGatewayId,
		VpcId:             vpcOut.Vpc.VpcId,
	}); err != nil {
		return nil, fmt.Errorf("attach internet gateway: %w", err)
	}

	// 4. Per-zone: private subnet, public subnet, EIP, NAT GW, private route table.
	// CIDRs use /20 (4094 IPs) to match HyperShift CLI:
	//   Public:  10.0.0.0/20, 10.0.16.0/20, 10.0.32.0/20, ...
	//   Private: 10.0.128.0/20, 10.0.144.0/20, 10.0.160.0/20, ...
	_, pubNet, _ := net.ParseCIDR(basePublicSubnetCIDR)
	_, privNet, _ := net.ParseCIDR(basePrivateSubnetCIDR)

	for i, zone := range spec.Zones {
		privCIDR := offsetCIDR(privNet, i)
		privSubnet, err := ec2Client.CreateSubnet(ctx, &ec2.CreateSubnetInput{
			VpcId:            vpcOut.Vpc.VpcId,
			CidrBlock:        aws.String(privCIDR),
			AvailabilityZone: aws.String(zone),
			TagSpecifications: []ec2types.TagSpecification{{
				ResourceType: ec2types.ResourceTypeSubnet,
				Tags: []ec2types.Tag{
					tag,
					nameTag(fmt.Sprintf("%s-private-%s", spec.InfraID, zone)),
					elbTag("kubernetes.io/role/internal-elb"),
				},
			}},
		})
		if err != nil {
			return nil, fmt.Errorf("create private subnet in %s: %w", zone, err)
		}
		out.PrivateSubnetIDs = append(out.PrivateSubnetIDs, aws.ToString(privSubnet.Subnet.SubnetId))

		pubCIDR := offsetCIDR(pubNet, i)
		pubSubnet, err := ec2Client.CreateSubnet(ctx, &ec2.CreateSubnetInput{
			VpcId:            vpcOut.Vpc.VpcId,
			CidrBlock:        aws.String(pubCIDR),
			AvailabilityZone: aws.String(zone),
			TagSpecifications: []ec2types.TagSpecification{{
				ResourceType: ec2types.ResourceTypeSubnet,
				Tags: []ec2types.Tag{
					tag,
					nameTag(fmt.Sprintf("%s-public-%s", spec.InfraID, zone)),
					elbTag("kubernetes.io/role/elb"),
				},
			}},
		})
		if err != nil {
			return nil, fmt.Errorf("create public subnet in %s: %w", zone, err)
		}
		out.PublicSubnetIDs = append(out.PublicSubnetIDs, aws.ToString(pubSubnet.Subnet.SubnetId))

		// Elastic IP for NAT
		eipOut, err := ec2Client.AllocateAddress(ctx, &ec2.AllocateAddressInput{
			Domain: ec2types.DomainTypeVpc,
			TagSpecifications: []ec2types.TagSpecification{{
				ResourceType: ec2types.ResourceTypeElasticIp,
				Tags:         []ec2types.Tag{tag, nameTag(fmt.Sprintf("%s-eip-%s", spec.InfraID, zone))},
			}},
		})
		if err != nil {
			return nil, fmt.Errorf("allocate EIP in %s: %w", zone, err)
		}
		out.ElasticIPAllocIDs = append(out.ElasticIPAllocIDs, aws.ToString(eipOut.AllocationId))

		// NAT Gateway in public subnet
		natOut, err := ec2Client.CreateNatGateway(ctx, &ec2.CreateNatGatewayInput{
			SubnetId:     pubSubnet.Subnet.SubnetId,
			AllocationId: eipOut.AllocationId,
			TagSpecifications: []ec2types.TagSpecification{{
				ResourceType: ec2types.ResourceTypeNatgateway,
				Tags:         []ec2types.Tag{tag, nameTag(fmt.Sprintf("%s-nat-%s", spec.InfraID, zone))},
			}},
		})
		if err != nil {
			return nil, fmt.Errorf("create NAT gateway in %s: %w", zone, err)
		}
		out.NATGatewayIDs = append(out.NATGatewayIDs, aws.ToString(natOut.NatGateway.NatGatewayId))

		// Private route table
		privRT, err := ec2Client.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{
			VpcId: vpcOut.Vpc.VpcId,
			TagSpecifications: []ec2types.TagSpecification{{
				ResourceType: ec2types.ResourceTypeRouteTable,
				Tags:         []ec2types.Tag{tag, nameTag(fmt.Sprintf("%s-private-rt-%s", spec.InfraID, zone))},
			}},
		})
		if err != nil {
			return nil, fmt.Errorf("create private route table in %s: %w", zone, err)
		}
		privRTID := aws.ToString(privRT.RouteTable.RouteTableId)
		out.PrivateRouteTableIDs = append(out.PrivateRouteTableIDs, privRTID)

		// Default route via NAT
		if _, err := ec2Client.CreateRoute(ctx, &ec2.CreateRouteInput{
			RouteTableId:         aws.String(privRTID),
			DestinationCidrBlock: aws.String("0.0.0.0/0"),
			NatGatewayId:         natOut.NatGateway.NatGatewayId,
		}); err != nil {
			return nil, fmt.Errorf("create private route in %s: %w", zone, err)
		}

		// Associate private subnet with private route table
		if _, err := ec2Client.AssociateRouteTable(ctx, &ec2.AssociateRouteTableInput{
			RouteTableId: aws.String(privRTID),
			SubnetId:     privSubnet.Subnet.SubnetId,
		}); err != nil {
			return nil, fmt.Errorf("associate private route table in %s: %w", zone, err)
		}
	}

	// 5. Public route table
	pubRT, err := ec2Client.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{
		VpcId: vpcOut.Vpc.VpcId,
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeRouteTable,
			Tags:         []ec2types.Tag{tag, nameTag(spec.InfraID + "-public-rt")},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("create public route table: %w", err)
	}
	out.PublicRouteTableID = aws.ToString(pubRT.RouteTable.RouteTableId)

	// Replace the VPC's main route table with the public one. This ensures
	// unassociated subnets route through the IGW by default, matching
	// HyperShift CLI behavior.
	mainRTs, err := ec2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{out.VPCID}},
			{Name: aws.String("association.main"), Values: []string{"true"}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("describe main route table: %w", err)
	}
	if len(mainRTs.RouteTables) > 0 {
		for _, assoc := range mainRTs.RouteTables[0].Associations {
			if aws.ToBool(assoc.Main) && assoc.RouteTableAssociationId != nil {
				if _, err := ec2Client.ReplaceRouteTableAssociation(ctx, &ec2.ReplaceRouteTableAssociationInput{
					RouteTableId:  aws.String(out.PublicRouteTableID),
					AssociationId: assoc.RouteTableAssociationId,
				}); err != nil {
					return nil, fmt.Errorf("replace main route table: %w", err)
				}
				break
			}
		}
	}

	// Default route via IGW
	if _, err := ec2Client.CreateRoute(ctx, &ec2.CreateRouteInput{
		RouteTableId:         pubRT.RouteTable.RouteTableId,
		DestinationCidrBlock: aws.String("0.0.0.0/0"),
		GatewayId:            igwOut.InternetGateway.InternetGatewayId,
	}); err != nil {
		return nil, fmt.Errorf("create public route: %w", err)
	}

	// Associate all public subnets with public route table
	for i, subnetID := range out.PublicSubnetIDs {
		if _, err := ec2Client.AssociateRouteTable(ctx, &ec2.AssociateRouteTableInput{
			RouteTableId: pubRT.RouteTable.RouteTableId,
			SubnetId:     aws.String(subnetID),
		}); err != nil {
			return nil, fmt.Errorf("associate public route table for subnet %d: %w", i, err)
		}
	}

	// 6. S3 VPC Endpoint
	s3Ep, err := ec2Client.CreateVpcEndpoint(ctx, &ec2.CreateVpcEndpointInput{
		VpcId:       vpcOut.Vpc.VpcId,
		ServiceName: aws.String(fmt.Sprintf("com.amazonaws.%s.s3", spec.Region)),
		RouteTableIds: append(out.PrivateRouteTableIDs, out.PublicRouteTableID),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeVpcEndpoint,
			Tags:         []ec2types.Tag{tag, nameTag(spec.InfraID + "-s3-endpoint")},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 endpoint: %w", err)
	}
	out.S3EndpointID = aws.ToString(s3Ep.VpcEndpoint.VpcEndpointId)

	// 7. DNS Zones
	// Private zone: <infraID>.<baseDomain>
	privZone, err := r53Client.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
		Name:            aws.String(fmt.Sprintf("%s.%s", spec.InfraID, spec.BaseDomain)),
		CallerReference: aws.String(fmt.Sprintf("%s-private", spec.InfraID)),
		HostedZoneConfig: &r53types.HostedZoneConfig{
			PrivateZone: true,
			Comment:     aws.String(fmt.Sprintf("Private zone for %s", spec.InfraID)),
		},
		VPC: &r53types.VPC{
			VPCId:     aws.String(out.VPCID),
			VPCRegion: r53types.VPCRegion(spec.Region),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create private DNS zone: %w", err)
	}
	out.PrivateDNSZoneID = aws.ToString(privZone.HostedZone.Id)

	// Local zone: <infraID>.hypershift.local
	localZone, err := r53Client.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{
		Name:            aws.String(fmt.Sprintf("%s.hypershift.local", spec.InfraID)),
		CallerReference: aws.String(fmt.Sprintf("%s-local", spec.InfraID)),
		HostedZoneConfig: &r53types.HostedZoneConfig{
			PrivateZone: true,
			Comment:     aws.String(fmt.Sprintf("Local zone for %s", spec.InfraID)),
		},
		VPC: &r53types.VPC{
			VPCId:     aws.String(out.VPCID),
			VPCRegion: r53types.VPCRegion(spec.Region),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create local DNS zone: %w", err)
	}
	out.LocalDNSZoneID = aws.ToString(localZone.HostedZone.Id)

	return out, nil
}

// DestroyInfra tears down the VPC + networking stack in reverse dependency order.
// If out is provided, resource IDs are used directly. If out is nil, resources
// are discovered by the kubernetes.io/cluster/{infraID}: owned tag.
func DestroyInfra(ctx context.Context, ec2Client EC2API, r53Client Route53API, infraID string, out *InfraOutput) error {
	if out == nil {
		discovered, err := discoverInfra(ctx, ec2Client, r53Client, infraID)
		if err != nil {
			return fmt.Errorf("discover infrastructure for %s: %w", infraID, err)
		}
		out = discovered
	}

	// 1. Delete DNS zones (delete non-default record sets first)
	for _, zoneID := range []string{out.PrivateDNSZoneID, out.LocalDNSZoneID} {
		if zoneID == "" {
			continue
		}
		if err := deleteHostedZone(ctx, r53Client, zoneID); err != nil {
			return err
		}
	}

	// 2. Delete S3 VPC endpoint
	if out.S3EndpointID != "" {
		if _, err := ec2Client.DeleteVpcEndpoints(ctx, &ec2.DeleteVpcEndpointsInput{
			VpcEndpointIds: []string{out.S3EndpointID},
		}); err != nil {
			return fmt.Errorf("delete S3 endpoint %s: %w", out.S3EndpointID, err)
		}
	}

	// 3. Delete route tables — disassociate subnets first, then delete
	allRTIDs := make([]string, 0, len(out.PrivateRouteTableIDs)+1)
	allRTIDs = append(allRTIDs, out.PrivateRouteTableIDs...)
	if out.PublicRouteTableID != "" {
		allRTIDs = append(allRTIDs, out.PublicRouteTableID)
	}
	for _, rtID := range allRTIDs {
		descRT, err := ec2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
			RouteTableIds: []string{rtID},
		})
		if err != nil {
			return fmt.Errorf("describe route table %s: %w", rtID, err)
		}
		for _, rt := range descRT.RouteTables {
			for _, assoc := range rt.Associations {
				if !aws.ToBool(assoc.Main) && assoc.RouteTableAssociationId != nil {
					if _, err := ec2Client.DisassociateRouteTable(ctx, &ec2.DisassociateRouteTableInput{
						AssociationId: assoc.RouteTableAssociationId,
					}); err != nil {
						return fmt.Errorf("disassociate route table %s: %w", rtID, err)
					}
				}
			}
		}
		if _, err := ec2Client.DeleteRouteTable(ctx, &ec2.DeleteRouteTableInput{
			RouteTableId: aws.String(rtID),
		}); err != nil {
			return fmt.Errorf("delete route table %s: %w", rtID, err)
		}
	}

	// 4. Delete NAT gateways and wait for deletion
	for _, natID := range out.NATGatewayIDs {
		if _, err := ec2Client.DeleteNatGateway(ctx, &ec2.DeleteNatGatewayInput{
			NatGatewayId: aws.String(natID),
		}); err != nil {
			return fmt.Errorf("delete NAT gateway %s: %w", natID, err)
		}
	}
	// Wait for NAT gateways to reach "deleted" state
	for _, natID := range out.NATGatewayIDs {
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for NAT gateway %s deletion: %w", natID, ctx.Err())
			default:
			}
			desc, err := ec2Client.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{
				NatGatewayIds: []string{natID},
			})
			if err != nil {
				return fmt.Errorf("describe NAT gateway %s: %w", natID, err)
			}
			if len(desc.NatGateways) == 0 || desc.NatGateways[0].State == ec2types.NatGatewayStateDeleted {
				break
			}
			time.Sleep(5 * time.Second)
		}
	}

	// 5. Release elastic IPs
	for _, allocID := range out.ElasticIPAllocIDs {
		if _, err := ec2Client.ReleaseAddress(ctx, &ec2.ReleaseAddressInput{
			AllocationId: aws.String(allocID),
		}); err != nil {
			return fmt.Errorf("release EIP %s: %w", allocID, err)
		}
	}

	// 6. Delete subnets (private and public)
	for _, subnetID := range append(out.PrivateSubnetIDs, out.PublicSubnetIDs...) {
		if _, err := ec2Client.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{
			SubnetId: aws.String(subnetID),
		}); err != nil {
			return fmt.Errorf("delete subnet %s: %w", subnetID, err)
		}
	}

	// 7. Detach and delete internet gateway
	if out.InternetGatewayID != "" {
		if _, err := ec2Client.DetachInternetGateway(ctx, &ec2.DetachInternetGatewayInput{
			InternetGatewayId: aws.String(out.InternetGatewayID),
			VpcId:             aws.String(out.VPCID),
		}); err != nil {
			return fmt.Errorf("detach internet gateway: %w", err)
		}
		if _, err := ec2Client.DeleteInternetGateway(ctx, &ec2.DeleteInternetGatewayInput{
			InternetGatewayId: aws.String(out.InternetGatewayID),
		}); err != nil {
			return fmt.Errorf("delete internet gateway: %w", err)
		}
	}

	// 8. Delete DHCP options (associate 'default' first, then delete)
	if out.DHCPOptionsID != "" {
		if _, err := ec2Client.AssociateDhcpOptions(ctx, &ec2.AssociateDhcpOptionsInput{
			DhcpOptionsId: aws.String("default"),
			VpcId:         aws.String(out.VPCID),
		}); err != nil {
			return fmt.Errorf("reset DHCP options to default: %w", err)
		}
		if _, err := ec2Client.DeleteDhcpOptions(ctx, &ec2.DeleteDhcpOptionsInput{
			DhcpOptionsId: aws.String(out.DHCPOptionsID),
		}); err != nil {
			return fmt.Errorf("delete DHCP options: %w", err)
		}
	}

	// 9. Delete VPC
	if out.VPCID != "" {
		if _, err := ec2Client.DeleteVpc(ctx, &ec2.DeleteVpcInput{
			VpcId: aws.String(out.VPCID),
		}); err != nil {
			return fmt.Errorf("delete VPC: %w", err)
		}
	}

	return nil
}

// discoverInfra finds AWS resources belonging to an HCP cluster by the
// kubernetes.io/cluster/{infraID}:owned tag and returns a populated InfraOutput.
func discoverInfra(ctx context.Context, ec2Client EC2API, r53Client Route53API, infraID string) (*InfraOutput, error) {
	out := &InfraOutput{}
	tagFilter := ec2types.Filter{
		Name:   aws.String(fmt.Sprintf("tag:kubernetes.io/cluster/%s", infraID)),
		Values: []string{"owned"},
	}

	// 1. Discover VPC
	vpcs, err := ec2Client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		Filters: []ec2types.Filter{tagFilter},
	})
	if err != nil {
		return nil, fmt.Errorf("discover VPCs: %w", err)
	}
	if len(vpcs.Vpcs) == 0 {
		return out, nil // nothing to clean up
	}
	out.VPCID = aws.ToString(vpcs.Vpcs[0].VpcId)
	if vpcs.Vpcs[0].DhcpOptionsId != nil {
		dhcpID := aws.ToString(vpcs.Vpcs[0].DhcpOptionsId)
		if dhcpID != "default" {
			out.DHCPOptionsID = dhcpID
		}
	}

	vpcFilter := ec2types.Filter{
		Name:   aws.String("vpc-id"),
		Values: []string{out.VPCID},
	}

	// 2. Discover subnets
	subnets, err := ec2Client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{tagFilter},
	})
	if err != nil {
		return nil, fmt.Errorf("discover subnets: %w", err)
	}
	for _, s := range subnets.Subnets {
		id := aws.ToString(s.SubnetId)
		if hasNameTagContaining(s.Tags, "private") {
			out.PrivateSubnetIDs = append(out.PrivateSubnetIDs, id)
		} else {
			out.PublicSubnetIDs = append(out.PublicSubnetIDs, id)
		}
	}

	// 3. Discover NAT gateways and their EIPs
	natGWs, err := ec2Client.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{
		Filter: []ec2types.Filter{vpcFilter, {
			Name:   aws.String("state"),
			Values: []string{"available", "pending"},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("discover NAT gateways: %w", err)
	}
	for _, ng := range natGWs.NatGateways {
		out.NATGatewayIDs = append(out.NATGatewayIDs, aws.ToString(ng.NatGatewayId))
		for _, addr := range ng.NatGatewayAddresses {
			if addr.AllocationId != nil {
				out.ElasticIPAllocIDs = append(out.ElasticIPAllocIDs, aws.ToString(addr.AllocationId))
			}
		}
	}

	// 4. Discover route tables
	rts, err := ec2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{tagFilter},
	})
	if err != nil {
		return nil, fmt.Errorf("discover route tables: %w", err)
	}
	for _, rt := range rts.RouteTables {
		id := aws.ToString(rt.RouteTableId)
		if hasNameTagContaining(rt.Tags, "public") {
			out.PublicRouteTableID = id
		} else {
			out.PrivateRouteTableIDs = append(out.PrivateRouteTableIDs, id)
		}
	}

	// 5. Discover internet gateways
	igws, err := ec2Client.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{
		Filters: []ec2types.Filter{{
			Name:   aws.String("attachment.vpc-id"),
			Values: []string{out.VPCID},
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("discover internet gateways: %w", err)
	}
	if len(igws.InternetGateways) > 0 {
		out.InternetGatewayID = aws.ToString(igws.InternetGateways[0].InternetGatewayId)
	}

	// 6. Discover VPC endpoints
	eps, err := ec2Client.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{
		Filters: []ec2types.Filter{vpcFilter},
	})
	if err != nil {
		return nil, fmt.Errorf("discover VPC endpoints: %w", err)
	}
	if len(eps.VpcEndpoints) > 0 {
		out.S3EndpointID = aws.ToString(eps.VpcEndpoints[0].VpcEndpointId)
	}

	// 7. Discover DNS zones by listing zones matching the infraID pattern.
	zones, err := r53Client.ListHostedZonesByName(ctx, &route53.ListHostedZonesByNameInput{})
	if err != nil {
		return nil, fmt.Errorf("list hosted zones: %w", err)
	}
	localSuffix := infraID + ".hypershift.local."
	privateSuffix := infraID + "."
	for _, z := range zones.HostedZones {
		name := aws.ToString(z.Name)
		if name == localSuffix {
			out.LocalDNSZoneID = aws.ToString(z.Id)
		} else if len(name) > len(privateSuffix) && name[:len(privateSuffix)] == privateSuffix && z.Config != nil && z.Config.PrivateZone {
			out.PrivateDNSZoneID = aws.ToString(z.Id)
		}
	}

	return out, nil
}

// offsetCIDR returns a /20 CIDR block offset by n * /20 from the base network.
// For example, offsetCIDR(10.0.0.0/20, 0) = "10.0.0.0/20",
// offsetCIDR(10.0.0.0/20, 1) = "10.0.16.0/20".
func offsetCIDR(base *net.IPNet, n int) string {
	ip := make(net.IP, len(base.IP))
	copy(ip, base.IP)
	// /20 = 4096 IPs = 16 in the third octet
	ip[2] = ip[2] + byte(n*16)
	return fmt.Sprintf("%s/20", ip)
}

// hasNameTagContaining checks if a tag list has a Name tag containing the substring.
func hasNameTagContaining(tags []ec2types.Tag, substr string) bool {
	for _, t := range tags {
		if aws.ToString(t.Key) == "Name" {
			val := aws.ToString(t.Value)
			for i := 0; i <= len(val)-len(substr); i++ {
				if val[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}

// deleteHostedZone deletes all non-default record sets from a hosted zone, then
// deletes the zone itself.
func deleteHostedZone(ctx context.Context, r53Client Route53API, zoneID string) error {
	records, err := r53Client.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	if err != nil {
		return fmt.Errorf("list records for zone %s: %w", zoneID, err)
	}

	var changes []r53types.Change
	for _, rs := range records.ResourceRecordSets {
		if rs.Type == r53types.RRTypeNs || rs.Type == r53types.RRTypeSoa {
			continue
		}
		rsCopy := rs
		changes = append(changes, r53types.Change{
			Action:            r53types.ChangeActionDelete,
			ResourceRecordSet: &rsCopy,
		})
	}

	if len(changes) > 0 {
		if _, err := r53Client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
			HostedZoneId: aws.String(zoneID),
			ChangeBatch:  &r53types.ChangeBatch{Changes: changes},
		}); err != nil {
			return fmt.Errorf("delete records in zone %s: %w", zoneID, err)
		}
	}

	if _, err := r53Client.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{
		Id: aws.String(zoneID),
	}); err != nil {
		return fmt.Errorf("delete hosted zone %s: %w", zoneID, err)
	}

	return nil
}
