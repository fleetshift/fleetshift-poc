package logpipeline

import "regexp"

type failurePattern struct {
	re      *regexp.Regexp
	reason  string
	message string
}

var failurePatterns = []failurePattern{
	{regexp.MustCompile(`(?i)exceeded quota`), "aws_quota", "AWS service quota exceeded"},
	{regexp.MustCompile(`(?i)InvalidClientTokenId`), "invalid_credentials", "AWS credentials are invalid"},
	{regexp.MustCompile(`(?i)SignatureDoesNotMatch`), "invalid_credentials", "AWS credential signature mismatch"},
	{regexp.MustCompile(`(?i)ExpiredToken`), "expired_credentials", "AWS credentials have expired"},
	{regexp.MustCompile(`(?i)bootstrap.*timeout`), "bootstrap_timeout", "Bootstrap node failed to come up within timeout"},
	{regexp.MustCompile(`(?i)context deadline exceeded`), "deadline_exceeded", "Operation exceeded timeout"},
	{regexp.MustCompile(`(?i)error creating VPC`), "aws_vpc_error", "Failed to create VPC"},
	{regexp.MustCompile(`(?i)error creating subnet`), "aws_subnet_error", "Failed to create subnet"},
	{regexp.MustCompile(`(?i)unable to resolve endpoint`), "dns_resolution", "DNS resolution failed"},
	{regexp.MustCompile(`(?i)no hosted zone found`), "dns_zone_missing", "Route53 hosted zone not found"},
	{regexp.MustCompile(`(?i)UnauthorizedAccess|AccessDenied|unauthorized`), "unauthorized", "Insufficient AWS permissions"},
	{regexp.MustCompile(`(?i)limit exceeded`), "aws_limit", "AWS resource limit exceeded"},
	{regexp.MustCompile(`(?i)connection refused`), "connection_error", "Connection to endpoint refused"},
	{regexp.MustCompile(`(?i)connection timed out`), "connection_timeout", "Connection to endpoint timed out"},
	{regexp.MustCompile(`(?i)i/o timeout`), "io_timeout", "I/O operation timed out"},
	{regexp.MustCompile(`(?i)InsufficientInstanceCapacity`), "aws_capacity", "Insufficient EC2 instance capacity in region"},
	{regexp.MustCompile(`(?i)VcpuLimitExceeded`), "aws_vcpu_limit", "vCPU limit exceeded in region"},
	{regexp.MustCompile(`(?i)EBS volume limit`), "aws_ebs_limit", "EBS volume limit exceeded"},
	{regexp.MustCompile(`(?i)elastic IP.*limit`), "aws_eip_limit", "Elastic IP address limit exceeded"},
	{regexp.MustCompile(`(?i)pull.*image.*error|ImagePullBackOff`), "image_pull_error", "Failed to pull container image"},
	{regexp.MustCompile(`(?i)certificate.*expired|x509.*expired`), "certificate_expired", "TLS certificate has expired"},
	{regexp.MustCompile(`(?i)etcd.*timeout|etcd.*unavailable`), "etcd_error", "etcd cluster error"},
	{regexp.MustCompile(`(?i)failed to create.*security group`), "aws_sg_error", "Failed to create security group"},
	{regexp.MustCompile(`(?i)failed to create.*load balancer`), "aws_elb_error", "Failed to create load balancer"},
	{regexp.MustCompile(`(?i)failed to create.*NAT gateway`), "aws_nat_error", "Failed to create NAT gateway"},
}

// ParseFailureReason scans the full install log for known failure patterns.
// Returns the first matching failure reason and a human-readable message.
// Returns ("unknown", "Unrecognized failure — check log_tail") if no pattern matches.
func ParseFailureReason(fullLog string) (reason, message string) {
	for _, p := range failurePatterns {
		if p.re.MatchString(fullLog) {
			return p.reason, p.message
		}
	}
	return "unknown", "Unrecognized failure — check log_tail"
}
