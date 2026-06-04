package logpipeline

import "testing"

func TestParseFailure_AWSQuota(t *testing.T) {
	log := `level=error msg="failed to create VPC: exceeded quota for VPCs in us-east-1"`
	reason, message := ParseFailureReason(log)
	if reason != "aws_quota" {
		t.Errorf("reason = %q, want %q", reason, "aws_quota")
	}
	if message == "" {
		t.Error("expected non-empty message")
	}
}

func TestParseFailure_InvalidCredentials(t *testing.T) {
	log := `level=fatal msg="Error: InvalidClientTokenId: The security token included in the request is invalid"`
	reason, _ := ParseFailureReason(log)
	if reason != "invalid_credentials" {
		t.Errorf("reason = %q, want %q", reason, "invalid_credentials")
	}
}

func TestParseFailure_BootstrapTimeout(t *testing.T) {
	log := `level=error msg="bootstrap timeout after 30 minutes waiting for control plane"`
	reason, _ := ParseFailureReason(log)
	if reason != "bootstrap_timeout" {
		t.Errorf("reason = %q, want %q", reason, "bootstrap_timeout")
	}
}

func TestParseFailure_Unknown(t *testing.T) {
	log := `level=error msg="something completely unexpected happened"`
	reason, _ := ParseFailureReason(log)
	if reason != "unknown" {
		t.Errorf("reason = %q, want %q", reason, "unknown")
	}
}

func TestParseFailure_MultilineLog(t *testing.T) {
	log := "level=info msg=\"Starting install\"\nlevel=info msg=\"Creating VPC\"\nlevel=error msg=\"error creating VPC: limit exceeded\"\n"
	reason, _ := ParseFailureReason(log)
	if reason != "aws_vpc_error" && reason != "aws_limit" {
		t.Errorf("reason = %q, want aws_vpc_error or aws_limit", reason)
	}
}
