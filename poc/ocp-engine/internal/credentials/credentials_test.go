package credentials

import (
	"testing"

	"github.com/ocp-engine/internal/config"
)

func TestResolve_Inline(t *testing.T) {
	env, err := Resolve(AWSCredentials{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secrettest",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if env["AWS_ACCESS_KEY_ID"] != "AKIATEST" {
		t.Errorf("AWS_ACCESS_KEY_ID = %q, want AKIATEST", env["AWS_ACCESS_KEY_ID"])
	}
	if env["AWS_SECRET_ACCESS_KEY"] != "secrettest" {
		t.Errorf("AWS_SECRET_ACCESS_KEY = %q, want secrettest", env["AWS_SECRET_ACCESS_KEY"])
	}
}

func TestResolve_File(t *testing.T) {
	env, err := Resolve(AWSCredentials{
		CredentialsFile: "/tmp/aws-credentials",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if env["AWS_SHARED_CREDENTIALS_FILE"] != "/tmp/aws-credentials" {
		t.Errorf("AWS_SHARED_CREDENTIALS_FILE = %q, want /tmp/aws-credentials", env["AWS_SHARED_CREDENTIALS_FILE"])
	}
}

func TestResolve_Profile(t *testing.T) {
	env, err := Resolve(AWSCredentials{
		Profile: "my-profile",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if env["AWS_PROFILE"] != "my-profile" {
		t.Errorf("AWS_PROFILE = %q, want my-profile", env["AWS_PROFILE"])
	}
}

func TestResolve_NoCredentials(t *testing.T) {
	_, err := Resolve(AWSCredentials{})
	if err == nil {
		t.Error("expected error for empty credentials, got nil")
	}
}

func TestResolve_RoleARN_NotImplemented(t *testing.T) {
	_, err := Resolve(AWSCredentials{
		RoleARN: "arn:aws:iam::123456789:role/test",
	})
	if err == nil {
		t.Error("expected error for role_arn (not yet implemented)")
	}
}

func TestResolveFromConfig(t *testing.T) {
	env, err := ResolveFromConfig(&config.AWSCredentials{
		AccessKeyID:     "AKIACONFIG",
		SecretAccessKey: "secretconfig",
	})
	if err != nil {
		t.Fatalf("ResolveFromConfig: %v", err)
	}
	if env["AWS_ACCESS_KEY_ID"] != "AKIACONFIG" {
		t.Errorf("AWS_ACCESS_KEY_ID = %q, want AKIACONFIG", env["AWS_ACCESS_KEY_ID"])
	}
}
