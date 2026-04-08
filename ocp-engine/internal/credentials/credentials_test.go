package credentials

import (
	"testing"
)

func TestResolve_Inline(t *testing.T) {
	creds := AWSCredentials{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secrettest",
	}
	env, err := Resolve(creds)
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
	creds := AWSCredentials{
		CredentialsFile: "/tmp/aws-credentials",
	}
	env, err := Resolve(creds)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if env["AWS_SHARED_CREDENTIALS_FILE"] != "/tmp/aws-credentials" {
		t.Errorf("AWS_SHARED_CREDENTIALS_FILE = %q, want /tmp/aws-credentials", env["AWS_SHARED_CREDENTIALS_FILE"])
	}
}

func TestResolve_Profile(t *testing.T) {
	creds := AWSCredentials{
		Profile: "my-profile",
	}
	env, err := Resolve(creds)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if env["AWS_PROFILE"] != "my-profile" {
		t.Errorf("AWS_PROFILE = %q, want my-profile", env["AWS_PROFILE"])
	}
}

func TestResolve_NoCredentials(t *testing.T) {
	creds := AWSCredentials{}
	_, err := Resolve(creds)
	if err == nil {
		t.Error("expected error for empty credentials, got nil")
	}
}
