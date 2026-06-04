package logpipeline

import "testing"

func TestScrub_AWSAccessKey(t *testing.T) {
	input := "Using credentials AKIAIOSFODNN7EXAMPLE for access"
	got := Scrub(input)
	if got == input {
		t.Error("expected AWS access key to be redacted")
	}
	if containsSubstr(got, "AKIAIOSFODNN7EXAMPLE") {
		t.Errorf("output still contains access key: %s", got)
	}
}

func TestScrub_SecretAccessKey(t *testing.T) {
	input := `secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"`
	got := Scrub(input)
	if containsSubstr(got, "wJalrXUtnFEMI") {
		t.Errorf("output still contains secret key: %s", got)
	}
}

func TestScrub_BearerToken(t *testing.T) {
	input := `Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.something`
	got := Scrub(input)
	if containsSubstr(got, "eyJhbGciOiJSUzI1NiIs") {
		t.Errorf("output still contains bearer token: %s", got)
	}
}

func TestScrub_Password(t *testing.T) {
	input := `password: "my-secret-password-123"`
	got := Scrub(input)
	if containsSubstr(got, "my-secret-password") {
		t.Errorf("output still contains password: %s", got)
	}
}

func TestScrub_PasswordEscapedQuotes(t *testing.T) {
	// Reproduces the exact format from openshift-install log output
	input := `time="2026-04-13T22:08:35Z" level=info msg="Login to the console with user: \"kubeadmin\", and password=\"Q4vHU-G4Rky-K9ZJh-hXYKZ\""`
	got := Scrub(input)
	if containsSubstr(got, "Q4vHU") {
		t.Errorf("output still contains password value: %s", got)
	}
}

func TestScrub_PasswordUnquoted(t *testing.T) {
	input := `password=mysecretvalue123`
	got := Scrub(input)
	if containsSubstr(got, "mysecretvalue") {
		t.Errorf("output still contains password: %s", got)
	}
}

func TestScrub_NoSecrets(t *testing.T) {
	input := `level=info msg="Creating VPC in us-east-1"`
	got := Scrub(input)
	if got != input {
		t.Errorf("clean line was modified: got %q, want %q", got, input)
	}
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
