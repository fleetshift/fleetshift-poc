package credentials

import (
	"context"
	"fmt"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/ocp-engine/internal/config"
)

type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	CredentialsFile string
	Profile         string
	RoleARN         string
}

func Resolve(creds AWSCredentials) (map[string]string, error) {
	env := make(map[string]string)

	switch {
	case creds.AccessKeyID != "" && creds.SecretAccessKey != "":
		env["AWS_ACCESS_KEY_ID"] = creds.AccessKeyID
		env["AWS_SECRET_ACCESS_KEY"] = creds.SecretAccessKey
		if creds.SessionToken != "" {
			env["AWS_SESSION_TOKEN"] = creds.SessionToken
		}
	case creds.CredentialsFile != "":
		env["AWS_SHARED_CREDENTIALS_FILE"] = creds.CredentialsFile
	case creds.Profile != "":
		env["AWS_PROFILE"] = creds.Profile
	case creds.RoleARN != "":
		return nil, fmt.Errorf("STS assume-role (role_arn) is not yet implemented; use access_key_id+secret_access_key, credentials_file, or profile instead")
	default:
		// Fall through to environment variables (set by fleetshift agent via cmd.Env)
		if envKey := os.Getenv("AWS_ACCESS_KEY_ID"); envKey != "" {
			env["AWS_ACCESS_KEY_ID"] = envKey
			env["AWS_SECRET_ACCESS_KEY"] = os.Getenv("AWS_SECRET_ACCESS_KEY")
			if st := os.Getenv("AWS_SESSION_TOKEN"); st != "" {
				env["AWS_SESSION_TOKEN"] = st
			}
		} else {
			return nil, fmt.Errorf("no AWS credentials provided: specify access_key_id+secret_access_key, credentials_file, profile, or role_arn")
		}
	}

	return env, nil
}

// ResolveFromConfig resolves AWS credentials directly from a parsed config.
func ResolveFromConfig(c *config.AWSCredentials) (map[string]string, error) {
	return Resolve(AWSCredentials{
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: c.SecretAccessKey,
		SessionToken:    c.SessionToken,
		CredentialsFile: c.CredentialsFile,
		Profile:         c.Profile,
		RoleARN:         c.RoleARN,
	})
}

// STSIdentity holds the result of a GetCallerIdentity check.
type STSIdentity struct {
	Account string
	ARN     string
}

// TestCredentials validates AWS credentials by calling STS GetCallerIdentity.
// awsEnv is the resolved credential env vars. region is the target AWS region.
func TestCredentials(awsEnv map[string]string, region string) (*STSIdentity, error) {
	ctx := context.Background()

	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))

	// Apply credential env vars
	if keyID, ok := awsEnv["AWS_ACCESS_KEY_ID"]; ok {
		secretKey := awsEnv["AWS_SECRET_ACCESS_KEY"]
		sessionToken := awsEnv["AWS_SESSION_TOKEN"]
		opts = append(opts, awsconfig.WithCredentialsProvider(
			awscreds.NewStaticCredentialsProvider(keyID, secretKey, sessionToken),
		))
	} else if profile, ok := awsEnv["AWS_PROFILE"]; ok {
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	} else if credsFile, ok := awsEnv["AWS_SHARED_CREDENTIALS_FILE"]; ok {
		os.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsFile)
		defer os.Unsetenv("AWS_SHARED_CREDENTIALS_FILE")
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := sts.NewFromConfig(cfg)
	result, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("AWS credentials invalid: %w", err)
	}

	return &STSIdentity{
		Account: derefString(result.Account),
		ARN:     derefString(result.Arn),
	}, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
