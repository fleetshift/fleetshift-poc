package credentials

import (
	"fmt"

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
		return nil, fmt.Errorf("no AWS credentials provided: specify access_key_id+secret_access_key, credentials_file, profile, or role_arn")
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
