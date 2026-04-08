package credentials

import "fmt"

type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
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
	case creds.CredentialsFile != "":
		env["AWS_SHARED_CREDENTIALS_FILE"] = creds.CredentialsFile
	case creds.Profile != "":
		env["AWS_PROFILE"] = creds.Profile
	case creds.RoleARN != "":
		env["OCP_ENGINE_ROLE_ARN"] = creds.RoleARN
	default:
		return nil, fmt.Errorf("no AWS credentials provided: specify access_key_id+secret_access_key, credentials_file, profile, or role_arn")
	}

	return env, nil
}
