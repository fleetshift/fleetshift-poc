package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
)

// newAuthEnrollSigningCmd builds the `fleetctl auth enroll-signing` command.
func newAuthEnrollSigningCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enroll-signing",
		Short: "Generate a signing key pair and enroll with the server",
		Long: `Generates an ECDSA P-256 key pair, authenticates via a dedicated OIDC
client to get a purpose-scoped ID token, and submits the token to the
server to create a signer enrollment. The private key is stored in the
OS keyring, or as a file under --config-dir when --insecure-storage is
set. The public key is exported in SSH format for the user to upload
to GitHub as a signing key.

Use --reuse-key to re-enroll with an existing stored key pair
(e.g. after a server-side reset or team change).`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthEnrollSigning(cmd, ctx)
		},
	}
	cmd.Flags().Bool("reuse-key", false, "reuse the stored signing key instead of generating a new one")
	return cmd
}

// runAuthEnrollSigning generates or reuses a signing key, obtains an enrollment
// ID token, and creates a signer enrollment on the server.
func runAuthEnrollSigning(cmd *cobra.Command, ctx *cmdContext) error {
	cfg, err := ctx.flags.loadConfig()
	if err != nil {
		return fmt.Errorf("load auth config (run 'fleetctl auth setup' first): %w", err)
	}
	if cfg.KeyEnrollmentClientID == "" {
		return fmt.Errorf("no key enrollment client ID configured (set --key-enrollment-client-id during 'fleetctl auth setup')")
	}

	reuseKey, _ := cmd.Flags().GetBool("reuse-key")
	privateKey, generated, err := acquireSigningKey(ctx.flags, reuseKey)
	if err != nil {
		return err
	}

	idToken, err := performEnrollmentOIDCFlow(cmd, cfg)
	if err != nil {
		return fmt.Errorf("enrollment OIDC flow: %w", err)
	}

	enrollmentID, err := generateEnrollmentID()
	if err != nil {
		return fmt.Errorf("generate enrollment ID: %w", err)
	}

	client := pb.NewSignerEnrollmentServiceClient(ctx.conn)
	enrollment, err := client.CreateSignerEnrollment(cmd.Context(), &pb.CreateSignerEnrollmentRequest{
		SignerEnrollmentId: enrollmentID,
		IdentityToken:      idToken,
	})
	if err != nil {
		return fmt.Errorf("create signer enrollment: %w", err)
	}

	if generated {
		pemData, err := marshalECPrivateKeyPEM(privateKey)
		if err != nil {
			return fmt.Errorf("marshal private key: %w", err)
		}
		if err := ctx.flags.store().SaveSigningKey(pemData); err != nil {
			return fmt.Errorf("save signing key: %w", err)
		}
	}

	sshPubKey, err := sshPublicKey(&privateKey.PublicKey)
	if err != nil {
		return fmt.Errorf("export SSH public key: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Signer enrolled successfully.\n")
	fmt.Fprintf(cmd.OutOrStdout(), "  Enrollment:       %s\n", enrollment.GetName())
	fmt.Fprintf(cmd.OutOrStdout(), "  Registry subject: %s\n", enrollment.GetRegistrySubject())
	fmt.Fprintf(cmd.OutOrStdout(), "\n")
	fmt.Fprintf(cmd.OutOrStdout(), "Your SSH signing public key (upload to GitHub → Settings → SSH and GPG keys → New SSH key, type \"Signing Key\"):\n\n")
	fmt.Fprintf(cmd.OutOrStdout(), "  %s\n\n", sshPubKey)
	fmt.Fprintf(cmd.OutOrStdout(), "Or visit: https://github.com/settings/ssh/new\n")

	return nil
}

// acquireSigningKey loads an existing key when reuseKey is true, or generates
// a fresh ECDSA P-256 key pair. generated is true when the key was newly
// created and must be persisted.
func acquireSigningKey(f globalFlags, reuseKey bool) (key *ecdsa.PrivateKey, generated bool, err error) {
	if reuseKey {
		key, err = loadSigningPrivateKey(f)
		if err != nil {
			return nil, false, fmt.Errorf("reuse existing signing key: %w", err)
		}
		return key, false, nil
	}
	key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, false, fmt.Errorf("generate key pair: %w", err)
	}
	return key, true, nil
}

// sshPublicKey exports pub in OpenSSH authorized_keys format.
func sshPublicKey(pub *ecdsa.PublicKey) (string, error) {
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return "", err
	}
	return string(ssh.MarshalAuthorizedKey(sshPub)), nil
}

// performEnrollmentOIDCFlow runs the key-enrollment OIDC code flow and returns the ID token.
func performEnrollmentOIDCFlow(cmd *cobra.Command, cfg auth.Config) (string, error) {
	enroll := cfg
	enroll.ClientID = cfg.KeyEnrollmentClientID
	enroll.Scopes = []string{"openid", "profile", "email"}
	tok, err := runLoopbackOIDC(cmd, cfg, oauthConfig(enroll), false, oidcEnrollFailStatus, oidcEnrollSuccessHTML)
	if err != nil {
		return "", err
	}

	idToken, ok := tok.Extra("id_token").(string)
	if !ok || idToken == "" {
		return "", fmt.Errorf("no id_token in token response")
	}
	return idToken, nil
}

// generateEnrollmentID returns a 16-byte hex identifier for a new signer enrollment.
func generateEnrollmentID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// marshalECPrivateKeyPEM encodes key as a PEM "EC PRIVATE KEY" block.
func marshalECPrivateKeyPEM(key *ecdsa.PrivateKey) (string, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", err
	}
	block := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: der,
	}
	return string(pem.EncodeToMemory(block)), nil
}
