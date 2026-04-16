// Package keyregistry implements external key registry clients.
package keyregistry

import (
	"context"
	"crypto"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// GitHubClient fetches SSH signing keys from the GitHub API.
// Unsupported key types are silently skipped.
type GitHubClient struct {
	HTTP *http.Client
}

type githubSSHKey struct {
	Key string `json:"key"` // "ecdsa-sha2-nistp256 AAAA..."
}

func (c *GitHubClient) FetchSigningKeys(ctx context.Context, endpoint string, subject domain.RegistrySubject) ([]crypto.PublicKey, error) {
	url := fmt.Sprintf("%s/users/%s/ssh_signing_keys", strings.TrimRight(endpoint, "/"), string(subject))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch signing keys: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("github user %q not found", subject)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("github API returned %d: %s", resp.StatusCode, body)
	}

	var keys []githubSSHKey
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	var out []crypto.PublicKey
	for _, k := range keys {
		pub, err := parseSSHPublicKey(k.Key)
		if err != nil {
			continue
		}
		out = append(out, pub)
	}

	if len(out) == 0 {
		// Check if the user has authentication keys — common mistake
		// is adding as "Authentication Key" instead of "Signing Key".
		if hasAuthKeys, _ := c.hasAuthenticationKeys(ctx, endpoint, subject); hasAuthKeys {
			return nil, fmt.Errorf(
				"github user %q has SSH authentication keys but no signing keys — "+
					"the key was likely added as an Authentication Key instead of a Signing Key; "+
					"go to https://github.com/settings/keys, remove it, and re-add with Key type set to \"Signing Key\"",
				subject)
		}
	}

	return out, nil
}

// hasAuthenticationKeys checks whether the GitHub user has any SSH
// authentication keys via GET /users/{username}/keys. This is used
// to produce a better error message when a user mistakenly adds
// their key as an authentication key instead of a signing key.
func (c *GitHubClient) hasAuthenticationKeys(ctx context.Context, endpoint string, subject domain.RegistrySubject) (bool, error) {
	url := fmt.Sprintf("%s/users/%s/keys", strings.TrimRight(endpoint, "/"), string(subject))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, nil
	}

	var keys []githubSSHKey
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return false, err
	}
	return len(keys) > 0, nil
}

// parseSSHPublicKey extracts a [crypto.PublicKey] from an SSH
// authorized-key line (e.g. "ecdsa-sha2-nistp256 AAAA...").
func parseSSHPublicKey(line string) (crypto.PublicKey, error) {
	sshPub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		return nil, fmt.Errorf("parse SSH authorized key: %w", err)
	}
	cpk, ok := sshPub.(ssh.CryptoPublicKey)
	if !ok {
		return nil, fmt.Errorf("SSH key type %s does not expose a crypto.PublicKey", sshPub.Type())
	}
	return cpk.CryptoPublicKey(), nil
}

