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
	return out, nil
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

