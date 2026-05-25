package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type githubSigningKey struct {
	Key string `json:"key"`
}

// HandleGitHubSigningKeys proxies requests to the GitHub SSH signing
// keys API so the browser can poll without CORS issues.
// GH signing endpoint does check CORS and we are unable to validate the signing in browser
// Route: GET /api/ui/github-signing-keys/{username}
func HandleGitHubSigningKeys(w http.ResponseWriter, r *http.Request) {
	username := r.PathValue("username")
	if username == "" {
		http.Error(w, "missing username", http.StatusBadRequest)
		return
	}

	url := fmt.Sprintf("https://api.github.com/users/%s/ssh_signing_keys", username)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		http.Error(w, "failed to build request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "failed to reach GitHub API", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		http.Error(w, fmt.Sprintf("GitHub user %q not found", username), http.StatusNotFound)
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		http.Error(w, fmt.Sprintf("GitHub API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body))), resp.StatusCode)
		return
	}

	var keys []githubSigningKey
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		http.Error(w, "failed to decode GitHub response", http.StatusInternalServerError)
		return
	}

	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.Key
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
