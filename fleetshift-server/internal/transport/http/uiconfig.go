package http

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type UIConfigOptions struct {
	WebDir          string
	OIDCAuthority   string
	OIDCUIClientID  string
	Logger          *slog.Logger
}

type pluginManifest struct {
	Name               string      `json:"name"`
	Version            string      `json:"version"`
	Extensions         []extension `json:"extensions"`
	RegistrationMethod string      `json:"registrationMethod"`
	BaseURL            string      `json:"baseURL"`
	LoadScripts        []string    `json:"loadScripts"`
}

type extension struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type pluginEntry struct {
	Name           string         `json:"name"`
	Key            string         `json:"key"`
	Label          string         `json:"label"`
	Persona        string         `json:"persona"`
	ManifestPath   string         `json:"manifestPath"`
	PluginManifest pluginManifest `json:"pluginManifest"`
}

type pluginRegistry struct {
	AssetsHost string                 `json:"assetsHost"`
	Plugins    map[string]pluginEntry `json:"plugins"`
}

type pluginPage struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Path      string `json:"path"`
	Scope     string `json:"scope"`
	Module    string `json:"module"`
	PluginKey string `json:"pluginKey"`
}

type navLayoutEntry struct {
	Type   string `json:"type"`
	PageID string `json:"pageId"`
}

func NewUIConfigMux(opts UIConfigOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/ui/config", handleConfig(opts))
	mux.HandleFunc("GET /api/ui/plugin-registry", handlePluginRegistry(opts))
	mux.HandleFunc("GET /api/ui/user-config", handleUserConfig(opts))
	return mux
}

func handleConfig(opts UIConfigOptions) http.HandlerFunc {
	type oidcConfig struct {
		Authority string `json:"authority"`
		ClientID  string `json:"clientId"`
		Scope     string `json:"scope"`
	}
	type configResponse struct {
		OIDC oidcConfig `json:"oidc"`
	}

	resp := configResponse{
		OIDC: oidcConfig{
			Authority: opts.OIDCAuthority,
			ClientID:  opts.OIDCUIClientID,
			Scope:     "openid profile email roles",
		},
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func handlePluginRegistry(opts UIConfigOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join(opts.WebDir, "plugin-registry.json")
		data, err := os.ReadFile(path)
		if err != nil {
			opts.Logger.Error("failed to read plugin-registry.json", "error", err)
			http.Error(w, "plugin registry not available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

func handleUserConfig(opts UIConfigOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join(opts.WebDir, "plugin-registry.json")
		data, err := os.ReadFile(path)
		if err != nil {
			opts.Logger.Error("failed to read plugin-registry.json", "error", err)
			http.Error(w, "plugin registry not available", http.StatusServiceUnavailable)
			return
		}

		var registry pluginRegistry
		if err := json.Unmarshal(data, &registry); err != nil {
			opts.Logger.Error("failed to parse plugin-registry.json", "error", err)
			http.Error(w, "invalid plugin registry", http.StatusInternalServerError)
			return
		}

		scalprumConfig := buildScalprumConfig(registry)
		pages := generatePluginPages(registry)
		navLayout := generateNavLayout(pages)
		entries := make([]pluginEntry, 0, len(registry.Plugins))
		for _, e := range registry.Plugins {
			entries = append(entries, e)
		}

		resp := map[string]interface{}{
			"scalprumConfig": scalprumConfig,
			"pluginPages":    pages,
			"navLayout":      navLayout,
			"pluginEntries":  entries,
			"assetsHost":     "",
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func buildScalprumConfig(registry pluginRegistry) map[string]interface{} {
	config := make(map[string]interface{})

	for name, entry := range registry.Plugins {
		cfg := map[string]interface{}{
			"name":             entry.Name,
			"manifestLocation": entry.ManifestPath,
		}
		if entry.Key == "management" {
			cfg["pluginManifest"] = entry.PluginManifest
		}
		config[name] = cfg
	}

	return config
}

var builtinPages = []pluginPage{
	{
		ID:        "orchestration-detail",
		Title:     "Orchestration Detail",
		Path:      "orchestration/:deploymentId",
		Scope:     "management-plugin",
		Module:    "DeploymentDetailPage",
		PluginKey: "management",
	},
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func generatePluginPages(registry pluginRegistry) []pluginPage {
	pages := make([]pluginPage, len(builtinPages))
	copy(pages, builtinPages)
	pathsSeen := make(map[string]bool)
	for _, p := range pages {
		pathsSeen[p.Path] = true
	}

	for _, entry := range registry.Plugins {
		if entry.Key != "management" {
			continue
		}

		for _, ext := range entry.PluginManifest.Extensions {
			if ext.Type != "fleetshift.module" {
				continue
			}

			label := entry.Label
			if l, ok := ext.Properties["label"].(string); ok && l != "" {
				label = l
			}

			var moduleName string
			if comp, ok := ext.Properties["component"].(map[string]interface{}); ok {
				if codeRef, ok := comp["$codeRef"].(string); ok {
					parts := strings.SplitN(codeRef, ".", 2)
					moduleName = parts[0]
				}
			}

			slug := strings.Trim(slugRe.ReplaceAllString(strings.ToLower(label), "-"), "-")
			if pathsSeen[slug] {
				continue
			}
			pathsSeen[slug] = true

			pageID := fmt.Sprintf("%s-%s", entry.Key, strings.ToLower(moduleName))

			pages = append(pages, pluginPage{
				ID:        pageID,
				Title:     label,
				Path:      slug,
				Scope:     entry.Name,
				Module:    moduleName,
				PluginKey: entry.Key,
			})
		}
	}

	return pages
}

func generateNavLayout(pages []pluginPage) []navLayoutEntry {
	var layout []navLayoutEntry
	for _, p := range pages {
		if p.ID == "orchestration-detail" {
			continue
		}
		layout = append(layout, navLayoutEntry{Type: "page", PageID: p.ID})
	}
	return layout
}
