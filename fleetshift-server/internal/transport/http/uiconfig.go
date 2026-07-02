package http

import (
	"context"
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
	WebDir         string
	OIDCAuthority  string
	OIDCUIClientID string
	Logger         *slog.Logger
	// AuthMiddleware, when non-nil, wraps routes that serve
	// user-specific data (e.g. /api/ui/user-config → navLayout).
	// Global bootstrap routes (/api/ui/config, /api/ui/plugin-registry)
	// are never wrapped.
	AuthMiddleware func(http.Handler) http.Handler
	// AuthConfigured, when non-nil, is called to check whether at
	// least one OIDC auth method has been configured. The result is
	// surfaced as "authConfigured" in the /api/ui/config response so
	// the frontend can enable auth gating on setup routes even when
	// the user has no token yet.
	AuthConfigured func(ctx context.Context) (bool, error)
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
	Type      string           `json:"type"`
	PageID    string           `json:"pageId,omitempty"`
	GroupID   string           `json:"groupId,omitempty"`
	PluginKey string           `json:"pluginKey,omitempty"`
	Label     string           `json:"label,omitempty"`
	Children  []navLayoutEntry `json:"children,omitempty"`
}

type moduleGroupMeta struct {
	id        string
	label     string
	pluginKey string
}

func NewUIConfigMux(opts UIConfigOptions) *http.ServeMux {
	mux := http.NewServeMux()
	// /api/ui/config must remain unauthenticated — the frontend needs
	// it before the user has logged in (OIDC bootstrap).
	mux.HandleFunc("GET /api/ui/config", handleConfig(opts))
	mux.HandleFunc("GET /api/ui/plugin-registry", handlePluginRegistry(opts))
	if opts.AuthMiddleware != nil {
		mux.Handle("GET /api/ui/user-config", opts.AuthMiddleware(http.HandlerFunc(handleUserConfig(opts))))
	} else {
		mux.HandleFunc("GET /api/ui/user-config", handleUserConfig(opts))
	}
	return mux
}

func handleConfig(opts UIConfigOptions) http.HandlerFunc {
	type oidcConfig struct {
		Authority string `json:"authority"`
		ClientID  string `json:"clientId"`
		Scope     string `json:"scope"`
	}

	oidc := oidcConfig{
		Authority: opts.OIDCAuthority,
		ClientID:  opts.OIDCUIClientID,
		Scope:     "openid profile email roles",
	}

	return func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"oidc": oidc,
		}

		// Surface whether auth has been configured so the frontend
		// can gate setup routes appropriately (e.g. require login
		// when revisiting /setup/auth after auth was configured).
		if opts.AuthConfigured != nil {
			configured, err := opts.AuthConfigured(r.Context())
			if err != nil {
				opts.Logger.ErrorContext(r.Context(), "failed to check auth configuration", "error", err)
				// Non-fatal — omit the field rather than failing
				// the entire config response.
			} else {
				resp["authConfigured"] = configured
			}
		}

		// Augment with plugin-derived global config when webDir is
		// available. These fields are NOT user-specific — scalprum
		// bootstrap, plugin pages, and plugin entries must load
		// regardless of authentication state.
		//
		// Failures are surfaced as errors rather than silently
		// omitted, because the frontend relies on these bootstrap
		// fields (scalprumConfig, pluginPages, pluginEntries).
		if opts.WebDir != "" {
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
			pages := generatePluginPages(registry)
			entries := make([]pluginEntry, 0, len(registry.Plugins))
			for _, e := range registry.Plugins {
				entries = append(entries, e)
			}
			resp["scalprumConfig"] = buildScalprumConfig(registry)
			resp["pluginPages"] = pages
			resp["pluginEntries"] = entries
			resp["assetsHost"] = ""
		}

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

// handleUserConfig returns user-specific configuration only. Currently
// this is just the navigation layout, which will become identity-aware
// (per-user or per-org/group) once those concepts are available.
// Global UI bootstrap data (scalprum, plugin pages, plugin entries) is
// served by /api/ui/config so the frontend can load without auth.
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

		pages := generatePluginPages(registry)
		navLayout := generateNavLayout(registry, pages)

		resp := map[string]interface{}{
			"navLayout": navLayout,
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
			"pluginManifest":   entry.PluginManifest,
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
var safeIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func generatePluginPages(registry pluginRegistry) []pluginPage {
	pages := make([]pluginPage, len(builtinPages))
	copy(pages, builtinPages)
	pathsSeen := make(map[string]bool)
	for _, p := range pages {
		pathsSeen[p.Path] = true
	}

	for _, entry := range registry.Plugins {
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

			id, _ := ext.Properties["id"].(string)
			if id != "" && !safeIDRe.MatchString(id) {
				id = ""
			}

			group, _ := ext.Properties["group"].(string)

			var pagePath string
			if id != "" && group != "" {
				pagePath = fmt.Sprintf("%s/%s", group, id)
			} else if id != "" {
				pagePath = fmt.Sprintf("%s/%s", entry.Key, id)
			} else {
				pagePath = strings.Trim(slugRe.ReplaceAllString(strings.ToLower(label), "-"), "-")
			}
			if pathsSeen[pagePath] {
				continue
			}
			pathsSeen[pagePath] = true

			var pageID string
			if id != "" {
				pageID = fmt.Sprintf("%s.%s", entry.Key, id)
			} else {
				pageID = fmt.Sprintf("%s-%s", entry.Key, strings.ToLower(moduleName))
			}

			pages = append(pages, pluginPage{
				ID:        pageID,
				Title:     label,
				Path:      pagePath,
				Scope:     entry.Name,
				Module:    moduleName,
				PluginKey: entry.Key,
			})
		}
	}

	return pages
}

func collectModuleGroups(registry pluginRegistry) map[string]moduleGroupMeta {
	groups := make(map[string]moduleGroupMeta)
	for _, entry := range registry.Plugins {
		for _, ext := range entry.PluginManifest.Extensions {
			if ext.Type != "fleetshift.module-group" {
				continue
			}
			id, _ := ext.Properties["id"].(string)
			if id == "" {
				continue
			}
			label, _ := ext.Properties["label"].(string)
			groups[id] = moduleGroupMeta{
				id:        id,
				label:     label,
				pluginKey: entry.Key,
			}
		}
	}
	return groups
}

func generateNavLayout(registry pluginRegistry, pages []pluginPage) []navLayoutEntry {
	groups := collectModuleGroups(registry)

	groupChildren := make(map[string][]navLayoutEntry)
	groupedPageIDs := make(map[string]bool)

	for _, entry := range registry.Plugins {
		for _, ext := range entry.PluginManifest.Extensions {
			if ext.Type != "fleetshift.module" {
				continue
			}
			group, _ := ext.Properties["group"].(string)
			if group == "" {
				continue
			}
			id, _ := ext.Properties["id"].(string)
			if id == "" || !safeIDRe.MatchString(id) {
				continue
			}
			pageID := fmt.Sprintf("%s.%s", entry.Key, id)
			groupChildren[group] = append(groupChildren[group], navLayoutEntry{Type: "page", PageID: pageID})
			groupedPageIDs[pageID] = true
		}
	}

	var layout []navLayoutEntry
	emittedGroups := make(map[string]bool)

	for _, p := range pages {
		if p.ID == "orchestration-detail" {
			continue
		}
		if groupedPageIDs[p.ID] {
			parts := strings.SplitN(p.Path, "/", 2)
			groupID := parts[0]
			meta, ok := groups[groupID]
			if !ok {
				layout = append(layout, navLayoutEntry{Type: "page", PageID: p.ID})
				continue
			}
			if emittedGroups[groupID] {
				continue
			}
			emittedGroups[groupID] = true
			layout = append(layout, navLayoutEntry{
				Type:      "group",
				GroupID:   meta.id,
				PluginKey: meta.pluginKey,
				Label:     meta.label,
				Children:  groupChildren[groupID],
			})
			continue
		}
		layout = append(layout, navLayoutEntry{Type: "page", PageID: p.ID})
	}
	return layout
}
