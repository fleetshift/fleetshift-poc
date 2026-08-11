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

// UIConfigOptions configures UI bootstrap HTTP routes.
type UIConfigOptions struct {
	WebDir string
	// UIOrigin is the trusted advertised browser origin (never from request
	// headers). Emitted as uiOrigin when nonempty.
	// TODO(fe): decide — consume (require uiOrigin == window.location.origin
	// and derive redirect URI as uiOrigin+"/auth/callback") or remove from
	// this contract.
	UIOrigin string
	// OIDCUIClientID and OIDCUIScope are packaging/deploy-supplied browser
	// OIDC inputs advertised on /api/ui/config. The server does not invent
	// defaults for either field.
	OIDCUIClientID string
	OIDCUIScope    string
	Logger         *slog.Logger
	// AuthMiddleware, when non-nil, wraps routes that serve
	// user-specific data (e.g. /api/ui/user-config → navLayout).
	// Global bootstrap routes (/api/ui/config, /api/ui/plugin-registry)
	// are never wrapped.
	AuthMiddleware func(http.Handler) http.Handler
	// AuthSnapshot returns the live browser OIDC snapshot from AuthMethod
	// state. configured=false is unconfigured (empty authority/endpoint).
	// err or configured=true with a partial tuple yields 503. When nil,
	// authConfigured is omitted (frontend treats that as false).
	// authorizationEndpoint is the IdP authorize URL from validated discovery.
	AuthSnapshot func(ctx context.Context) (authority, authorizationEndpoint string, configured bool, err error)
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

// NewUIConfigMux mounts unauthenticated /api/ui/config and
// /api/ui/plugin-registry, plus /api/ui/user-config (optionally auth-wrapped).
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

// oidcConfig is the oidc object on /api/ui/config.
// AuthorizationEndpoint is omitted when unconfigured.
// TODO(fe): decide — consume AuthorizationEndpoint (seed Sign-in without a
// prior discovery fetch) or remove it from this contract.
type oidcConfig struct {
	Authority             string `json:"authority"`
	ClientID              string `json:"clientId"`
	Scope                 string `json:"scope"`
	AuthorizationEndpoint string `json:"authorizationEndpoint,omitempty"`
}

// handleConfig serves GET /api/ui/config in the FE-compatible shape: oidc,
// authConfigured, and plugin bootstrap fields, plus additive uiOrigin and
// oidc.authorizationEndpoint.
func handleConfig(opts UIConfigOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		oidc := oidcConfig{
			ClientID: opts.OIDCUIClientID,
			Scope:    opts.OIDCUIScope,
		}

		resp := map[string]any{
			"oidc": oidc,
		}
		if opts.UIOrigin != "" {
			resp["uiOrigin"] = opts.UIOrigin
		}

		if opts.AuthSnapshot != nil {
			authority, authorizationEndpoint, configured, err := opts.AuthSnapshot(r.Context())
			if err != nil {
				if opts.Logger != nil {
					opts.Logger.ErrorContext(r.Context(), "ui config auth snapshot failed", "error", err)
				}
				http.Error(w, "ui config unavailable", http.StatusServiceUnavailable)
				return
			}
			if configured {
				if authority == "" || authorizationEndpoint == "" {
					if opts.Logger != nil {
						opts.Logger.ErrorContext(r.Context(), "ui config auth snapshot incomplete")
					}
					http.Error(w, "ui config unavailable", http.StatusServiceUnavailable)
					return
				}
				oidc.Authority = authority
				oidc.AuthorizationEndpoint = authorizationEndpoint
			}
			resp["oidc"] = oidc
			resp["authConfigured"] = configured
		}

		// Plugin-derived global config remains on this route because the
		// current frontend loads it here; moving it requires a coordinated
		// frontend change.
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
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			if opts.Logger != nil {
				opts.Logger.ErrorContext(r.Context(), "ui config encode failed", "error", err)
			}
		}
	}
}

// handlePluginRegistry serves the static plugin-registry.json from WebDir.
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
