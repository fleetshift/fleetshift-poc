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
	WebDir         string
	OIDCAuthority  string
	OIDCUIClientID string
	Logger         *slog.Logger
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
		navLayout := generateNavLayout(registry, pages)
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
