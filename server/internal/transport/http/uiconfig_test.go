package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGeneratePluginPages_GroupedModulePath(t *testing.T) {
	registry := pluginRegistry{
		Plugins: map[string]pluginEntry{
			"settings-plugin": {
				Name:  "settings-plugin",
				Key:   "settings",
				Label: "Settings",
				PluginManifest: pluginManifest{
					Extensions: []extension{
						{
							Type: "fleetshift.module-group",
							Properties: map[string]interface{}{
								"id":    "settings",
								"label": "Settings",
							},
						},
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "navigation",
								"label":     "Navigation",
								"group":     "settings",
								"component": map[string]interface{}{"$codeRef": "NavPage.default"},
							},
						},
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "auth",
								"label":     "Authentication",
								"group":     "settings",
								"component": map[string]interface{}{"$codeRef": "AuthPage.default"},
							},
						},
					},
				},
			},
		},
	}

	pages := generatePluginPages(registry)

	var navPage, authPage *pluginPage
	for i := range pages {
		switch pages[i].ID {
		case "settings.navigation":
			navPage = &pages[i]
		case "settings.auth":
			authPage = &pages[i]
		}
	}

	if navPage == nil {
		t.Fatal("expected settings.navigation page")
	}
	if navPage.Path != "settings/navigation" {
		t.Errorf("grouped module path: got %q, want %q", navPage.Path, "settings/navigation")
	}

	if authPage == nil {
		t.Fatal("expected settings.auth page")
	}
	if authPage.Path != "settings/auth" {
		t.Errorf("grouped module path: got %q, want %q", authPage.Path, "settings/auth")
	}
}

func TestGeneratePluginPages_UngroupedModulePath(t *testing.T) {
	registry := pluginRegistry{
		Plugins: map[string]pluginEntry{
			"core-plugin": {
				Name:  "core-plugin",
				Key:   "core",
				Label: "Core",
				PluginManifest: pluginManifest{
					Extensions: []extension{
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "clusters",
								"label":     "Clusters",
								"component": map[string]interface{}{"$codeRef": "ClustersPage.default"},
							},
						},
					},
				},
			},
		},
	}

	pages := generatePluginPages(registry)

	var clusterPage *pluginPage
	for i := range pages {
		if pages[i].ID == "core.clusters" {
			clusterPage = &pages[i]
		}
	}

	if clusterPage == nil {
		t.Fatal("expected core.clusters page")
	}
	if clusterPage.Path != "core/clusters" {
		t.Errorf("ungrouped module path: got %q, want %q", clusterPage.Path, "core/clusters")
	}
}

func TestGenerateNavLayout_NestedGroups(t *testing.T) {
	registry := pluginRegistry{
		Plugins: map[string]pluginEntry{
			"settings-plugin": {
				Name:  "settings-plugin",
				Key:   "settings",
				Label: "Settings",
				PluginManifest: pluginManifest{
					Extensions: []extension{
						{
							Type: "fleetshift.module-group",
							Properties: map[string]interface{}{
								"id":    "settings",
								"label": "Settings",
							},
						},
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "navigation",
								"label":     "Navigation",
								"group":     "settings",
								"component": map[string]interface{}{"$codeRef": "NavPage.default"},
							},
						},
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "auth",
								"label":     "Authentication",
								"group":     "settings",
								"component": map[string]interface{}{"$codeRef": "AuthPage.default"},
							},
						},
					},
				},
			},
			"core-plugin": {
				Name:  "core-plugin",
				Key:   "core",
				Label: "Core",
				PluginManifest: pluginManifest{
					Extensions: []extension{
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "clusters",
								"label":     "Clusters",
								"component": map[string]interface{}{"$codeRef": "ClustersPage.default"},
							},
						},
					},
				},
			},
		},
	}

	pages := generatePluginPages(registry)
	layout := generateNavLayout(registry, pages)

	var groupEntry *navLayoutEntry
	var flatPages []navLayoutEntry
	for i := range layout {
		if layout[i].Type == "group" {
			groupEntry = &layout[i]
		} else if layout[i].Type == "page" {
			flatPages = append(flatPages, layout[i])
		}
	}

	if groupEntry == nil {
		t.Fatal("expected a group entry in navLayout")
	}
	if groupEntry.GroupID != "settings" {
		t.Errorf("group id: got %q, want %q", groupEntry.GroupID, "settings")
	}
	if groupEntry.Label != "Settings" {
		t.Errorf("group label: got %q, want %q", groupEntry.Label, "Settings")
	}
	if len(groupEntry.Children) != 2 {
		t.Fatalf("group children count: got %d, want 2", len(groupEntry.Children))
	}
	if groupEntry.Children[0].PageID != "settings.navigation" {
		t.Errorf("first child: got %q, want %q", groupEntry.Children[0].PageID, "settings.navigation")
	}
	if groupEntry.Children[1].PageID != "settings.auth" {
		t.Errorf("second child: got %q, want %q", groupEntry.Children[1].PageID, "settings.auth")
	}

	foundClusters := false
	for _, p := range flatPages {
		if p.PageID == "core.clusters" {
			foundClusters = true
		}
		if p.PageID == "settings.navigation" || p.PageID == "settings.auth" {
			t.Errorf("grouped page %q should not appear as top-level", p.PageID)
		}
	}
	if !foundClusters {
		t.Error("expected core.clusters as a top-level page entry")
	}
}

func TestGenerateNavLayout_NoGroups(t *testing.T) {
	registry := pluginRegistry{
		Plugins: map[string]pluginEntry{
			"core-plugin": {
				Name:  "core-plugin",
				Key:   "core",
				Label: "Core",
				PluginManifest: pluginManifest{
					Extensions: []extension{
						{
							Type: "fleetshift.module",
							Properties: map[string]interface{}{
								"id":        "clusters",
								"label":     "Clusters",
								"component": map[string]interface{}{"$codeRef": "ClustersPage.default"},
							},
						},
					},
				},
			},
		},
	}

	pages := generatePluginPages(registry)
	layout := generateNavLayout(registry, pages)

	for _, entry := range layout {
		if entry.Type == "group" {
			t.Error("expected no group entries when no module-group extensions exist")
		}
	}
}

func TestHandleConfig_AuthConfiguredTrue(t *testing.T) {
	opts := UIConfigOptions{
		OIDCAuthority:  "https://keycloak.example.com/realms/test",
		OIDCUIClientID: "ui-client",
		Logger:         slog.Default(),
		AuthConfigured: func(_ context.Context) (bool, error) {
			return true, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	val, ok := resp["authConfigured"]
	if !ok {
		t.Fatal("authConfigured field missing from response")
	}
	if val != true {
		t.Errorf("authConfigured = %v, want true", val)
	}
}

func TestHandleConfig_AuthConfiguredFalse(t *testing.T) {
	opts := UIConfigOptions{
		OIDCAuthority:  "https://keycloak.example.com/realms/test",
		OIDCUIClientID: "ui-client",
		Logger:         slog.Default(),
		AuthConfigured: func(_ context.Context) (bool, error) {
			return false, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	val, ok := resp["authConfigured"]
	if !ok {
		t.Fatal("authConfigured field missing from response")
	}
	if val != false {
		t.Errorf("authConfigured = %v, want false", val)
	}
}

func TestHandleConfig_AuthConfiguredNil_OmitsField(t *testing.T) {
	opts := UIConfigOptions{
		OIDCAuthority:  "https://keycloak.example.com/realms/test",
		OIDCUIClientID: "ui-client",
		Logger:         slog.Default(),
		// AuthConfigured intentionally nil.
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["authConfigured"]; ok {
		t.Error("authConfigured should be omitted when AuthConfigured callback is nil")
	}
}

func TestHandleConfig_AuthConfiguredError_OmitsField(t *testing.T) {
	opts := UIConfigOptions{
		OIDCAuthority:  "https://keycloak.example.com/realms/test",
		OIDCUIClientID: "ui-client",
		Logger:         slog.Default(),
		AuthConfigured: func(_ context.Context) (bool, error) {
			return false, errors.New("db down")
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Should still return 200 — authConfigured error is non-fatal.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["authConfigured"]; ok {
		t.Error("authConfigured should be omitted on error (non-fatal)")
	}
}
