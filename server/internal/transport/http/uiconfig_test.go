package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestHandleConfig_Unconfigured(t *testing.T) {
	opts := UIConfigOptions{
		UIOrigin:       "http://127.0.0.1:8085",
		OIDCUIClientID: "fleetshift-ui",
		OIDCUIScope:    "openid profile email groups audience:server:client_id:fleetshift",
		Logger:         slog.Default(),
		AuthSnapshot: func(_ context.Context) (string, string, bool, error) {
			return "", "", false, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["uiOrigin"] != "http://127.0.0.1:8085" {
		t.Fatalf("uiOrigin = %v", resp["uiOrigin"])
	}
	if resp["authConfigured"] != false {
		t.Fatalf("authConfigured = %v, want false", resp["authConfigured"])
	}
	oidc, ok := resp["oidc"].(map[string]any)
	if !ok {
		t.Fatalf("oidc type = %T", resp["oidc"])
	}
	if oidc["authority"] != "" ||
		oidc["clientId"] != "fleetshift-ui" ||
		oidc["scope"] != "openid profile email groups audience:server:client_id:fleetshift" {
		t.Fatalf("oidc = %#v", oidc)
	}
	if _, present := oidc["authorizationEndpoint"]; present {
		t.Fatal("unconfigured must omit authorizationEndpoint")
	}
	if _, ok := resp["authentication"]; ok {
		t.Fatal("authentication union must not appear in transitional response")
	}
	if _, ok := resp["schemaVersion"]; ok {
		t.Fatal("schemaVersion must not appear in transitional response")
	}
}

func TestHandleConfig_ConfiguredOIDC(t *testing.T) {
	const uiScope = "openid profile email groups audience:server:client_id:fleetshift"
	opts := UIConfigOptions{
		UIOrigin:       "http://127.0.0.1:8085",
		OIDCUIClientID: "fleetshift-ui",
		OIDCUIScope:    uiScope,
		Logger:         slog.Default(),
		AuthSnapshot: func(_ context.Context) (string, string, bool, error) {
			return "https://127.0.0.1:5556/dex", "https://127.0.0.1:5556/dex/auth", true, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["authConfigured"] != true {
		t.Fatalf("authConfigured = %v, want true", resp["authConfigured"])
	}
	oidc := resp["oidc"].(map[string]any)
	if oidc["authority"] != "https://127.0.0.1:5556/dex" ||
		oidc["authorizationEndpoint"] != "https://127.0.0.1:5556/dex/auth" ||
		oidc["clientId"] != "fleetshift-ui" ||
		oidc["scope"] != uiScope {
		t.Fatalf("oidc = %#v", oidc)
	}
}

func TestHandleConfig_AuthNil_OmitsAuthConfigured(t *testing.T) {
	opts := UIConfigOptions{
		OIDCUIClientID: "fleetshift-ui",
		Logger:         slog.Default(),
		// AuthSnapshot intentionally nil.
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["authConfigured"]; ok {
		t.Error("authConfigured should be omitted when AuthSnapshot callback is nil")
	}
}

func TestHandleConfig_EmptyClientIDAndScopePassThrough(t *testing.T) {
	opts := UIConfigOptions{
		Logger: slog.Default(),
		AuthSnapshot: func(_ context.Context) (string, string, bool, error) {
			return "", "", false, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	oidc := resp["oidc"].(map[string]any)
	if oidc["clientId"] != "" || oidc["scope"] != "" {
		t.Fatalf("server must not invent clientId/scope defaults; oidc=%#v", oidc)
	}
}

func TestHandleConfig_AuthError(t *testing.T) {
	opts := UIConfigOptions{
		UIOrigin: "http://127.0.0.1:8085",
		Logger:   slog.Default(),
		AuthSnapshot: func(_ context.Context) (string, string, bool, error) {
			return "", "", false, errors.New("db down")
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestHandleConfig_ConfiguredIncomplete(t *testing.T) {
	opts := UIConfigOptions{
		UIOrigin: "http://127.0.0.1:8085",
		Logger:   slog.Default(),
		AuthSnapshot: func(_ context.Context) (string, string, bool, error) {
			return "https://issuer.example/dex", "", true, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestHandleConfig_IncludesPluginFields(t *testing.T) {
	dir := t.TempDir()
	registry := `{
		"assetsHost": "",
		"plugins": {
			"core-plugin": {
				"name": "core-plugin",
				"key": "core",
				"label": "Core",
				"persona": "ops",
				"manifestPath": "/plugins/core/plugin-manifest.json",
				"pluginManifest": {
					"name": "core-plugin",
					"version": "1.0.0",
					"extensions": [
						{
							"type": "fleetshift.module",
							"properties": {
								"id": "home",
								"label": "Home",
								"component": {"$codeRef": "HomePage.default"}
							}
						}
					],
					"registrationMethod": "callback",
					"baseURL": "/",
					"loadScripts": ["plugin.js"]
				}
			}
		}
	}`
	if err := os.WriteFile(filepath.Join(dir, "plugin-registry.json"), []byte(registry), 0o600); err != nil {
		t.Fatal(err)
	}

	opts := UIConfigOptions{
		WebDir:         dir,
		UIOrigin:       "http://127.0.0.1:8085",
		OIDCUIClientID: "fleetshift-ui",
		Logger:         slog.Default(),
		AuthSnapshot: func(_ context.Context) (string, string, bool, error) {
			return "", "", false, nil
		},
	}

	handler := handleConfig(opts)
	req := httptest.NewRequest(http.MethodGet, "/api/ui/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"scalprumConfig", "pluginPages", "pluginEntries", "assetsHost"} {
		if _, ok := resp[key]; !ok {
			t.Fatalf("missing plugin bootstrap field %q", key)
		}
	}
}
