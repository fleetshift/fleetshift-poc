package kubernetes

import (
	"io"
	"log/slog"
	"testing"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func res(group, resource string) Resource {
	return Resource{ApiGroups: []string{group}, Resources: []string{resource}}
}

// --- Watch-all mode (no allow list) ---

func TestIsResourceAllowed_WatchAll_NoLists(t *testing.T) {
	if !IsResourceAllowed("apps", "deployments", nil, nil, nil, discardLogger) {
		t.Error("expected allowed: no lists configured")
	}
}

func TestIsResourceAllowed_WatchAll_DefaultDenyBlocks(t *testing.T) {
	defaultDeny := []Resource{res("", "events")}
	if IsResourceAllowed("", "events", nil, nil, defaultDeny, discardLogger) {
		t.Error("expected denied: in default deny")
	}
}

func TestIsResourceAllowed_WatchAll_UserDenyBlocks(t *testing.T) {
	userDeny := []Resource{res("apps", "deployments")}
	if IsResourceAllowed("apps", "deployments", nil, userDeny, nil, discardLogger) {
		t.Error("expected denied: in user deny")
	}
}

func TestIsResourceAllowed_WatchAll_UserDenyTakesPrecedenceOverDefaultDeny(t *testing.T) {
	userDeny := []Resource{res("apps", "deployments")}
	defaultDeny := []Resource{res("apps", "deployments")}
	if IsResourceAllowed("apps", "deployments", nil, userDeny, defaultDeny, discardLogger) {
		t.Error("expected denied: user deny takes precedence")
	}
}

// --- Watch-selected mode (allow list present) ---

func TestIsResourceAllowed_WatchSelected_AllowedResource(t *testing.T) {
	allow := []Resource{res("apps", "deployments")}
	if !IsResourceAllowed("apps", "deployments", allow, nil, nil, discardLogger) {
		t.Error("expected allowed: in allow list")
	}
}

func TestIsResourceAllowed_WatchSelected_NotInAllowDenied(t *testing.T) {
	allow := []Resource{res("apps", "deployments")}
	if IsResourceAllowed("", "pods", allow, nil, nil, discardLogger) {
		t.Error("expected denied: not in allow list")
	}
}

func TestIsResourceAllowed_WatchSelected_UserDenyBeatsAllow(t *testing.T) {
	allow := []Resource{res("apps", "deployments")}
	userDeny := []Resource{res("apps", "deployments")}
	if IsResourceAllowed("apps", "deployments", allow, userDeny, nil, discardLogger) {
		t.Error("expected denied: user deny beats allow")
	}
}

// This is the bug: user allow should override default deny.
func TestIsResourceAllowed_WatchSelected_UserAllowOverridesDefaultDeny(t *testing.T) {
	allow := []Resource{res("", "endpoints")}
	defaultDeny := []Resource{res("", "endpoints")}
	if !IsResourceAllowed("", "endpoints", allow, nil, defaultDeny, discardLogger) {
		t.Error("expected allowed: user allow overrides default deny")
	}
}

func TestIsResourceAllowed_WatchSelected_UserDenyBeatsAllowEvenIfDefaultDenied(t *testing.T) {
	allow := []Resource{res("", "endpoints")}
	userDeny := []Resource{res("", "endpoints")}
	defaultDeny := []Resource{res("", "endpoints")}
	if IsResourceAllowed("", "endpoints", allow, userDeny, defaultDeny, discardLogger) {
		t.Error("expected denied: user deny always wins, even over user allow")
	}
}

// --- Wildcard behavior ---

func TestIsResourceAllowed_WildcardDeny(t *testing.T) {
	userDeny := []Resource{res("*", "secrets")}
	if IsResourceAllowed("", "secrets", nil, userDeny, nil, discardLogger) {
		t.Error("expected denied: wildcard group matches core group")
	}
}

func TestIsResourceAllowed_WildcardAllow_OverridesDefaultDeny(t *testing.T) {
	allow := []Resource{res("", "*")}
	defaultDeny := []Resource{res("", "endpoints")}
	if !IsResourceAllowed("", "endpoints", allow, nil, defaultDeny, discardLogger) {
		t.Error("expected allowed: wildcard allow overrides specific default deny")
	}
}
