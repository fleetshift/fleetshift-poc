// Package aioinit holds OCM Fleet AIO packaging helpers: public origin and
// listen addresses, sandbox PKI, peer Dex config, serve argv,
// and kind networking.
package aioinit

import (
	"net"
	"os"
	"strings"
)

const (
	// PublicHost is the browser-facing DNS name.
	PublicHost = "ocm-fleet.localhost"
	// HostsMarker tags (comments) the AIO-container /etc/hosts mapping.
	HostsMarker = "ocm-fleet-aio"

	gatewayPort = "8085"
	httpPort    = "8086"
	dexPort     = "5556"
	grpcPort    = "50051"

	// PublicEnvPath is sourced by the resource-manager s6 run script for the public
	// origin and Dex discovery URL.
	PublicEnvPath = "/run/ocm-fleet/public.env"
)

// publicOrigin is the browser-facing https URL for PublicHost:gatewayPort.
func publicOrigin() string {
	return "https://" + net.JoinHostPort(PublicHost, gatewayPort)
}

// PeerDexIssuer is the public peer-Dex issuer (Dex-on only). Scheme, host,
// port, and path must match discovery issuer and ID-token iss exactly.
var PeerDexIssuer = publicOrigin() + "/idp"

// Endpoints is the AIO public origin plus internal listen addresses.
// PublicOrigin is the browser-facing URL. HTTPListen and DexListen are the
// container-loopback listen addresses for the resource manager server and Dex.
type Endpoints struct {
	PublicOrigin  string
	UICallback    string
	SilentRenew   string
	GatewayListen string
	HTTPListen    string
	DexListen     string
	GRPCListen    string
}

// FixedEndpoints is the AIO endpoint set, derived from PublicHost and the
// fixed listen ports.
var FixedEndpoints = Endpoints{
	PublicOrigin:  publicOrigin(),
	UICallback:    publicOrigin() + "/app/auth/callback",
	SilentRenew:   publicOrigin() + "/app/silent-renew.html",
	GatewayListen: ":" + gatewayPort,
	HTTPListen:    net.JoinHostPort("127.0.0.1", httpPort),
	DexListen:     net.JoinHostPort("127.0.0.1", dexPort),
	GRPCListen:    ":" + grpcPort,
}

// WritePublicEnv writes PUBLIC_HOST and PUBLIC_ORIGIN to path. On Dex-on it
// also writes DEX_DISCOVERY_URL for the resource-manager s6 Dex-ready probe.
func WritePublicEnv(path string, dexOn bool) error {
	var b strings.Builder
	b.WriteString("PUBLIC_HOST=" + PublicHost + "\n")
	b.WriteString("PUBLIC_ORIGIN=")
	b.WriteString(FixedEndpoints.PublicOrigin)
	b.WriteString("\n")
	if dexOn {
		b.WriteString("DEX_DISCOVERY_URL=")
		b.WriteString(PeerDexIssuer)
		b.WriteString("/.well-known/openid-configuration\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0644)
}
