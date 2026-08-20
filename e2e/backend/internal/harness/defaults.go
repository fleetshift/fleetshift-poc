package harness

import "time"

// Fixed harness facts for the Dex-on AIO: product endpoints (no environment-file
// ABI), image/login identity, and start/login deadlines.
const (
	// PublicHost is the AIO TLS-edge DNS name. It is a .localhost name and
	// must resolve to loopback without /etc/hosts.
	PublicHost = "fleetshift-sandbox.localhost"
	// UIPort is the host and container UI/gateway port.
	UIPort = "8085"
	// GRPCPort is the host and container gRPC port.
	GRPCPort = "50051"

	// UIOrigin is the HTTPS origin (UI + Dex under /idp).
	UIOrigin = "https://" + PublicHost + ":" + UIPort
	// GRPCTarget is the plaintext gRPC dial target.
	GRPCTarget = "127.0.0.1:" + GRPCPort
	// Issuer is the bundled-Dex issuer on the single HTTPS origin.
	Issuer = UIOrigin + "/idp"

	// ImageRef is the AIO tag produced by `npx nx run fleetshift-poc:image:aio`.
	ImageRef = "quay.io/stolostron/fleetshift:latest"

	// cliClientID is Dex's public client for fleetctl login.
	cliClientID = "fleetshift-cli"
	// cliScopes includes the server audience so access tokens are accepted on gRPC.
	cliScopes = "openid,profile,email,audience:server:client_id:fleetshift"

	// containerCAPath is the sandbox CA inside the AIO container.
	containerCAPath = "/data/sandbox/pki/ca.crt"
	// labelKey/labelValue mark the suite's podman container.
	labelKey   = "fleetshift.e2e"
	labelValue = "backend"
	// nxImageAIO is the Nx target that builds ImageRef.
	nxImageAIO = "fleetshift-poc:image:aio"
	// credentialsName is the insecure-storage tokens filename under --config-dir.
	credentialsName = "credentials.json"

	readyTimeout             = 30 * time.Second
	copyCATimeout            = 20 * time.Second
	imageBuildTimeout        = 25 * time.Minute
	fleetctlBuildTimeout     = 1 * time.Minute
	playwrightInstallTimeout = 5 * time.Minute
	commandTimeout           = 10 * time.Second
	loginTimeout             = 1 * time.Minute
	grpcAuthTimeout          = 10 * time.Second
	pollInterval             = 200 * time.Millisecond
)
