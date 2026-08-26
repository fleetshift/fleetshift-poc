package harness

import "time"

// Fixed harness facts for the Kind-capable Dex-on AIO: product endpoints (no
// environment-file ABI), image/login identity, and start/login deadlines.
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
	// prebuiltAIOEnv skips nx image:aio when set to 1 and the tag is loaded (CI).
	prebuiltAIOEnv = "FLEETSHIFT_E2E_AIO_PREBUILT"

	// cliClientID is Dex's public client for fleetctl login.
	cliClientID = "fleetshift-cli"
	// cliScopes includes the server audience so access tokens are accepted on gRPC.
	cliScopes = "openid,profile,email,audience:server:client_id:fleetshift"

	// containerCAPath is the sandbox CA inside the AIO container.
	containerCAPath = "/data/sandbox/pki/ca.crt"
	// labelKey/labelValue mark the suite's podman container.
	labelKey   = "fleetshift.e2e"
	labelValue = "backend"
	// kindNetwork is the shared Kind/AIO podman network (alias fleetshift).
	kindNetwork = "kind"
	// kindNetworkAlias is the AIO's name on kindNetwork for loopback-forward.
	kindNetworkAlias = "fleetshift"
	// engineSocketEnv is the optional absolute host engine socket path.
	engineSocketEnv = "PODMAN_SOCKET"
	// dockerCompatSocket is the Docker / podman-mac-helper well-known socket.
	dockerCompatSocket = "/var/run/docker.sock"
	// containerEngineSocket is the in-AIO mount of the host engine socket.
	containerEngineSocket = "/var/run/docker.sock"
	// kindClusterLabel is the Kind node container label for cluster name.
	kindClusterLabel = "io.x-k8s.kind.cluster"
	// kindRoleLabel is the Kind node container label for node role.
	kindRoleLabel = "io.x-k8s.kind.role"
	// kindControlPlaneRole is the Kind control-plane node role.
	kindControlPlaneRole = "control-plane"
	// KindClusterIDPrefix is the fleetctl Kind cluster id prefix for this suite.
	KindClusterIDPrefix = "kind-e2e-"
	// kindClusterNamePrefix is the Kind addon's host Kind/podman name prefix.
	kindClusterNamePrefix = "fs--"
	// suiteHostKindPrefix is the host Kind name prefix for leftover nodes this suite may create.
	suiteHostKindPrefix = kindClusterNamePrefix + KindClusterIDPrefix
	// nxImageAIO is the Nx target that builds ImageRef.
	nxImageAIO = "fleetshift-poc:image:aio"
	// credentialsName is the insecure-storage tokens filename under --config-dir.
	credentialsName = "credentials.json"
	// authConfigName is the OIDC client settings filename under --config-dir.
	authConfigName = "auth.json"

	// PersonaOps and PersonaDev are the bundled Dex sandbox personas.
	PersonaOps = "ops"
	PersonaDev = "dev"

	readyTimeout             = 30 * time.Second
	copyCATimeout            = 20 * time.Second
	smokeKindTimeout         = 30 * time.Second
	podmanRunTimeout         = 30 * time.Second
	imageBuildTimeout        = 25 * time.Minute
	fleetctlBuildTimeout     = 1 * time.Minute
	playwrightInstallTimeout = 5 * time.Minute
	commandTimeout           = 10 * time.Second
	loginTimeout             = 1 * time.Minute
	grpcAuthTimeout          = 10 * time.Second
	// grpcProbeTimeout is the per-attempt fleetctl deadline inside
	// requireUnauthenticatedRPC. It must stay shorter than grpcAuthTimeout
	// so a hung first RPC cannot consume the whole poll budget.
	grpcProbeTimeout        = 2 * time.Second
	pollInterval            = 200 * time.Millisecond
	engineSocketDialTimeout = 2 * time.Second
	// pollLogInterval is how often identical wait-loop messages are logged.
	pollLogInterval = 3 * time.Second
	// commandHeartbeatInterval is how often a still-running quiet command
	// (image build, and similar) repeats its progress line.
	commandHeartbeatInterval = 15 * time.Second
)
