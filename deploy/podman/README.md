# FleetShift Podman Deployment

Step-by-step guide to deploy the full FleetShift stack using podman containers.

## Prerequisites

### 1. Podman installed and running

```bash
# Verify podman is installed
podman --version

# Verify podman machine is running (macOS only — podman runs in a VM on Mac)
podman machine list

# If no machine exists or it's not running:
podman machine init
podman machine start
```

### 2. /etc/hosts entry for Keycloak

Keycloak needs to be resolvable by both the browser and containers. Add this to `/etc/hosts`:

```bash
# Check if already present
grep keycloak /etc/hosts

# If not, add it (requires sudo)
sudo sh -c 'echo "127.0.0.1 keycloak" >> /etc/hosts'
```

### 3. Both repos cloned side by side

```
/Repos/
├── fleetshift-poc_mshort/           # This repo
└── fleetshift-user-interface_mshort/ # UI repo
```

### 4. Logged in to quay.io (only needed for push, not for local testing)

```bash
podman login quay.io
# Username: rheemshort
# Password: <your quay.io token>
```

---

## Step 1: Build All Container Images

### Build the FleetShift server image

```bash
cd /Repos/fleetshift-poc_mshort

# Build the server image (uses existing Dockerfile)
make image-build IMAGE_TAG=0.1.0

# Verify it built
podman images | grep fleetshift-server
# Expected: quay.io/rheemshort/fleetshift-server   0.1.0   <hash>   <time>   <size>
```

This builds the Go backend with `fleetshift` and `fleetctl` binaries, plus docker.io, kind, and kubectl in the runtime image. Expect this to take 2-5 minutes on first build.

### Build the UI images

```bash
cd /Repos/fleetshift-user-interface_mshort

# Build all three UI images at once
make image-build-all IMAGE_TAG=0.1.0

# Or build individually if you want to watch progress:
make image-build-mock-servers IMAGE_TAG=0.1.0
make image-build-mock-ui-plugins IMAGE_TAG=0.1.0
make image-build-gui IMAGE_TAG=0.1.0

# Verify all three built
podman images | grep fleetshift
# Expected:
# quay.io/rheemshort/fleetshift-server          0.1.0   ...
# quay.io/rheemshort/fleetshift-mock-servers     0.1.0   ...
# quay.io/rheemshort/fleetshift-mock-ui-plugins  0.1.0   ...
# quay.io/rheemshort/fleetshift-gui              0.1.0   ...
```

The mock-servers build takes the longest (installs native build tools for better-sqlite3). Expect 3-7 minutes per image on first build.

---

## Step 2: Prepare Configuration

```bash
cd /Repos/fleetshift-poc_mshort/deploy

# Copy the template to create your local config
cp .env.template .env

# Review the defaults (they should work as-is for demo mode)
cat .env
```

Key defaults:
- `DEMO_MODE=true` — includes local Keycloak
- `IMAGE_TAG=0.1.0` — matches what we just built
- All hostnames use container names (e.g., `fleetshift-server`, `keycloak`)

If you want to change anything (e.g., ports), edit `.env` now. The `.env` file is gitignored.

---

## Step 3: Start the Stack

```bash
cd /Repos/fleetshift-poc_mshort/deploy/podman

# Start everything
make up
```

You should see output like:

```
==> Creating podman network: fleetshift
==> Creating volumes
==> [init] Generating TLS certs
TLS certs generated in /certs/
==> Starting keycloak
    Waiting for keycloak to be healthy...
    Keycloak is ready.
==> Starting fleetshift-server
    Waiting for fleetshift-server to be healthy...
    FleetShift server is ready.
==> [init] Registering OIDC auth method
Auth method registered.
==> Starting fleetshift-mock-ui-plugins
    Waiting for fleetshift-mock-ui-plugins...
    Mock UI plugins ready.
==> Starting fleetshift-mock-servers
    Waiting for fleetshift-mock-servers...
    Mock servers ready.
==> Starting fleetshift-gui
==> FleetShift stack is running!
    GUI:             http://localhost:3000
    Mock API:        http://localhost:4000
    FleetShift API:  http://localhost:8085
    Mock Plugins:    http://localhost:8001
    Keycloak Admin:  https://keycloak:8443
    Keycloak (HTTP): http://localhost:8180
```

If any step hangs, Ctrl+C and check logs (see Troubleshooting below).

---

## Step 4: Verify Everything is Running

```bash
# Check container status
make status

# Expected output:
# NAMES                        STATUS          PORTS
# keycloak                     Up X minutes    0.0.0.0:8180->8180/tcp, 0.0.0.0:8443->8443/tcp
# fleetshift-server            Up X minutes    0.0.0.0:8085->8085/tcp, 0.0.0.0:50051->50051/tcp
# fleetshift-mock-ui-plugins   Up X minutes    0.0.0.0:8001->8001/tcp
# fleetshift-mock-servers      Up X minutes    0.0.0.0:4000->4000/tcp
# fleetshift-gui               Up X minutes    0.0.0.0:3000->3000/tcp
```

All five containers should show "Up".

---

## Step 5: Test the APIs

### FleetShift server API

```bash
# Should return JSON (deployments list, possibly empty)
curl -s http://localhost:8085/v1/deployments | python3 -m json.tool

# Should return JSON (targets list with kind-local and ocp-aws)
curl -s http://localhost:8085/v1/targets | python3 -m json.tool
```

### Keycloak OIDC

```bash
# Should return OIDC discovery document
curl -s http://localhost:8180/auth/realms/fleetshift/.well-known/openid-configuration | python3 -m json.tool

# Check the issuer field — should be:
# "issuer": "http://keycloak:8180/auth/realms/fleetshift"
```

### Mock servers

```bash
# Should return a response (may require auth token)
curl -s http://localhost:4000/api/v1/plugin-registry | python3 -m json.tool
```

### Plugin assets

```bash
# Should return HTML directory listing or plugin files
curl -s http://localhost:8001/ | head -20
```

---

## Step 6: Test the GUI

### Open the GUI

1. Open your browser to: **http://localhost:3000**

2. You should be redirected to the Keycloak login page at `http://keycloak:8180/auth/realms/fleetshift/...`

   If the redirect fails or you see a connection error to `keycloak:8180`:
   - Verify `/etc/hosts` has `127.0.0.1 keycloak`
   - Try accessing `http://keycloak:8180/auth` directly in your browser

3. Log in with one of these accounts:

   | Username | Password | Role | Description |
   |----------|----------|------|-------------|
   | `ops`    | `test`   | ops  | Operations persona — manages clusters |
   | `dev`    | `test`   | dev  | Developer persona — manages applications |
   | `admin`  | `test`   | both | Has both ops and dev roles |

4. After login, you should see the FleetShift dashboard.

### Verify GUI functionality

- **Navigation**: Check that the left sidebar shows navigation items
- **Cluster view**: Check that cluster information loads
- **Plugin loading**: Check browser DevTools Network tab — plugin JS bundles should load from port 8001

---

## Step 7: Test Container Lifecycle

### View logs

```bash
# Tail all container logs (interleaved)
make logs

# Tail a specific container's logs
make logs-fleetshift-server
make logs-fleetshift-mock-servers
make logs-fleetshift-gui

# Ctrl+C to stop tailing
```

### Restart a single container

```bash
# Restart just the mock-servers (e.g., after config change)
make restart-fleetshift-mock-servers

# Verify it came back
make status
```

### Stop the stack (preserve data)

```bash
# Stop and remove containers, keep volumes intact
make down

# Verify containers are gone
podman ps -a | grep fleetshift
# Should show nothing

# Verify volumes still exist
podman volume ls | grep fleetshift
# Should show: fleetshift-data, fleetshift-mock-servers-db, keycloak-certs, ui-plugins-dist
```

### Restart the stack (data persists)

```bash
# Start again — should be faster since certs already exist
make up

# "TLS certs already exist, skipping generation" confirms persistence
# Log back in to the GUI — your data should still be there
```

### Full cleanup (destroy everything)

```bash
# Stop containers AND remove all volumes and network
make down ARGS=--clean

# Verify everything is gone
podman volume ls | grep fleetshift    # Should show nothing
podman network ls | grep fleetshift   # Should show nothing
```

---

## Step 8: Push Images to Registry (Optional)

Only needed when you want others to pull your images.

```bash
# Push server image
cd /Repos/fleetshift-poc_mshort
make image-push IMAGE_TAG=0.1.0

# Push UI images
cd /Repos/fleetshift-user-interface_mshort
make image-push IMAGE_TAG=0.1.0

# Verify on quay.io:
# https://quay.io/user/rheemshort/
```

---

## Troubleshooting

### Container won't start

```bash
# Check what went wrong
podman logs <container-name>

# Common issues:
# - Port already in use: stop whatever is using the port, or change in .env
# - Image not found: verify images with `podman images | grep fleetshift`
```

### Keycloak health check hangs

```bash
# Check keycloak logs
podman logs keycloak

# Keycloak takes 30-60 seconds to start. If it takes longer:
# - Check if port 8180 or 8443 is already in use
# - Check if the realm JSON file exists: ls ../keycloak/fleetshift-realm.json
```

### FleetShift server health check hangs

```bash
# Check server logs
podman logs fleetshift-server

# Common issues:
# - Can't mount podman socket: verify path exists
#   ls /run/user/$(id -u)/podman/podman.sock  # Linux
#   # On Mac, the socket is inside the podman machine VM
# - OIDC CA file missing: only happens if keycloak-certs init failed
```

### Podman socket path on macOS

On macOS, the podman socket path is different from Linux. The start.sh script uses the Linux path (`/run/user/$(id -u)/podman/podman.sock`). On macOS with podman machine, you may need to adjust this.

Check your actual socket path:

```bash
podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'
# or
podman info --format '{{.Host.RemoteSocket.Path}}'
```

If the path is different, update the `-v` mount in start.sh for the fleetshift-server container, or set it as an env var.

### "exec 3<>/dev/tcp/localhost/PORT" fails in health check

This is a bash-specific feature. If the container uses a shell without `/dev/tcp` support:

```bash
# Alternative health check: use curl or nc instead
podman exec <container> curl -sf http://localhost:PORT/ || echo "not ready"
```

### GUI shows blank page or can't connect

```bash
# Check GUI logs
podman logs fleetshift-gui

# Verify the webpack proxy target is set
podman exec fleetshift-gui env | grep API_PROXY_TARGET
# Should show: API_PROXY_TARGET=http://fleetshift-mock-servers:4000

# Verify mock-servers is reachable from the GUI container
podman exec fleetshift-gui curl -s http://fleetshift-mock-servers:4000/api/v1/plugin-registry | head -20
```

### Network issues between containers

```bash
# Verify all containers are on the same network
podman network inspect fleetshift | python3 -m json.tool

# Test connectivity between containers
podman exec fleetshift-mock-servers curl -s http://fleetshift-server:8085/v1/deployments | head -20
podman exec fleetshift-gui curl -s http://fleetshift-mock-servers:4000/ | head -20
```
