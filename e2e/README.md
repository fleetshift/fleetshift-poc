# FleetShift E2E Tests

End-to-end tests that exercise the full FleetShift platform against real infrastructure.

## Available Tests

| Test | Command | Duration | What it tests |
|------|---------|----------|---------------|
| `TestAWSProvision` | `make test-e2e-aws` | ~60 min | Full OCP cluster lifecycle on AWS with CCO STS mode |

## Prerequisites

### Software

- Go 1.25+
- `oc` CLI (for extracting binaries from release images)
- `dbus-x11` + `gnome-keyring` (Linux headless environments only)
- A browser accessible from your machine (for SSO logins)

### Keycloak (one-time per OCP cluster)

Deploy Keycloak with the FleetShift realm. Scripts are in `e2e/setup/keycloak/`:

```bash
cd e2e/setup/keycloak
./deploy.sh                    # Deploy Keycloak + realm + test users
./add-user.sh \                # Add your personal user
  --username you@company.com \
  --password yourpass \
  --github your-github-username \
  --roles ops,dev
```

See `e2e/setup/keycloak/README.md` for details on clients, realm config, and troubleshooting.

### AWS OIDC Federation (one-time per AWS account)

Register Keycloak as an IAM OIDC identity provider and create the `OCP-Provisioner` role. Script is in `e2e/setup/aws-oidc-federation/`:

```bash
# Get your Keycloak route
KEYCLOAK_HOST=$(oc get route -n keycloak-prod -o jsonpath='{.items[0].spec.host}')

# Run setup
KEYCLOAK_HOST=$KEYCLOAK_HOST ./e2e/setup/aws-oidc-federation/setup-aws-oidc-federation.sh

# To tear down:
KEYCLOAK_HOST=$KEYCLOAK_HOST ./e2e/setup/aws-oidc-federation/setup-aws-oidc-federation.sh --teardown
```

### AWS

- Route53 hosted zone for your base domain

### GitHub

- Each test run generates a new signing key that must be added to GitHub

## Setup

```bash
cd e2e
cp .env.example .env
# Edit .env with your values
```

## Running

### Linux headless (containers, SSH)

Unlock the keyring first, then run in the same shell:

```bash
eval "$(dbus-launch --sh-syntax)" && echo "" | gnome-keyring-daemon --unlock --components=secrets
make test-e2e-aws
```

### Desktop (macOS, Linux with desktop)

```bash
make test-e2e-aws
```

### Run all E2E tests

```bash
make test-e2e
```

## Interactive Steps

The test pauses for three browser-based logins:

1. **Keycloak login** — device code flow, open the printed URL
2. **Red Hat SSO login** — device code flow for pull secret
3. **Signing key enrollment** — browser redirect flow, then add the SSH key to GitHub

After validation, the test pauses for **manual inspection** before destroying the cluster.

## File Locations

### During test run

| File | Purpose |
|------|---------|
| `/tmp/fleetshift-e2e-server.log` | FleetShift server log (persists across runs) |
| `/tmp/fleetshift-e2e-data/fleetshift-e2e.db` | SQLite database (cleaned at start of each run) |
| `/tmp/ocp-provision-<cluster-name>/` | ocp-engine work directory (deleted by agent after completion) |
| `/tmp/ocp-provision-<cluster-name>/.openshift_install.log` | Raw OpenShift installer log |
| `/tmp/ocp-provision-<cluster-name>/auth/kubeconfig` | Cluster kubeconfig (cert-based, survives OIDC switch) |
| `/tmp/ocp-provision-<cluster-name>/auth/kubeadmin-password` | Kubeadmin password |
| `/tmp/ocp-provision-<cluster-name>/extra-manifests/` | OIDC manifests written by the agent |

### After test completion

| File | Purpose |
|------|---------|
| `/tmp/fleetshift-e2e-server.log` | Server log (always preserved) |
| `/tmp/fleetshift-e2e-data/` | DB dir (preserved for post-mortem, cleaned next run) |
| `/tmp/fleetshift-e2e-workdir/` | Copy of work dir (best-effort, may not exist if agent cleaned up first) |

### Configuration

| File | Purpose |
|------|---------|
| `e2e/.env` | Your test configuration (gitignored) |
| `e2e/.env.example` | Template with all available variables |
| `~/.config/fleetshift/auth.json` | fleetctl auth config (written by `fleetctl auth setup`) |

## Logging Architecture

ocp-engine splits output across two channels:

- **stdout** — JSON structured events: phase results, milestones, final result. Machine-readable, parsed by the agent via gRPC callback.
- **stderr** — Human-readable installer progress. Scrubbed of credentials during the cluster phase by the log pipeline. Raw during earlier phases (extract, ccoctl, manifests).

When running via fleetshift-server, both channels go to `/tmp/fleetshift-e2e-server.log`.

### Useful log searches

```bash
# Phase results
grep '"phase"' /tmp/fleetshift-e2e-server.log

# Errors and failures
grep -E '"status":"failed"|level=fatal|level=error' /tmp/fleetshift-e2e-server.log

# Credential resolution
grep "credentials resolved" /tmp/fleetshift-e2e-server.log

# OIDC manifest merge
grep -E "Merged|Injected|authentication" /tmp/fleetshift-e2e-server.log

# Deployment state changes
grep "deployment state changed" /tmp/fleetshift-e2e-server.log

# Delivery completion
grep "delivery completed" /tmp/fleetshift-e2e-server.log

# Bootstrap progress (from installer log, if work dir exists)
grep "level=info" /tmp/ocp-provision-<cluster-name>/.openshift_install.log | tail -20
```

## Troubleshooting

### Keyring errors

```
failed to unlock correct collection '/org/freedesktop/secrets/collection/login'
```

Run the keyring unlock before the test:
```bash
eval "$(dbus-launch --sh-syntax)" && echo "" | gnome-keyring-daemon --unlock --components=secrets
```

### Test failed — server still running

The test prompts before shutting down on failure. Use `fleetctl` to inspect:
```bash
fleetctl deployment list
fleetctl deployment get <name> -o json
```

Press Enter to shut down when done.

### Test failed — cluster still running

The test prints a warning banner with the cluster name. There are three ways to destroy, from easiest to most manual:

**Option 1: fleetshift-server is still running**

```bash
fleetctl deployment delete <cluster-name>
```

**Option 2: Work dir still exists (ideal)**

If the ocp-engine work directory still exists (check `/tmp/ocp-provision-<cluster-name>/`), this is the simplest and most complete cleanup — it destroys both cluster infrastructure and ccoctl resources in one command:

```bash
bin/ocp-engine destroy --work-dir /tmp/ocp-provision-<cluster-name>
```

This works because the work dir has the installer binary, ccoctl binary, metadata.json, and cluster.yaml with `cco_sts_mode: true`. AWS credentials are read from environment or the cluster.yaml.

**Option 3: Work dir is gone — rebuild manually**

```bash
# Set up a destroy work directory
CLUSTER_NAME=fleetshift-e2e-XXXX
INFRA_ID=fleetshift-e2e-XX-XXXXX   # check server log for this
REGION=us-west-2
WORK_DIR=/tmp/ocp-destroy-${CLUSTER_NAME}
RELEASE_IMAGE=quay.io/openshift-release-dev/ocp-release:4.21.0-multi
PULL_SECRET=/path/to/pull-secret.json

mkdir -p $WORK_DIR

# Write metadata.json
cat > $WORK_DIR/metadata.json << EOF
{
  "infraID": "${INFRA_ID}",
  "aws": {
    "region": "${REGION}",
    "identifier": [{"kubernetes.io/cluster/${INFRA_ID}": "owned"}]
  }
}
EOF

# Extract openshift-install
oc adm release extract --command=openshift-install \
  --to=$WORK_DIR --registry-config=$PULL_SECRET $RELEASE_IMAGE

# Get AWS credentials
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)

# Write cluster.yaml for ocp-engine
cat > $WORK_DIR/cluster.yaml << EOF
ocp_engine:
  pull_secret_file: /dev/null
  credentials:
    access_key_id: $AWS_ACCESS_KEY_ID
    secret_access_key: $AWS_SECRET_ACCESS_KEY
baseDomain: aws-acm-cluster-virt.devcluster.openshift.com
metadata:
  name: ${CLUSTER_NAME}
platform:
  aws:
    region: ${REGION}
EOF

# Destroy cluster infrastructure
bin/ocp-engine destroy --work-dir $WORK_DIR

# Clean up ccoctl resources (OIDC provider, IAM roles, S3 bucket)
oc adm release extract --command=ccoctl \
  --to=$WORK_DIR --registry-config=$PULL_SECRET $RELEASE_IMAGE

$WORK_DIR/ccoctl aws delete --name=$CLUSTER_NAME --region=$REGION
```

**Option 4: Direct openshift-install (no ocp-engine)**

```bash
# Same setup as Option 2, then:
$WORK_DIR/openshift-install destroy cluster --dir=$WORK_DIR --log-level=info
$WORK_DIR/ccoctl aws delete --name=$CLUSTER_NAME --region=$REGION
```

### Check for orphaned AWS resources

Run this after any destroy to make sure nothing was left behind:

```bash
CLUSTER_NAME=fleetshift-e2e-XXXX
INFRA_ID=fleetshift-e2e-XX-XXXXX
REGION=us-west-2

echo "=== EC2 Instances ==="
aws ec2 describe-instances --region $REGION \
  --filters "Name=instance-state-name,Values=running,pending,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name,Tags[?Key==`Name`].Value|[0]]' \
  --output table 2>&1 | grep -i fleet || echo "  none"

echo "=== S3 Buckets (ccoctl OIDC) ==="
aws s3 ls | grep -i fleet || echo "  none"

echo "=== IAM OIDC Providers (ccoctl) ==="
aws iam list-open-id-connect-providers \
  --query 'OpenIDConnectProviderList[].Arn' --output text | tr '\t' '\n' \
  | grep -i fleet | grep -v "keycloak" || echo "  none"

echo "=== IAM Roles (ccoctl per-operator) ==="
aws iam list-roles --query 'Roles[].RoleName' --output text | tr '\t' '\n' \
  | grep -i fleet | grep -v "OCP-Provisioner" || echo "  none"

echo "=== IAM Instance Profiles ==="
aws iam list-instance-profiles \
  --query 'InstanceProfiles[].InstanceProfileName' --output text | tr '\t' '\n' \
  | grep -i fleet || echo "  none"

echo "=== VPCs ==="
aws ec2 describe-vpcs --region $REGION \
  --query 'Vpcs[].[VpcId,Tags[?Key==`Name`].Value|[0]]' --output text \
  | grep -i fleet || echo "  none"

echo "=== Elastic IPs ==="
aws ec2 describe-addresses --region $REGION \
  --query 'Addresses[].[AllocationId,Tags[?Key==`Name`].Value|[0]]' --output text \
  | grep -i fleet || echo "  none"

echo "=== ELBs ==="
aws elbv2 describe-load-balancers --region $REGION \
  --query 'LoadBalancers[].LoadBalancerName' --output text | tr '\t' '\n' \
  | grep -i fleet || echo "  none"

echo "=== Route53 Hosted Zones ==="
aws route53 list-hosted-zones --query 'HostedZones[].Name' --output text | tr '\t' '\n' \
  | grep -i fleet || echo "  none"
```

**Note:** The `OCP-Provisioner` IAM role and the Keycloak OIDC provider are permanent infrastructure — don't delete those. Only delete resources with your cluster name prefix.

### Restart server with existing DB

```bash
bin/fleetshift serve --db /tmp/fleetshift-e2e-data/fleetshift-e2e.db --log-level debug
```

### Query target properties directly

```bash
sqlite3 /tmp/fleetshift-e2e-data/fleetshift-e2e.db \
  "SELECT id, name, properties FROM targets WHERE id LIKE 'k8s-%';"
```

## Test Flow (TestAWSProvision)

```
01_Build              — Build fleetshift-server, fleetctl, ocp-engine
02_KeycloakLogin      — Device code flow → Keycloak JWT (browser)
03_RedHatSSOLogin     — Device code flow → pull secret (browser)
04_StartServer        — Start fleetshift-server + auth setup
05_EnrollSigningKey   — Enroll signing key (browser) + add to GitHub
06_CreateDeployment   — fleetctl deployment create --sign
07_WaitForProvision   — Poll every 30s for STATE_ACTIVE (~45-60 min)
08_ValidateDeployment — Check target properties in DB
09_ValidateClusterOIDC— Login as OIDC user, check STS mode, operators
10_ManualInspection   — Pause for manual cluster inspection
11_DestroyDeployment  — fleetctl deployment delete
12_ValidateCleanup    — Verify AWS resources cleaned up
```

## Setup Directory Structure

```
e2e/setup/
├── keycloak/                          # Keycloak deployment on OCP
│   ├── deploy.sh                      # Deploy Keycloak + realm + test users
│   ├── teardown.sh                    # Remove everything
│   ├── add-user.sh                    # Add personal user with GitHub username
│   ├── README.md                      # Detailed Keycloak setup docs
│   ├── realm/
│   │   └── fleetshift-realm.json      # Realm config (3 clients, 3 test users, 2 roles)
│   └── manifests/
│       ├── namespace.yaml             # keycloak-prod namespace
│       ├── cert-manager-sub.yaml      # cert-manager operator
│       ├── cluster-issuer.yaml        # Let's Encrypt ClusterIssuer
│       ├── certificate.yaml           # Keycloak TLS certificate
│       ├── rhbk-sub.yaml             # RHBK operator (Keycloak)
│       ├── postgres-statefulset.yaml  # PostgreSQL database
│       └── keycloak.yaml              # Keycloak CR
│
└── aws-oidc-federation/               # AWS IAM setup for STS
    └── setup-aws-oidc-federation.sh   # Register Keycloak as IAM OIDC provider,
                                       # create OCP-Provisioner IAM role
                                       # Requires: KEYCLOAK_HOST env var
                                       # Idempotent, supports --teardown
```
