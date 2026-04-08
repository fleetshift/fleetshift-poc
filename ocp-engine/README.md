# ocp-engine

A stateless CLI tool for provisioning and deprovisioning OpenShift 4.20 clusters on AWS. It wraps `openshift-install` with phased execution and structured JSON output, designed for integration with an external management platform.

No Kubernetes cluster required. No CRDs. No controllers. Just a binary on a management server.

## Prerequisites

The following must be available on your management server:

- **`oc` CLI** -- used to extract `openshift-install` from OCP release images
- **Podman or Docker** -- used to pull OCP release images
- **Red Hat pull secret** -- download from [console.redhat.com](https://console.redhat.com/openshift/install/pull-secret)
- **AWS credentials** with IAM permissions sufficient for IPI provisioning (EC2, VPC, ELB, Route53, IAM, S3, etc.)

## Installation

```bash
git clone <repo-url>
cd ocp-engine
go build -o ocp-engine .
```

Move the binary to a location in your PATH:

```bash
sudo mv ocp-engine /usr/local/bin/
```

## Quick Start

### 1. Create a cluster config

```yaml
# cluster.yaml
cluster:
  name: my-cluster
  base_domain: example.com
  version: "4.20"

platform:
  aws:
    region: us-east-1
    credentials:
      access_key_id: "AKIA..."
      secret_access_key: "..."

pull_secret_file: /path/to/pull-secret.json
ssh_public_key_file: ~/.ssh/id_rsa.pub
```

### 2. Validate configuration (dry run)

```bash
ocp-engine gen-config --config cluster.yaml --work-dir /clusters/my-cluster
```

This generates `install-config.yaml` in the work directory without creating any AWS resources. Inspect it to verify your settings.

### 3. Provision the cluster

```bash
ocp-engine provision --config cluster.yaml --work-dir /clusters/my-cluster
```

This runs through 5 phases and takes approximately 30-45 minutes:

| Phase | What happens | AWS resources created? |
|---|---|---|
| extract | Downloads `openshift-install` from release image | No |
| install-config | Generates `install-config.yaml` | No |
| manifests | Generates Kubernetes manifests | No |
| ignition | Generates ignition configs | No |
| cluster | Creates AWS infrastructure and installs OCP | **Yes** |

Each phase outputs a JSON line to stdout on completion:

```json
{"phase":"extract","status":"complete","elapsed_seconds":45}
{"phase":"install-config","status":"complete","elapsed_seconds":0}
{"phase":"manifests","status":"complete","elapsed_seconds":8}
{"phase":"ignition","status":"complete","elapsed_seconds":3}
{"phase":"cluster","status":"complete","elapsed_seconds":2100}
```

On success, your kubeconfig is at `/clusters/my-cluster/auth/kubeconfig`:

```bash
export KUBECONFIG=/clusters/my-cluster/auth/kubeconfig
oc get nodes
```

### 4. Check status

```bash
ocp-engine status --work-dir /clusters/my-cluster
```

Returns structured JSON:

```json
{
  "state": "succeeded",
  "completed_phases": ["extract", "install-config", "manifests", "ignition", "cluster"],
  "infra_id": "my-cluster-a1b2c",
  "has_kubeconfig": true,
  "has_metadata": true
}
```

### 5. Destroy the cluster

```bash
ocp-engine destroy --work-dir /clusters/my-cluster
```

This runs `openshift-install destroy cluster`, which finds all AWS resources tagged with `kubernetes.io/cluster/<infraID>: owned` and deletes them. Destroy is idempotent -- safe to run multiple times.

## Commands

### `ocp-engine provision`

Provision a new OCP cluster on AWS.

```
ocp-engine provision --config <path> --work-dir <path>
```

| Flag | Required | Description |
|---|---|---|
| `--config` | Yes | Path to `cluster.yaml` configuration file |
| `--work-dir` | Yes | Path to work directory for this cluster (created if it doesn't exist) |

### `ocp-engine status`

Check the status of a work directory.

```
ocp-engine status --work-dir <path>
```

| Flag | Required | Description |
|---|---|---|
| `--work-dir` | Yes | Path to work directory to inspect |

**Possible states:**

| State | Meaning |
|---|---|
| `empty` | Work directory exists but no phases have started |
| `running` | A provision or destroy operation is currently active |
| `succeeded` | All phases complete, kubeconfig available |
| `failed` | A phase failed, process exited |
| `partial` | Phases partially complete, process not running (e.g., server crashed) |

### `ocp-engine destroy`

Destroy a cluster and clean up all AWS resources.

```
ocp-engine destroy --work-dir <path>
```

| Flag | Required | Description |
|---|---|---|
| `--work-dir` | Yes | Path to work directory (must contain `metadata.json` and `openshift-install`) |

### `ocp-engine gen-config`

Generate `install-config.yaml` without running any install phases. Useful for validating configuration.

```
ocp-engine gen-config --config <path> --work-dir <path>
```

| Flag | Required | Description |
|---|---|---|
| `--config` | Yes | Path to `cluster.yaml` configuration file |
| `--work-dir` | Yes | Path to work directory (created if it doesn't exist) |

## Configuration Reference

### Full `cluster.yaml` example

```yaml
cluster:
  name: my-cluster                # Required. Cluster name.
  base_domain: example.com        # Required. Base DNS domain.
  version: "4.20"                 # OCP version.

platform:
  aws:
    region: us-east-1             # Required. AWS region.
    credentials:                  # Required. One of the 4 modes below.
      access_key_id: "AKIA..."
      secret_access_key: "..."
    tags:                         # Optional. Applied to all AWS resources.
      environment: staging
      team: platform

control_plane:
  replicas: 3                     # Default: 3
  instance_type: m6a.xlarge       # Default: m6a.xlarge
  root_volume:
    size_gb: 120                  # Default: 120
    type: gp3                     # Default: gp3

compute:
  replicas: 3                     # Default: 3
  instance_type: m6a.xlarge       # Default: m6a.xlarge
  root_volume:
    size_gb: 120                  # Default: 120
    type: gp3                     # Default: gp3

networking:
  cluster_network: 10.128.0.0/14  # Default: 10.128.0.0/14
  service_network: 172.30.0.0/16  # Default: 172.30.0.0/16
  machine_network: 10.0.0.0/16   # Default: 10.0.0.0/16
  host_prefix: 23                 # Default: 23

pull_secret_file: /path/to/pull-secret.json    # Required. Path to pull secret.
ssh_public_key_file: /path/to/id_rsa.pub       # Optional. SSH key for node access.

release_image: quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64  # Optional. Override release image.
additional_trust_bundle_file: /path/to/ca-bundle.pem  # Optional. Custom CA bundle.
fips: false                       # Optional. Enable FIPS mode.
publish: External                 # Optional. External (default) or Internal.
```

### Required fields

- `cluster.name`
- `cluster.base_domain`
- `platform.aws.region`
- AWS credentials (one mode)
- `pull_secret_file`

Everything else has sensible defaults.

### AWS Credential Modes

Choose one of four credential modes:

**Inline credentials:**
```yaml
credentials:
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

**Credentials file:**
```yaml
credentials:
  credentials_file: /path/to/aws/credentials
```

**Named profile:**
```yaml
credentials:
  profile: my-aws-profile
```

**STS assume role:**
```yaml
credentials:
  role_arn: arn:aws:iam::123456789:role/ocp-installer
```

## Work Directory

Each cluster gets its own work directory containing all artifacts:

```
/clusters/my-cluster/
  cluster.yaml              # Input config (copied)
  install-config.yaml       # Generated install config
  openshift-install         # Cached binary from release image
  manifests/                # Generated by openshift-install
  openshift/                # Generated by openshift-install
  auth/
    kubeconfig              # Cluster access (on success)
    kubeadmin-password      # Admin password (on success)
  metadata.json             # Cluster metadata (needed for destroy)
  .openshift_install.log    # Consolidated installer log
  _phase_extract_complete   # Phase completion markers
  _phase_install-config_complete
  _phase_manifests_complete
  _phase_ignition_complete
  _phase_cluster_complete
  _pid                      # PID of running process
```

## Exit Codes

- **0** -- Success
- **1** -- Failure (details in JSON output on stdout)

## Error Handling

All errors are returned as structured JSON on stdout:

```json
{
  "category": "phase_error",
  "phase": "cluster",
  "message": "bootstrap timeout after 30 minutes",
  "log_tail": "last 20 lines of installer log...",
  "has_metadata": true,
  "requires_destroy": true
}
```

**Error categories:**

| Category | Meaning | What to do |
|---|---|---|
| `config_error` | Invalid config (bad region, missing pull secret, etc.) | Fix config and retry |
| `prereq_error` | Missing prerequisite (`oc`, container runtime) | Install missing tool and retry |
| `phase_error` | `openshift-install` failed during a phase | Check `requires_destroy` (see below) |
| `already_running` | Another operation is running in this work directory | Wait or check status |
| `workdir_error` | Work directory issue (missing metadata for destroy, etc.) | Check work directory |

### Handling Failures

**Failed before `cluster` phase** (`requires_destroy: false`):
No AWS resources were created. Delete the work directory and retry.

```bash
rm -rf /clusters/my-cluster
ocp-engine provision --config cluster.yaml --work-dir /clusters/my-cluster
```

**Failed during `cluster` phase** (`requires_destroy: true`):
AWS resources may exist. Destroy before retrying.

```bash
ocp-engine destroy --work-dir /clusters/my-cluster
# Then retry with a fresh work directory
ocp-engine provision --config cluster.yaml --work-dir /clusters/my-cluster-2
```

## Running Multiple Clusters

Each cluster uses its own work directory. Run as many as you want in parallel:

```bash
ocp-engine provision --config cluster-a.yaml --work-dir /clusters/a &
ocp-engine provision --config cluster-b.yaml --work-dir /clusters/b &
ocp-engine provision --config cluster-c.yaml --work-dir /clusters/c &
wait
```

There is no shared state between clusters. Each is an independent process with its own `openshift-install` invocation.

## AWS Resource Tagging

`openshift-install` automatically tags all AWS resources with:

```
kubernetes.io/cluster/<infraID>: owned
```

The `infraID` is auto-generated during install and stored in `metadata.json`. The destroy command uses these tags to find and delete all resources belonging to a cluster.

Any custom tags you specify in `platform.aws.tags` are applied on top of the infrastructure tags.

## Platform Integration

`ocp-engine` is designed to be called by an external management platform. The platform is responsible for:

- **State tracking** -- which clusters exist, what state they're in
- **Retry logic** -- when and whether to retry failed provisions
- **Scheduling** -- when to provision/destroy clusters
- **Credential management** -- providing AWS credentials and pull secrets

The engine just does what it's told and returns structured results. Parse the JSON output from stdout to drive your automation.

### Integration example (bash)

```bash
#!/bin/bash
output=$(ocp-engine provision --config cluster.yaml --work-dir /clusters/001 2>/dev/null)
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Cluster provisioned successfully"
    kubeconfig="/clusters/001/auth/kubeconfig"
else
    requires_destroy=$(echo "$output" | tail -1 | jq -r '.requires_destroy // false')
    if [ "$requires_destroy" = "true" ]; then
        echo "Provision failed with AWS resources created. Destroying..."
        ocp-engine destroy --work-dir /clusters/001
    else
        echo "Provision failed before AWS resources were created. Safe to retry."
    fi
fi
```
