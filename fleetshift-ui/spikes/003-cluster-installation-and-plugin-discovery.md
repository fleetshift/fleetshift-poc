# SPIKE: Cluster Installation and Plugin Discovery

**Jira**: FM-TBD (pending approval to create)
**Epic**: SPIKE - User Interface (Web UI)
**Status**: Done

## Problem

When a user adds a cluster to FleetShift, the UI needs to figure out two things: how to connect to it, and what plugins are relevant for it. Different clusters have different things installed — one might have Prometheus, another might have cert-manager, a third might be a plain OpenShift with nothing extra. The UI needs to discover what is available and load the right plugins automatically, without the user having to manually configure what features to show.

On top of that, different Kubernetes distributions come with different built-in capabilities. OpenShift has Routes and ConsolePlugins. Vanilla Kubernetes does not. Some things like pods, nodes, and storage are always there regardless of distribution. The discovery system needs to handle all of this.

## What was explored

### Cluster connection flow

Once a cluster connection is established (authentication is handled separately), the system goes through a set of steps to get the cluster ready. The UI shows progress in real time via as each step completes:

1. Verify API connectivity and get the cluster version
2. Create API clients for the resource types we need
3. Run plugin discovery (see below)
4. Detect the platform (OpenShift vs vanilla Kubernetes vs others)
5. Count nodes
6. Register the cluster

If any step fails, the user sees which step broke and why. They can fix the issue and retry.

### Plugin discovery

There is no single standard API in Kubernetes that tells you "here is what addons are installed." Discovery has to use multiple signals and combine them.

#### CRD matching

This is the main generic mechanism. When operators or controllers are installed on a cluster, they register CRDs. Each CRD belongs to an API group — for example, Prometheus Operator registers CRDs under `monitoring.coreos.com`, cert-manager registers under `cert-manager.io`, Strimzi registers under `kafka.strimzi.io`.

By listing all CRDs on a cluster and matching their API groups against a known mapping, we can figure out what is installed. The mapping is maintained on our side — it says things like "if you see `monitoring.coreos.com`, the observability plugin is relevant."

This works across all Kubernetes distributions because CRDs are a core K8s concept. The downside is that the mapping needs to be maintained. If a new operator shows up that we want to support, someone has to add its API group to the map.

#### API group detection

Some capabilities register as API groups without CRDs. The metrics-server is the main example — it registers the `metrics.k8s.io` API group. By checking the cluster's API group list, we can detect if the metrics API is available and enable the observability plugin or other plugins accordingly.

#### OpenShift ConsolePlugins

OpenShift has a purpose-built mechanism for this. Operators can register a `ConsolePlugin` custom resource (under `console.openshift.io/v1`) that declares a UI plugin — its name, which service serves its frontend assets, and what backend endpoints it proxies. This is the cleanest discovery path available because it was designed specifically for frontend plugin registration.

When we detect that a cluster is OpenShift, we list all ConsolePlugin resources and use them as an additional discovery signal.

#### Always-on (base) plugins

Some things are always present on every Kubernetes cluster — pods, nodes, namespaces, services, storage, events. There is no API that tells you "this cluster has pods" because every cluster has pods. These are part of the core K8s API and the corresponding plugins are always enabled regardless of what discovery finds.

#### Platform-specific base plugins

Different distributions have different built-in capabilities beyond core K8s:

- **OpenShift** adds Routes, Builds, ImageStreams, DeploymentConfigs, ConsolePlugins, and more. An OpenShift cluster should have the routes plugin enabled by default even if no extra CRDs are installed.
- **Rancher** has Projects, Catalogs, and its own extension system.
- **EKS/GKE/AKS** have their own managed addon systems, but those are accessed through cloud provider APIs, not the K8s API itself.

The base plugin set needs to vary by detected platform. Platform detection is done by checking for known API groups — if `route.openshift.io` or `apps.openshift.io` is present, it is OpenShift. Similar checks could be added for other distributions.

**Open question**: Which platforms do we target? OpenShift is a given. Raw Kubernetes makes sense too since we need it as the baseline even for OpenShift. But do we want to invest in platform-specific support for Rancher, EKS, GKE, AKS, or others? At least not initially — but this needs a decision.

### How it all comes together

The final list of plugins for a cluster is the union of:

1. **Always-on plugins** — core K8s stuff, hardcoded.
2. **Platform-specific plugins** — based on detected distribution.
3. **CRD-matched plugins** — from the API group mapping.
4. **API-detected plugins** — like metrics-server.
5. **ConsolePlugin-registered plugins** — OpenShift only.

This list is stored with the cluster record and used by the shell to decide what to load.

### Cluster persistence

Cluster connection configs are persisted so they survive server restarts. 

The config file is watched for changes and hot-reloaded — if someone edits it, the server reconnects without restarting.

### Open areas

- The CRD-to-plugin mapping is hardcoded and needs to be maintained as new operators are supported.
- Cloud provider addon APIs (EKS, GKE, AKS) are not integrated — discovery only uses the K8s API.
- Support for Rancher and other non-OpenShift distributions has not been explored beyond basic CRD detection.

## Key files

- Cluster connection flow: `packages/mock-servers/src/k8s/client/connect.ts`
- Plugin discovery: `packages/mock-servers/src/k8s/client/discovery.ts`
- Cluster registry: `packages/mock-servers/src/k8s/client/registry.ts`
- Cluster persistence: `packages/mock-servers/src/k8s/client/persistence.ts`
- Add Cluster UI: `packages/gui/src/pages/ClusterListPage/AddCluster/`
- Debug page (discovery details): `packages/gui/src/pages/DebugPage.tsx`
