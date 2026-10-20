# Architecture glossary

This guide distinguishes project names, architectural scope, and component
boundaries that are easy to conflate. It focuses on the high-level architecture.
For domain concepts such as fulfillment, placement, and resource identity, follow
the [architecture reading guide](architecture.md#reading-guide).

| Term | Meaning here |
| --- | --- |
| **Open Cluster Management (OCM)** | The community, project family, and multicluster ecosystem. Expand the name on first use. When discussing a particular architecture or integration, name the relevant OCM components, such as the ManifestWork controller or klusterlet. |
| **OCM Fleet** | This experimental OCM variant and project: the resource manager server, console, CLI, fleetlet, and addon/extension interfaces. An addon can participate in an OCM Fleet deployment without belonging to the project. Any included core addons/extensions must be identified explicitly. |
| **the OCM Fleet architecture** | This project's particular design, including its component relationships, fleetlet channels, and addon contracts. |
| **a resource manager architecture** | A management architecture organized around a logically central service responsible for management APIs, authorization, persistent management state, and orchestration. This is descriptive terminology defined here, without assuming an industry-standard taxonomy. |
| **platform** / **platform instance** | The complete deployed system or a particular installation, including its clients, enabled integrations, agents, and supporting services. Deployment participation does not imply project membership. Use **parent/child platform instance** for recursive installations. |
| **management plane** | The APIs, services, and user interfaces through which a platform instance represents, observes, configures, and governs resources and coordinates their lifecycle. It includes the resource manager server and the management capabilities supplied by addons. |
| **resource manager server** / **resource manager (RM)** | The stateful component that owns domain APIs, authorization, persistence, and orchestration. Introduce **resource manager server** first; **resource manager** or **RM** can then refer to that component within the same context. |
| **console** | The browser application, introduced as the **OCM Fleet console**. |
| **`fleetctl`** | The platform's command-line client. |
| **addon** | A platform extension that can provide API, UI, orchestration, or delivery capabilities. Its capabilities may span clients and independently running workloads, with their own execution and trust boundaries. See [addon integration](architecture/addon_integration.md). |
| **UI plugin** | A module loaded by the console to provide UI capabilities. An addon may supply a UI plugin; core and pre-installed plugins also exist. See [addon UI architecture](addon-ui-architecture.md). |
| **fleetlet** | The canonical component that connects local addon processes with the platform through channels and provides transport and shared cross-cutting services, including application authorization and provenance verification for delivery. Addons provide domain- and resource-specific behavior, such as manifest generation, constraint enforcement, and resource application. Multiple deployments are **fleetlet instances**, potentially connected to different addons. See [fleetlet and transport](architecture/fleetlet_and_transport.md). |

**The OCM Fleet architecture is a resource manager architecture.** Logical
centrality does not require one process, replica, or global deployment. Attribute
specific choices such as fleetlet channels and addon contracts to OCM Fleet;
they are not implied requirements of every resource manager architecture.

Use the full project or component name when establishing context, then the scoped
short form. For example: “OCM Fleet includes a console. The console
calls the resource manager server.” Capitalization helps distinguish the project
name **OCM Fleet** from the component **resource manager**.

Qualify overloaded uses of **platform**: a Kubernetes distribution, the platform
API layer, and the `platform` target type have specific meanings in their own
contexts. Multiple resource manager replicas can belong to one **platform
instance**.

For architectural comparisons, name the mechanism being compared, such as
**OCM's ManifestWork controller** and **the resource manager's delivery contract**.
