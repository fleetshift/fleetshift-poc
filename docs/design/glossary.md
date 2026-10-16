# Architecture glossary

This guide distinguishes project names, architectural scope, and component
boundaries that are easy to conflate. It focuses on the high-level architecture.
For domain concepts such as fulfillment, placement, and resource identity, follow
the [architecture reading guide](architecture.md#reading-guide).

| Term | Meaning here |
| --- | --- |
| **Open Cluster Management (OCM)** | The community, project family, and multicluster ecosystem. Expand the name on first use. When discussing a particular architecture or integration, name the relevant OCM components, such as the ManifestWork controller or klusterlet. |
| **OCM Resource Manager** | This experimental OCM variant and the project/platform as a whole: the resource manager server, console, CLI, and extension framework, together with enabled addons. |
| **resource manager architecture** | The architectural approach explored by this experiment, in which a dedicated resource manager server owns domain APIs and database persistence directly. |
| **platform** / **platform instance** | The complete system or a particular installation, including its clients, enabled addons, agents, and supporting services. Use **parent/child platform instance** for recursive installations. |
| **management plane** | The services responsible for managing targets, including the resource manager server and participating addon services. |
| **resource manager server** / **resource manager (RM)** | The stateful component that owns domain APIs, authorization, persistence, and orchestration. Introduce **resource manager server** first; **resource manager** or **RM** can then refer to that component within the same context. |
| **console** | The browser application, introduced as the **OCM Resource Manager console**. Use **OpenShift console** when referring to that separate application. |
| **`fleetctl`** | The platform's command-line client. |
| **addon** | A platform extension that can provide API, UI, orchestration, or delivery capabilities. Its capabilities may span clients and independently running workloads, with their own execution and trust boundaries. See [addon integration](architecture/addon_integration.md). |
| **UI plugin** | A module loaded by the console to provide UI capabilities. An addon may supply a UI plugin; core and pre-installed plugins also exist. See [addon UI architecture](addon-ui-architecture.md). |
| **fleetlet** | The transport broker that connects local addon processes with the platform through channels. It handles connectivity and routing; addons provide application behavior. See [fleetlet and transport](architecture/fleetlet_and_transport.md). |

Use the full project or component name when establishing context, then the scoped
short form. For example: “OCM Resource Manager includes a console. The console
calls the resource manager server.” Capitalization helps distinguish the project
name **OCM Resource Manager** from the component **resource manager**.

Qualify overloaded uses of **platform**: a Kubernetes distribution, the platform
API layer, and the `platform` target type have specific meanings in their own
contexts. Multiple resource manager replicas can belong to one **platform
instance**.

For architectural comparisons, name the mechanism being compared, such as
**OCM's ManifestWork controller** and **the resource manager's delivery contract**.
