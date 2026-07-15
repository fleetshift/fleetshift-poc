import {
  createClusterProvider,
  createModule,
  createModuleGroup,
  createOnboardingAction,
  createPfModuleReplacementPlugin,
  createPfTransformImport,
  createSearchResultRenderer,
  createSetup,
  FleetshiftPlugin,
  getDynamicModules,
} from "@fleetshift/build-utils";
import { ModuleFederationPlugin as BaseMFPlugin } from "@module-federation/enhanced/rspack";
import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";
import { fileURLToPath } from "url";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const monorepoRoot = path.resolve(configDir, "../..");
const pfSharedModules = getDynamicModules(configDir, monorepoRoot);
const pfTransformImport = createPfTransformImport();

const swcLoaderRule = {
  test: /\.tsx?$/,
  exclude: [/node_modules/, /packages\/common\/dist/],
  loader: "builtin:swc-loader" as const,
  options: {
    jsc: {
      parser: { syntax: "typescript" as const, tsx: true },
      // TODO: enable reactCompiler: true when rspack >= 2.1.0
      transform: { react: { runtime: "automatic" as const } },
    },
    transformImport: pfTransformImport,
  },
  type: "javascript/auto" as const,
};

const sharedModules = {
  react: { singleton: true, requiredVersion: "*" },
  "@fleetshift/common": {
    requiredVersion: "*",
    version: "*",
  },
  "react-dom": { singleton: true, requiredVersion: "*" },
  "@scalprum/core": { singleton: true, requiredVersion: "*" },
  "@scalprum/react-core": { singleton: true, requiredVersion: "*" },
  "@openshift/dynamic-plugin-sdk": {
    singleton: true,
    requiredVersion: "*",
    version: "*",
  },
  "react-router-dom": { singleton: true, requiredVersion: "*" },
  "react/jsx-runtime": { singleton: true, requiredVersion: "^19" },
  "oidc-client-ts": { singleton: true, requiredVersion: "*" },
  "react-oidc-context": { singleton: true, requiredVersion: "*" },
  ...pfSharedModules,
};

class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false, manifest: false });
  }
}

const mfOverride = {
  libraryType: "global",
  pluginOverride: {
    ModuleFederationPlugin,
  },
};

const p = (rel: string) => path.resolve(configDir, rel);

const ManagementPlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "targets",
      label: "Targets",
      component: { $codeRef: "TargetsPage.default" },
      icon: { $codeRef: "TargetsIcon.default" },
      description: "View and manage deployment targets",
      keywords: ["target", "deploy", "rollout"],
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/management/management-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/management/management-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "management-plugin",
    version: "1.0.0",
    exposedModules: {
      TargetsPage: p("./src/plugins/management-plugin/TargetsPage.tsx"),
      TargetsIcon: p("./src/plugins/management-plugin/TargetsIcon.tsx"),
    },
  },
});

const CorePlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "clusters",
      label: "Clusters",
      component: { $codeRef: "ClustersModule.default" },
      icon: { $codeRef: "ClustersIcon.default" },
      description: "View and manage your fleet of clusters",
      keywords: ["cluster", "fleet", "manage", "create", "deploy", "provision"],
      extensionPoints: {
        providers: {
          description: "Cluster providers available in the creation wizard",
          type: "fleetshift.cluster-provider",
        },
      },
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/core/core-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/core/core-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "core-plugin",
    version: "1.0.0",
    exposedModules: {
      ClustersModule: p("./src/plugins/core-plugin/ClustersModule.tsx"),
      ClustersIcon: p("./src/plugins/core-plugin/ClustersIcon.tsx"),
    },
  },
});

const SigningPlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "signing-keys",
      label: "Signing Keys",
      component: { $codeRef: "SigningKeyEnrollment.default" },
      icon: { $codeRef: "SigningKeysIcon.default" },
      description: "Manage signing keys for deployment verification",
      keywords: ["signing", "key", "enrollment", "cosign"],
    }),
    createSetup({
      id: "signing-key-enrollment",
      label: "Signing Key Enrollment",
      description:
        "Enroll signing keys for deployment verification and supply chain security.",
      path: "enroll",
      component: { $codeRef: "SigningKeyEnrollment.default" },
      requires: [],
      requiresAuth: true,
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/signing/signing-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/signing/signing-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "signing-plugin",
    version: "1.0.0",
    exposedModules: {
      SigningKeyEnrollment: p(
        "./src/plugins/signing-plugin/SigningKeyEnrollment.tsx",
      ),
      SigningKeysIcon: p("./src/plugins/signing-plugin/SigningKeysIcon.tsx"),
      useSigningKey: p("./src/plugins/signing-plugin/useSigningKey.ts"),
      signingKeyApi: p("./src/plugins/signing-plugin/signingKeyApi.ts"),
    },
  },
});

const RoutingPlugin = new FleetshiftPlugin({
  extensions: [],
  sharedModules,
  entryScriptFilename: "plugins/routing/routing-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/routing/routing-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "routing-plugin",
    version: "1.0.0",
    exposedModules: {
      PluginLink: p("./src/plugins/routing-plugin/PluginLink.tsx"),
      usePluginNavigate: p(
        "./src/plugins/routing-plugin/usePluginNavigate.tsx",
      ),
      usePluginLinks: p("./src/plugins/routing-plugin/usePluginLinks.tsx"),
    },
  },
});

const GcpHcpPlugin = new FleetshiftPlugin({
  extensions: [
    createClusterProvider({
      id: "gcphcp",
      label: "GCP Hosted Control Plane",
      description:
        "Create a managed OpenShift cluster on Google Cloud Platform.",
      keywords: [
        "gcp",
        "google cloud",
        "hosted control plane",
        "managed",
        "hcp",
      ],
      to: { search: "?create=gcphcp" },
      icon: { $codeRef: "GcpHcpProviderCard.GcpHcpIcon" },
      card: { $codeRef: "GcpHcpProviderCard.default" },
      wizard: { $codeRef: "CreateGcpHcpWizard.default" },
      searchIcon: { $codeRef: "GcpHcpIcon.default" },
    }),
    createOnboardingAction({
      id: "gcphcp-connect",
      label: "GCP Hosted Control Plane",
      description: "Connect your GCP project to create managed HCP clusters.",
      icon: { $codeRef: "GcpHcpIcon.default" },
      card: { $codeRef: "GcpHcpOnboardingCard.default" },
      form: { $codeRef: "GcpHcpConnectionForm.default" },
      overviewCta: "Integrate your first addon",
      category: "fleetshift.cluster-provider",
    }),
    createSearchResultRenderer({
      id: "gcphcp-cluster-renderer",
      label: "GCP HCP Cluster",
      resourceType: "gcphcp.fleetshift.io/Cluster",
      resolve: { $codeRef: "GcpHcpSearchResult.resolveGcpHcpCluster" },
      icon: { $codeRef: "GcpHcpSearchResult.GcpHcpClusterIcon" },
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/gcphcp/gcphcp-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/gcphcp/gcphcp-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "gcphcp-plugin",
    version: "1.0.0",
    exposedModules: {
      GcpHcpProviderCard: p(
        "./src/plugins/gcphcp-plugin/GcpHcpProviderCard.tsx",
      ),
      CreateGcpHcpWizard: p(
        "./src/plugins/gcphcp-plugin/CreateGcpHcpWizard.tsx",
      ),
      GcpHcpIcon: p("./src/plugins/gcphcp-plugin/GcpHcpIcon.tsx"),
      GcpHcpOnboardingCard: p(
        "./src/plugins/gcphcp-plugin/GcpHcpOnboardingCard.tsx",
      ),
      GcpHcpConnectionForm: p(
        "./src/plugins/gcphcp-plugin/GcpHcpConnectionForm.tsx",
      ),
      GcpHcpSearchResult: p(
        "./src/plugins/gcphcp-plugin/GcpHcpSearchResult.tsx",
      ),
    },
  },
});

const OverviewPlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "overview",
      label: "Overview",
      component: { $codeRef: "OverviewDashboard.default" },
      icon: { $codeRef: "OverviewIcon.default" },
      description: "Fleet overview dashboard",
      keywords: ["overview", "dashboard", "summary"],
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/overview/overview-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/overview/overview-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "overview-plugin",
    version: "1.0.0",
    exposedModules: {
      OverviewDashboard: p(
        "./src/plugins/overview-plugin/OverviewDashboard.tsx",
      ),
      OverviewIcon: p("./src/plugins/overview-plugin/OverviewIcon.tsx"),
    },
  },
});

const KindPlugin = new FleetshiftPlugin({
  extensions: [
    createClusterProvider({
      id: "kind",
      label: "Kind",
      description: "Create a local Kind cluster for development and testing.",
      keywords: ["kind", "local", "development", "testing"],
      to: { search: "?create=kind" },
      icon: { $codeRef: "KindProviderCard.KindIcon" },
      card: { $codeRef: "KindProviderCard.default" },
      wizard: { $codeRef: "CreateClusterWizard.default" },
      searchIcon: { $codeRef: "KindIcon.default" },
    }),
    createSearchResultRenderer({
      id: "kind-cluster-renderer",
      label: "Kind Cluster",
      resourceType: "kind.fleetshift.io/Cluster",
      resolve: { $codeRef: "KindSearchResult.resolveKindCluster" },
      icon: { $codeRef: "KindSearchResult.KindClusterIcon" },
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/kind/kind-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/kind/kind-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "kind-plugin",
    version: "1.0.0",
    exposedModules: {
      KindProviderCard: p("./src/plugins/kind-plugin/KindProviderCard.tsx"),
      CreateClusterWizard: p(
        "./src/plugins/kind-plugin/CreateClusterWizard.tsx",
      ),
      KindIcon: p("./src/plugins/kind-plugin/KindIcon.tsx"),
      KindSearchResult: p("./src/plugins/kind-plugin/KindSearchResult.tsx"),
    },
  },
});

const SettingsPlugin = new FleetshiftPlugin({
  extensions: [
    createModuleGroup({
      id: "settings",
      label: "Settings",
      description: "Workspace configuration and preferences",
      keywords: ["settings", "preferences", "configuration"],
    }),
    createModule({
      id: "navigation",
      label: "Navigation",
      group: "settings",
      component: { $codeRef: "SettingsPage.default" },
      icon: { $codeRef: "SettingsIcon.default" },
      description: "Manage nav layout and workspace preferences",
      keywords: ["settings", "preferences", "nav", "order", "navigation"],
    }),
    createModule({
      id: "auth-settings",
      label: "Authentication",
      group: "settings",
      component: { $codeRef: "AuthSettingsPage.default" },
      icon: { $codeRef: "AuthIcon.default" },
      description:
        "Configure authentication provider, backing store, and OIDC settings",
      keywords: [
        "auth",
        "authentication",
        "oidc",
        "identity",
        "keycloak",
        "login",
      ],
    }),
    createModule({
      id: "extensions",
      label: "Extensions",
      group: "settings",
      component: { $codeRef: "ExtensionsPage.default" },
      icon: { $codeRef: "ExtensionsIcon.default" },
      description: "Discover and configure extensions for your fleet.",
      keywords: [
        "extension",
        "addon",
        "integration",
        "onboarding",
        "configure",
      ],
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/settings/settings-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/settings/settings-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "settings-plugin",
    version: "1.0.0",
    exposedModules: {
      SettingsPage: p("./src/plugins/settings-plugin/SettingsPage.tsx"),
      SettingsIcon: p("./src/plugins/settings-plugin/SettingsIcon.tsx"),
      AuthSettingsPage: p("./src/plugins/setup-plugin/InitialSetupForm.tsx"),
      AuthIcon: p("./src/plugins/settings-plugin/AuthIcon.tsx"),
      ExtensionsPage: p("./src/plugins/setup-plugin/WhatsNextPage.tsx"),
      ExtensionsIcon: p("./src/plugins/setup-plugin/ExtensionsIcon.tsx"),
    },
  },
});

const SetupPlugin = new FleetshiftPlugin({
  extensions: [
    createSetup({
      id: "initial-setup",
      label: "Authentication",
      description: "Configure authentication provider and backing store.",
      path: "auth",
      component: { $codeRef: "InitialSetupForm.default" },
      requires: [],
      requiresAuth: false,
      priority: 0,
    }),
    createSetup({
      id: "whats-next",
      label: "What's Next",
      description: "Configure addons and integrations.",
      path: "whats-next",
      component: { $codeRef: "WhatsNextPage.default" },
      requires: ["signing-key-enrollment"],
      requiresAuth: true,
      priority: 100,
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/setup/setup-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/setup/setup-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "setup-plugin",
    version: "1.0.0",
    exposedModules: {
      InitialSetupForm: p("./src/plugins/setup-plugin/InitialSetupForm.tsx"),
      WhatsNextPage: p("./src/plugins/setup-plugin/WhatsNextPage.tsx"),
      SetupProgress: p("./src/plugins/setup-plugin/useSetupProgress.ts"),
    },
  },
});

const ConfigurationPlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "configuration",
      label: "Configuration",
      component: { $codeRef: "ConfigurationPage.default" },
      icon: { $codeRef: "ConfigurationIcon.default" },
      description:
        "Deploy and manage applications across your OpenShift fleet using GitOps and Helm. Keep workloads consistent as you scale from a single cluster to many.",
      keywords: ["configuration", "gitops", "helm", "deploy", "applications"],
    }),
  ],
  sharedModules,
  entryScriptFilename:
    "plugins/configuration/configuration-plugin.[contenthash].js",
  pluginManifestFilename:
    "plugins/configuration/configuration-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "configuration-plugin",
    version: "1.0.0",
    exposedModules: {
      ConfigurationPage: p(
        "./src/plugins/configuration-plugin/ConfigurationPage.tsx",
      ),
      ConfigurationIcon: p(
        "./src/plugins/configuration-plugin/ConfigurationIcon.tsx",
      ),
    },
  },
});

const VirtualizationPlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "virtualization",
      label: "Virtualization",
      component: { $codeRef: "VirtualizationPage.default" },
      icon: { $codeRef: "VirtualizationIcon.default" },
      description:
        "Run and manage virtual machines alongside containers across your fleet with live migration and snapshot support.",
      keywords: [
        "virtualization",
        "vm",
        "virtual machine",
        "kubevirt",
        "migration",
      ],
    }),
  ],
  sharedModules,
  entryScriptFilename:
    "plugins/virtualization/virtualization-plugin.[contenthash].js",
  pluginManifestFilename:
    "plugins/virtualization/virtualization-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "virtualization-plugin",
    version: "1.0.0",
    exposedModules: {
      VirtualizationPage: p(
        "./src/plugins/virtualization-plugin/VirtualizationPage.tsx",
      ),
      VirtualizationIcon: p(
        "./src/plugins/virtualization-plugin/VirtualizationIcon.tsx",
      ),
    },
  },
});

const AddonDemoPlugin = new FleetshiftPlugin({
  extensions: [
    // --- Cluster providers ---
    createClusterProvider({
      id: "eks",
      label: "Amazon EKS",
      description:
        "Create a managed Kubernetes cluster on Amazon Elastic Kubernetes Service.",
      keywords: ["eks", "aws", "amazon", "elastic kubernetes"],
      to: { search: "?create=eks" },
      icon: { $codeRef: "EksProvider.EksIcon" },
      card: { $codeRef: "EksProvider.EksProviderCard" },
      wizard: { $codeRef: "EksProvider.EksWizard" },
      searchIcon: { $codeRef: "EksIcon.default" },
    }),
    createClusterProvider({
      id: "aks",
      label: "Azure AKS",
      description:
        "Create a managed Kubernetes cluster on Azure Kubernetes Service.",
      keywords: ["aks", "azure", "microsoft"],
      to: { search: "?create=aks" },
      icon: { $codeRef: "AksProvider.AksIcon" },
      card: { $codeRef: "AksProvider.AksProviderCard" },
      wizard: { $codeRef: "AksProvider.AksWizard" },
      searchIcon: { $codeRef: "AksIcon.default" },
    }),
    createClusterProvider({
      id: "rosa",
      label: "ROSA",
      description:
        "Create a Red Hat OpenShift Service on AWS (ROSA) managed cluster.",
      keywords: ["rosa", "openshift", "red hat", "aws"],
      to: { search: "?create=rosa" },
      icon: { $codeRef: "RosaProvider.RosaIcon" },
      card: { $codeRef: "RosaProvider.RosaProviderCard" },
      wizard: { $codeRef: "RosaProvider.RosaWizard" },
      searchIcon: { $codeRef: "RosaIcon.default" },
    }),
    createClusterProvider({
      id: "vsphere",
      label: "vSphere",
      description:
        "Create an OpenShift cluster on VMware vSphere infrastructure.",
      keywords: ["vsphere", "vmware", "on-premise", "datacenter"],
      to: { search: "?create=vsphere" },
      icon: { $codeRef: "VsphereProvider.VsphereIcon" },
      card: { $codeRef: "VsphereProvider.VsphereProviderCard" },
      wizard: { $codeRef: "VsphereProvider.VsphereWizard" },
      searchIcon: { $codeRef: "VsphereIcon.default" },
    }),

    // --- Provider onboarding ---
    createOnboardingAction({
      id: "eks-connect",
      label: "Amazon EKS",
      description: "Link your AWS account to import and manage EKS clusters.",
      icon: { $codeRef: "EksIcon.default" },
      card: { $codeRef: "EksOnboarding.EksOnboardingCard" },
      form: { $codeRef: "EksOnboarding.EksOnboardingForm" },
      category: "fleetshift.cluster-provider",
    }),
    createOnboardingAction({
      id: "aks-connect",
      label: "Azure AKS",
      description:
        "Link your Azure subscription to import and manage AKS clusters.",
      icon: { $codeRef: "AksIcon.default" },
      card: { $codeRef: "AksOnboarding.AksOnboardingCard" },
      form: { $codeRef: "AksOnboarding.AksOnboardingForm" },
      category: "fleetshift.cluster-provider",
    }),
    createOnboardingAction({
      id: "rosa-connect",
      label: "ROSA",
      description:
        "Link your Red Hat OpenShift on AWS account to manage ROSA clusters.",
      icon: { $codeRef: "RosaIcon.default" },
      card: { $codeRef: "RosaOnboarding.RosaOnboardingCard" },
      form: { $codeRef: "RosaOnboarding.RosaOnboardingForm" },
      category: "fleetshift.cluster-provider",
    }),
    createOnboardingAction({
      id: "vsphere-connect",
      label: "vSphere",
      description:
        "Link your VMware vSphere environment to deploy and manage clusters.",
      icon: { $codeRef: "VsphereIcon.default" },
      card: { $codeRef: "VsphereOnboarding.VsphereOnboardingCard" },
      form: { $codeRef: "VsphereOnboarding.VsphereOnboardingForm" },
      category: "fleetshift.cluster-provider",
    }),
    createOnboardingAction({
      id: "kind-connect",
      label: "Kind",
      description:
        "Configure a local Kind cluster for development and testing.",
      icon: { $codeRef: "KindOnboardingIcon.default" },
      card: { $codeRef: "KindOnboarding.KindOnboardingCard" },
      form: { $codeRef: "KindOnboarding.KindOnboardingForm" },
      category: "fleetshift.cluster-provider",
    }),

    // --- Module pages ---
    createModule({
      id: "security",
      label: "Security",
      component: { $codeRef: "SecurityPage.default" },
      icon: { $codeRef: "SecurityIcon.default" },
      description:
        "Scan images, enforce admission policies, and monitor compliance across your fleet.",
      keywords: [
        "security",
        "vulnerability",
        "compliance",
        "policy",
        "admission",
        "scan",
        "cve",
      ],
    }),
    createModule({
      id: "observability",
      label: "Observability",
      component: { $codeRef: "ObservabilityPage.default" },
      icon: { $codeRef: "ObservabilityIcon.default" },
      description:
        "Unified metrics, logs, and traces across your fleet from a single pane of glass.",
      keywords: [
        "observability",
        "monitoring",
        "metrics",
        "logs",
        "traces",
        "alerting",
      ],
    }),
    createModule({
      id: "compliance",
      label: "Compliance",
      component: { $codeRef: "CompliancePage.default" },
      icon: { $codeRef: "ComplianceIcon.default" },
      description:
        "Track regulatory compliance and audit readiness across your fleet.",
      keywords: ["compliance", "audit", "regulation", "policy", "governance"],
    }),

    // --- Module onboarding ---
    createOnboardingAction({
      id: "security-enable",
      label: "Security",
      description:
        "Scan images, enforce admission policies, and monitor compliance.",
      icon: { $codeRef: "SecurityIcon.default" },
      card: { $codeRef: "SecurityOnboardingCard.SecurityOnboardingCard" },
      form: { $codeRef: "SecurityOnboardingForm.default" },
      category: "fleetshift.module",
    }),
    createOnboardingAction({
      id: "observability-enable",
      label: "Observability",
      description: "Unified metrics, logs, and traces across your fleet.",
      icon: { $codeRef: "ObservabilityIcon.default" },
      card: {
        $codeRef: "ObservabilityOnboardingCard.ObservabilityOnboardingCard",
      },
      form: { $codeRef: "ObservabilityOnboardingForm.default" },
      category: "fleetshift.module",
    }),
    createOnboardingAction({
      id: "compliance-enable",
      label: "Compliance",
      description: "Track regulatory compliance and audit readiness.",
      icon: { $codeRef: "ComplianceIcon.default" },
      card: { $codeRef: "ComplianceOnboardingCard.ComplianceOnboardingCard" },
      form: { $codeRef: "ComplianceOnboardingForm.default" },
      category: "fleetshift.module",
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/addon-demo/addon-demo-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/addon-demo/addon-demo-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "addon-demo-plugin",
    version: "1.0.0",
    exposedModules: {
      // Provider cards + wizards
      EksProvider: p(
        "./src/plugins/addon-demo-plugin/providers/EksProvider.tsx",
      ),
      AksProvider: p(
        "./src/plugins/addon-demo-plugin/providers/AksProvider.tsx",
      ),
      RosaProvider: p(
        "./src/plugins/addon-demo-plugin/providers/RosaProvider.tsx",
      ),
      VsphereProvider: p(
        "./src/plugins/addon-demo-plugin/providers/VsphereProvider.tsx",
      ),
      // Provider icons
      EksIcon: p("./src/plugins/addon-demo-plugin/icons/EksIcon.tsx"),
      AksIcon: p("./src/plugins/addon-demo-plugin/icons/AksIcon.tsx"),
      RosaIcon: p("./src/plugins/addon-demo-plugin/icons/RosaIcon.tsx"),
      VsphereIcon: p("./src/plugins/addon-demo-plugin/icons/VsphereIcon.tsx"),
      // Provider onboarding
      EksOnboarding: p(
        "./src/plugins/addon-demo-plugin/providers/EksOnboarding.tsx",
      ),
      AksOnboarding: p(
        "./src/plugins/addon-demo-plugin/providers/AksOnboarding.tsx",
      ),
      RosaOnboarding: p(
        "./src/plugins/addon-demo-plugin/providers/RosaOnboarding.tsx",
      ),
      VsphereOnboarding: p(
        "./src/plugins/addon-demo-plugin/providers/VsphereOnboarding.tsx",
      ),
      KindOnboarding: p(
        "./src/plugins/addon-demo-plugin/providers/KindOnboarding.tsx",
      ),
      KindOnboardingIcon: p("./src/plugins/kind-plugin/KindIcon.tsx"),
      // Module pages
      SecurityPage: p(
        "./src/plugins/addon-demo-plugin/security/SecurityPage.tsx",
      ),
      ObservabilityPage: p(
        "./src/plugins/addon-demo-plugin/observability/ObservabilityPage.tsx",
      ),
      CompliancePage: p(
        "./src/plugins/addon-demo-plugin/compliance/CompliancePage.tsx",
      ),
      // Module icons
      SecurityIcon: p("./src/plugins/addon-demo-plugin/icons/SecurityIcon.tsx"),
      ObservabilityIcon: p(
        "./src/plugins/addon-demo-plugin/icons/ObservabilityIcon.tsx",
      ),
      ComplianceIcon: p(
        "./src/plugins/addon-demo-plugin/icons/ComplianceIcon.tsx",
      ),
      // Module onboarding
      SecurityOnboardingCard: p(
        "./src/plugins/addon-demo-plugin/onboarding/SecurityOnboardingCard.tsx",
      ),
      ObservabilityOnboardingCard: p(
        "./src/plugins/addon-demo-plugin/onboarding/ObservabilityOnboardingCard.tsx",
      ),
      ComplianceOnboardingCard: p(
        "./src/plugins/addon-demo-plugin/onboarding/ComplianceOnboardingCard.tsx",
      ),
      SecurityOnboardingForm: p(
        "./src/plugins/addon-demo-plugin/onboarding/SecurityOnboardingForm.tsx",
      ),
      ObservabilityOnboardingForm: p(
        "./src/plugins/addon-demo-plugin/onboarding/ObservabilityOnboardingForm.tsx",
      ),
      ComplianceOnboardingForm: p(
        "./src/plugins/addon-demo-plugin/onboarding/ComplianceOnboardingForm.tsx",
      ),
    },
  },
});

const pluginConfigs = [
  { plugin: OverviewPlugin, key: "overview" },
  { plugin: ManagementPlugin, key: "management" },
  { plugin: CorePlugin, key: "core" },
  { plugin: SigningPlugin, key: "signing" },
  { plugin: RoutingPlugin, key: "routing" },
  { plugin: GcpHcpPlugin, key: "gcphcp" },
  { plugin: KindPlugin, key: "kind" },
  { plugin: SetupPlugin, key: "setup" },
  { plugin: ConfigurationPlugin, key: "configuration" },
  { plugin: VirtualizationPlugin, key: "virtualization" },
  { plugin: AddonDemoPlugin, key: "addon-demo" },
  { plugin: SettingsPlugin, key: "settings" },
] as const;

const configs: Configuration[] = pluginConfigs.map(({ plugin, key }) => ({
  name: key,
  entry: {
    mock: path.resolve(configDir, "./src/index.ts"),
  },
  output: {
    publicPath: "auto",
    chunkFilename: `plugins/${key}/[name].js`,
    assetModuleFilename: `plugins/${key}/assets/[hash][ext]`,
    uniqueName: key,
  },
  mode: "development" as const,
  ignoreWarnings: [/Plugin base URL/, /Plugin has no extensions/],
  stats: {
    preset: "normal",
    colors: true,
    timings: true,
    modules: false,
  },
  plugins: [
    plugin,
    new rspack.DefinePlugin({
      "process.env.DRAGGABLE_DEBUG": "false",
    }),
    createPfModuleReplacementPlugin(monorepoRoot),
    new rspack.NormalModuleReplacementPlugin(
      /^@patternfly\/react-core\/dist\/esm\/(components|helpers|layouts)(\/|$)/,
      (resource) => {
        const compMatch = resource.request.match(
          /^(@patternfly\/react-core\/)dist\/esm\/((?:components|layouts)\/[^/]+)/,
        );
        if (compMatch) {
          resource.request = `${compMatch[1]}dist/dynamic/${compMatch[2]}`;
          return;
        }
        resource.request = resource.request.replace(
          "/dist/esm/",
          "/dist/dynamic/",
        );
        resource.request = resource.request.replace(/\.(js|mjs)$/, "");
      },
    ),
  ],
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
    fallback: {
      cookie: false,
      "set-cookie-parser": false,
    },
  },
  module: {
    rules: [
      swcLoaderRule,
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
      },
      {
        test: /\.scss$/,
        use: ["style-loader", "css-loader", "sass-loader"],
      },
      {
        test: /\.(png|jpe?g|gif|svg|webp)$/i,
        type: "asset/resource",
      },
    ],
  },
}));

export default configs;
