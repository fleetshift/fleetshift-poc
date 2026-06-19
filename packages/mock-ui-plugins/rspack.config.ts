import {
  createClusterProvider,
  createModule,
  createOnboardingAction,
  createPfModuleReplacementPlugin,
  createPfTransformImport,
  createSetup,
  FleetshiftPlugin,
  getDynamicModules,
} from "@fleetshift/build-utils";
import { ModuleFederationPlugin as BaseMFPlugin } from "@module-federation/enhanced/rspack";
import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";

const monorepoRoot = path.resolve(__dirname, "../..");
const pfSharedModules = getDynamicModules(__dirname, monorepoRoot);
const pfTransformImport = createPfTransformImport();

const swcLoaderRule = {
  test: /\.tsx?$/,
  exclude: [/node_modules/, /packages\/common\/dist/],
  loader: "builtin:swc-loader" as const,
  options: {
    jsc: {
      parser: { syntax: "typescript" as const, tsx: true },
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
  "react/jsx-runtime": { singleton: true, requiredVersion: "^18" },
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

const p = (rel: string) => path.resolve(__dirname, rel);

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
    },
  },
});

const SettingsPlugin = new FleetshiftPlugin({
  extensions: [
    createModule({
      id: "settings",
      label: "Navigation",
      component: { $codeRef: "SettingsPage.default" },
      icon: { $codeRef: "SettingsIcon.default" },
      description: "Manage nav layout and workspace preferences",
      keywords: ["settings", "preferences", "nav", "order", "navigation"],
    }),
    createModule({
      id: "auth-settings",
      label: "Authentication",
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

const SecurityPlugin = new FleetshiftPlugin({
  extensions: [
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
      ],
    }),
  ],
  sharedModules,
  entryScriptFilename: "plugins/security/security-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/security/security-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "security-plugin",
    version: "1.0.0",
    exposedModules: {
      SecurityPage: p("./src/plugins/security-plugin/SecurityPage.tsx"),
      SecurityIcon: p("./src/plugins/security-plugin/SecurityIcon.tsx"),
    },
  },
});

const ObservabilityPlugin = new FleetshiftPlugin({
  extensions: [
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
  ],
  sharedModules,
  entryScriptFilename:
    "plugins/observability/observability-plugin.[contenthash].js",
  pluginManifestFilename:
    "plugins/observability/observability-plugin-manifest.json",
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "observability-plugin",
    version: "1.0.0",
    exposedModules: {
      ObservabilityPage: p(
        "./src/plugins/observability-plugin/ObservabilityPage.tsx",
      ),
      ObservabilityIcon: p(
        "./src/plugins/observability-plugin/ObservabilityIcon.tsx",
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
  { plugin: SecurityPlugin, key: "security" },
  { plugin: ObservabilityPlugin, key: "observability" },
  { plugin: SettingsPlugin, key: "settings" },
] as const;

const configs: Configuration[] = pluginConfigs.map(({ plugin, key }) => ({
  name: key,
  entry: {
    mock: path.resolve(__dirname, "./src/index.ts"),
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
