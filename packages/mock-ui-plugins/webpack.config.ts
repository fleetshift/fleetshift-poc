/* eslint-disable @typescript-eslint/ban-ts-comment */
import path from "path";
import {
  ModuleFederationPlugin as BaseMFPlugin,
  ContainerPlugin,
} from "@module-federation/enhanced";
import { DynamicRemotePlugin } from "@openshift/dynamic-plugin-sdk-webpack";
import { getDynamicModules, createTsLoaderRule } from "@fleetshift/build-utils";
import webpack from "webpack";
import type { Configuration } from "webpack";

const monorepoRoot = path.resolve(__dirname, "../..");
const nodeModulesRoot = path.resolve(monorepoRoot, "node_modules");
const pfSharedModules = getDynamicModules(__dirname, monorepoRoot);
const tsLoaderRule = {
  ...createTsLoaderRule({ nodeModulesRoot }),
  exclude: [/node_modules/, /packages\/common\/dist/],
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

// Wrap MF plugin to disable federated type generation (dts-plugin crashes in Docker)
class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false });
  }
}

// @ts-ignore — @module-federation/enhanced types differ from SDK expectations
const mfOverride = {
  libraryType: "global",
  pluginOverride: {
    ModuleFederationPlugin,
    ContainerPlugin,
  },
};

const p = (rel: string) => path.resolve(__dirname, rel);

const ManagementPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        id: "targets",
        label: "Targets",
        component: { $codeRef: "TargetsPage.default" },
        description: "View and manage deployment targets",
        keywords: ["target", "deploy", "rollout"],
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/management/management-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/management/management-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "management-plugin",
    version: "1.0.0",
    exposedModules: {
      TargetsPage: p("./src/plugins/management-plugin/TargetsPage.tsx"),
    },
  },
});

const DayOnePlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        id: "day-one",
        label: "Day One",
        component: { $codeRef: "DayOnePage.default" },
        description: "Initial setup and onboarding",
        keywords: ["setup", "onboarding", "welcome"],
        extensionPoints: {
          steps: {
            description: "Setup steps shown as cards on the Day One page",
            type: "fleetshift.setup",
          },
        },
      },
    },
    {
      type: "fleetshift.setup",
      properties: {
        id: "auth-setup",
        label: "Authentication",
        description:
          "Configure authentication provider and backing store for your management engine.",
        path: "auth",
        component: { $codeRef: "InitialSetupForm.default" },
        requires: [],
        requiresAuth: false,
      },
    },
    {
      type: "fleetshift.setup",
      properties: {
        id: "cluster-deploy",
        label: "Deploy Cluster",
        description:
          "Select a cluster provider and deploy your first managed cluster.",
        path: "deploy",
        component: { $codeRef: "SetupClusterDeploy.default" },
        requires: ["signing-key-enrollment"],
        requiresAuth: true,
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/day-one/day-one-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/day-one/day-one-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "day-one-plugin",
    version: "1.0.0",
    exposedModules: {
      DayOnePage: p("./src/plugins/day-one-plugin/DayOnePage.tsx"),
      InitialSetupForm: p("./src/plugins/day-one-plugin/InitialSetupForm.tsx"),
      SetupClusterDeploy: p(
        "./src/plugins/day-one-plugin/cluster-providers/SetupClusterDeploy.tsx",
      ),
    },
  },
});

const CorePlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        id: "clusters",
        label: "Clusters",
        component: { $codeRef: "ClustersModule.default" },
        description: "View and manage your fleet of clusters",
        keywords: ["cluster", "fleet", "manage"],
      },
    },
    {
      type: "fleetshift.module",
      properties: {
        id: "create-cluster",
        label: "Create Cluster",
        component: { $codeRef: "CreateClusterModule.default" },
        description: "Launch the cluster creation wizard",
        keywords: ["cluster", "create", "deploy", "provision", "wizard"],
        searchResult: { $codeRef: "CreateClusterSearchResult.default" },
        extensionPoints: {
          providers: {
            description: "Cluster providers available in the creation wizard",
            type: "fleetshift.cluster-provider",
          },
        },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/core/core-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/core/core-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "core-plugin",
    version: "1.0.0",
    exposedModules: {
      ClustersModule: p("./src/plugins/core-plugin/ClustersModule.tsx"),
      CreateClusterModule: p(
        "./src/plugins/core-plugin/CreateClusterModule.tsx",
      ),
      CreateClusterSearchResult: p(
        "./src/plugins/core-plugin/CreateClusterSearchResult.tsx",
      ),
    },
  },
});

const SigningPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        id: "signing-keys",
        label: "Signing Keys",
        component: { $codeRef: "SigningKeyEnrollment.default" },
        description: "Manage signing keys for deployment verification",
        keywords: ["signing", "key", "enrollment", "cosign"],
      },
    },
    {
      type: "fleetshift.setup",
      properties: {
        id: "signing-key-enrollment",
        label: "Signing Key Enrollment",
        description:
          "Enroll signing keys for deployment verification and supply chain security.",
        path: "enroll",
        component: { $codeRef: "SigningKeyEnrollment.default" },
        requires: ["auth-setup"],
        requiresAuth: true,
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/signing/signing-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/signing/signing-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "signing-plugin",
    version: "1.0.0",
    exposedModules: {
      SigningKeyEnrollment: p(
        "./src/plugins/signing-plugin/SigningKeyEnrollment.tsx",
      ),
      useSigningKey: p("./src/plugins/signing-plugin/useSigningKey.ts"),
      signingKeyApi: p("./src/plugins/signing-plugin/signingKeyApi.ts"),
    },
  },
});

const RoutingPlugin = new DynamicRemotePlugin({
  extensions: [],
  sharedModules,
  entryScriptFilename: "plugins/routing/routing-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/routing/routing-plugin-manifest.json",
  // @ts-ignore
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

const GcpHcpPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.cluster-provider",
      properties: {
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
        to: { pathname: "/gcphcp" },
        icon: { $codeRef: "GcpHcpProviderCard.GcpHcpIcon" },
        card: { $codeRef: "GcpHcpProviderCard.default" },
        wizard: { $codeRef: "CreateGcpHcpWizard.default" },
        searchIcon: { $codeRef: "GcpHcpIcon.default" },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/gcphcp/gcphcp-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/gcphcp/gcphcp-plugin-manifest.json",
  // @ts-ignore
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
    },
  },
});

const OverviewPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.module",
      properties: {
        id: "overview",
        label: "Overview",
        component: { $codeRef: "OverviewDashboard.default" },
        description: "Fleet overview dashboard",
        keywords: ["overview", "dashboard", "summary"],
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/overview/overview-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/overview/overview-plugin-manifest.json",
  // @ts-ignore
  moduleFederationSettings: mfOverride,
  pluginMetadata: {
    name: "overview-plugin",
    version: "1.0.0",
    exposedModules: {
      OverviewDashboard: p(
        "./src/plugins/overview-plugin/OverviewDashboard.tsx",
      ),
    },
  },
});

const KindPlugin = new DynamicRemotePlugin({
  extensions: [
    {
      type: "fleetshift.cluster-provider",
      properties: {
        id: "kind",
        label: "Kind",
        description: "Create a local Kind cluster for development and testing.",
        keywords: ["kind", "local", "development", "testing"],
        to: { pathname: "/kind" },
        icon: { $codeRef: "KindProviderCard.KindIcon" },
        card: { $codeRef: "KindProviderCard.default" },
        wizard: { $codeRef: "CreateClusterWizard.default" },
        searchIcon: { $codeRef: "KindIcon.default" },
      },
    },
  ],
  sharedModules,
  entryScriptFilename: "plugins/kind/kind-plugin.[contenthash].js",
  pluginManifestFilename: "plugins/kind/kind-plugin-manifest.json",
  // @ts-ignore
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

const pluginConfigs = [
  { plugin: OverviewPlugin, key: "overview" },
  { plugin: ManagementPlugin, key: "management" },
  { plugin: DayOnePlugin, key: "day-one" },
  { plugin: CorePlugin, key: "core" },
  { plugin: SigningPlugin, key: "signing" },
  { plugin: RoutingPlugin, key: "routing" },
  { plugin: GcpHcpPlugin, key: "gcphcp" },
  { plugin: KindPlugin, key: "kind" },
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
  mode: "development",
  cache: {
    type: "filesystem",
    name: key,
  },
  plugins: [
    plugin,
    new webpack.DefinePlugin({
      "process.env.DRAGGABLE_DEBUG": "false",
    }),
  ],
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
  },
  module: {
    rules: [
      tsLoaderRule,
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
