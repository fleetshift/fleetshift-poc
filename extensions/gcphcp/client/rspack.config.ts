import {
  createClusterDetailTab,
  createClusterProvider,
  createCommonModuleReplacementPlugin,
  createCommonTransformImport,
  createOnboardingAction,
  createPfModuleReplacementPlugin,
  createPfTransformImport,
  createSearchResultRenderer,
  FleetshiftPlugin,
  getCommonDynamicModules,
  getDynamicModules,
} from "@fleetshift/build-utils";
import { ModuleFederationPlugin as BaseMFPlugin } from "@module-federation/enhanced/rspack";
import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";
import { fileURLToPath } from "url";
// @ts-ignore
import { BundleAnalyzerPlugin } from "webpack-bundle-analyzer";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const uiRoot = path.resolve(configDir, "..");
const pfSharedModules = getDynamicModules(configDir, uiRoot);
const commonSharedModules = getCommonDynamicModules();
const pfTransformImport = createPfTransformImport();
const commonTransformImport = createCommonTransformImport();

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
    transformImport: [...pfTransformImport, commonTransformImport],
  },
  type: "javascript/auto" as const,
};

const sharedModules = {
  react: { singleton: true, requiredVersion: "*" },
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
  ...pfSharedModules,
  ...commonSharedModules,
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
    createClusterDetailTab({
      id: "gcphcp-events",
      label: "Events",
      title: "Events",
      eventKey: "events",
      priority: 50,
      service: "gcphcp.fleetshift.io",
      component: { $codeRef: "GcpHcpDeliveryEventsTab.default" },
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
      GcpHcpProviderCard: p("./src/GcpHcpProviderCard.tsx"),
      CreateGcpHcpWizard: p("./src/CreateGcpHcpWizard.tsx"),
      GcpHcpIcon: p("./src/GcpHcpIcon.tsx"),
      GcpHcpOnboardingCard: p("./src/GcpHcpOnboardingCard.tsx"),
      GcpHcpConnectionForm: p("./src/GcpHcpConnectionForm.tsx"),
      GcpHcpSearchResult: p("./src/GcpHcpSearchResult.tsx"),
      GcpHcpDeliveryEventsTab: p("./src/GcpHcpDeliveryEventsTab.tsx"),
    },
  },
});

const pluginConfigs = [{ plugin: GcpHcpPlugin, key: "gcphcp" }] as const;

const configs: Configuration[] = pluginConfigs.map(({ plugin, key }) => ({
  name: key,
  cache: {
    type: "persistent" as const,
    name: key,
    buildDependencies: [fileURLToPath(import.meta.url)],
  },
  entry: {
    mock: path.resolve(configDir, "./src/index.ts"),
  },
  output: {
    publicPath: "auto",
    chunkFilename: `plugins/${key}/[name].js`,
    assetModuleFilename: `plugins/${key}/assets/[hash][ext]`,
    uniqueName: key,
  },
  mode: (process.env.NODE_ENV === "development" ? "development" : "production") as Configuration["mode"],
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
    createPfModuleReplacementPlugin(uiRoot),
    createCommonModuleReplacementPlugin(),
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
    ...(process.env.ANALYZE_BUNDLE === "true"
      ? [
          new BundleAnalyzerPlugin({
            // analyzerMode: "json",
          }),
        ]
      : []),
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
