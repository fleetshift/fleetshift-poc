import path from "path";
import * as mf from "@module-federation/enhanced";
import webpack from "webpack";
const { ModuleFederationPlugin: BaseMFPlugin } = mf;
// Disable federated type generation (dts-plugin crashes in Docker containers)
class ModuleFederationPlugin extends BaseMFPlugin {
  constructor(options: ConstructorParameters<typeof BaseMFPlugin>[0]) {
    super({ ...options, dts: false });
  }
}
import HtmlWebpackPlugin from "html-webpack-plugin";
import MiniCssExtractPlugin from "mini-css-extract-plugin";
import * as buildUtils from "@fleetshift/build-utils";
const { getDynamicModules, createTsLoaderRule } = buildUtils;
import type { Configuration } from "webpack";

const configDir = typeof __dirname === "string" ? __dirname : process.cwd();
const monorepoRoot = path.resolve(configDir, "../..");
const nodeModulesRoot = path.resolve(monorepoRoot, "node_modules");
const pfSharedModules = getDynamicModules(configDir, monorepoRoot);
const tsLoaderRule = createTsLoaderRule({ nodeModulesRoot });

const config: Configuration = {
  entry: "./src/index.ts",
  output: {
    publicPath: "/",
    path: path.resolve(configDir, "dist"),
    chunkFilename: "shell/[name].js",
    clean: true,
  },
  mode: "development",
  cache: {
    type: "filesystem",
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
    // Don't follow symlinks so workspace packages resolve via their package.json exports
    symlinks: false,
  },
  module: {
    rules: [
      { ...tsLoaderRule, exclude: [/node_modules/, /__tests__/] },
      {
        test: /\.css$/,
        use: [MiniCssExtractPlugin.loader, "css-loader"],
      },
      {
        test: /\.s[ac]ss$/,
        use: [MiniCssExtractPlugin.loader, "css-loader", "sass-loader"],
      },
      {
        test: /\.(png|jpe?g|gif|svg|ico)$/,
        type: "asset/resource",
      },
    ],
  },
  plugins: [
    new ModuleFederationPlugin({
      name: "fleetshift_shell",
      remotes: {},
      shared: {
        react: { singleton: true, requiredVersion: "^18" },
        "react/jsx-runtime": { singleton: true, requiredVersion: "^18" },
        "react-dom": { singleton: true, requiredVersion: "^18" },
        "react-router-dom": { singleton: true, requiredVersion: "^6" },
        "@scalprum/core": { singleton: true },
        "@scalprum/react-core": { singleton: true },
        "@openshift/dynamic-plugin-sdk": { singleton: true },
        "@fleetshift/common": { requiredVersion: "*", version: "*" },
        "oidc-client-ts": { singleton: true, requiredVersion: "*" },
        "react-oidc-context": { singleton: true, requiredVersion: "*" },
        ...pfSharedModules,
      },
    }),
    new MiniCssExtractPlugin({ chunkFilename: "shell/[name].css" }),
    new HtmlWebpackPlugin({
      template: "./src/index.html",
      favicon: "./src/assets/masthead.ico",
    }),
    new HtmlWebpackPlugin({
      template: "./src/silent-renew.html",
      filename: "silent-renew.html",
      inject: false,
    }),
    new webpack.DefinePlugin({
      // bug in draggable that is referencing process.env...
      "process.env.DRAGGABLE_DEBUG": "false",
    }),
  ],
};

export default config;
