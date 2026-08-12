import type { Configuration } from "@rspack/core";
import rspack from "@rspack/core";
import path from "path";
import { fileURLToPath } from "url";

const configDir = path.dirname(fileURLToPath(import.meta.url));

const config: Configuration = {
  cache: {
    type: "persistent",
    buildDependencies: [fileURLToPath(import.meta.url)],
  },
  entry: path.resolve(configDir, "src/index.tsx"),
  output: {
    publicPath: "/",
    path: path.resolve(configDir, "dist"),
    chunkFilename: "e2e-dummy-app/[name].js",
    clean: true,
  },
  mode: process.env.NODE_ENV === "development" ? "development" : "production",
  stats: {
    preset: "normal",
    colors: true,
    timings: true,
    modules: false,
  },
  resolve: {
    extensions: [".ts", ".tsx", ".js", ".jsx"],
    symlinks: false,
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        exclude: [/node_modules/, /__tests__/],
        loader: "builtin:swc-loader",
        options: {
          jsc: {
            parser: { syntax: "typescript", tsx: true },
            // TODO: enable reactCompiler: true when rspack >= 2.1.0
            transform: { react: { runtime: "automatic" } },
          },
        },
        type: "javascript/auto",
      },
      {
        test: /\.css$/,
        use: [rspack.CssExtractRspackPlugin.loader, "css-loader"],
      },
      {
        test: /\.s[ac]ss$/,
        use: [
          rspack.CssExtractRspackPlugin.loader,
          "css-loader",
          "sass-loader",
        ],
      },
      {
        test: /\.(png|jpe?g|gif|svg|ico)$/,
        type: "asset/resource",
      },
    ],
  },
  plugins: [
    new rspack.CssExtractRspackPlugin({ chunkFilename: "dummy/[name].css" }),
    new rspack.HtmlRspackPlugin({
      template: "./src/index.html",
    }),
    new rspack.HtmlRspackPlugin({
      template: "./src/index.html",
      filename: "login.html",
    }),
  ],
  devServer: {
    port: 8881,
    historyApiFallback: true,
  },
};

export default config;
