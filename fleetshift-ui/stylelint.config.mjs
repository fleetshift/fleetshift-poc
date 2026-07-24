const VENDOR = "pf-|react-|leaflet-";

function pluginPattern(prefix, example) {
  return [
    `^(${VENDOR}|${prefix})[a-z][a-z0-9-]*(__[a-z0-9-]+)?(--[a-z0-9-]+)?$`,
    { message: `Custom classes must start with '${prefix}' (e.g. ${example}). Vendor classes (pf-*, react-*, leaflet-*) are recognized but should be nested inside an ${prefix}* parent by convention.` },
  ];
}

/** @type {import('stylelint').Config} */
export default {
  extends: ["stylelint-config-standard-scss"],
  rules: {
    "selector-class-pattern": null,
    "custom-property-pattern": null,
    "scss/no-global-function-names": null,
    "no-descending-specificity": null,
  },
  overrides: [
    {
      files: ["packages/gui/src/**/*.scss", "packages/gui/src/**/*.css"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-", "ome-search, ome-search__menu"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/core-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-core-", "ome-core-clusters, ome-core-clusters__toolbar"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/overview-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-overview-", "ome-overview-dashboard, ome-overview-capacity__bar"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/gcphcp-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-gcphcp-", "ome-gcphcp-wizard, ome-gcphcp-wizard__step"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/day-one-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-day-one-", "ome-day-one-welcome, ome-day-one-welcome__card"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/signing-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-signing-", "ome-signing-keys, ome-signing-keys__form"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/management-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-mgmt-", "ome-mgmt-targets, ome-mgmt-targets__row"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/kind-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-kind-", "ome-kind-wizard, ome-kind-wizard__step"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/settings-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-settings-", "ome-settings-nav-order, ome-settings-nav-order__item"),
      },
    },
    {
      files: ["packages/mock-ui-plugins/src/plugins/addon-demo-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ome-addon-", "ome-addon-security, ome-addon-security__severity-badges"),
      },
    },
  ],
};
