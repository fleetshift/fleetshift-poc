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
      files: ["web/src/**/*.scss", "web/src/**/*.css"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-", "ocm-fleet-search, ocm-fleet-search__menu"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/core-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-core-", "ocm-fleet-core-clusters, ocm-fleet-core-clusters__toolbar"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/overview-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-overview-", "ocm-fleet-overview-dashboard, ocm-fleet-overview-capacity__bar"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/day-one-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-day-one-", "ocm-fleet-day-one-welcome, ocm-fleet-day-one-welcome__card"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/signing-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-signing-", "ocm-fleet-signing-keys, ocm-fleet-signing-keys__form"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/management-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-mgmt-", "ocm-fleet-mgmt-targets, ocm-fleet-mgmt-targets__row"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/kind-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-kind-", "ocm-fleet-kind-wizard, ocm-fleet-kind-wizard__step"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/settings-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-settings-", "ocm-fleet-settings-nav-order, ocm-fleet-settings-nav-order__item"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/addon-demo-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-fleet-addon-", "ocm-fleet-addon-security, ocm-fleet-addon-security__severity-badges"),
      },
    },
  ],
};
