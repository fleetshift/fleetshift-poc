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
        "selector-class-pattern": pluginPattern("ocm-rm-", "ocm-rm-search, ocm-rm-search__menu"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/core-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-core-", "ocm-rm-core-clusters, ocm-rm-core-clusters__toolbar"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/overview-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-overview-", "ocm-rm-overview-dashboard, ocm-rm-overview-capacity__bar"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/day-one-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-day-one-", "ocm-rm-day-one-welcome, ocm-rm-day-one-welcome__card"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/signing-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-signing-", "ocm-rm-signing-keys, ocm-rm-signing-keys__form"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/management-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-mgmt-", "ocm-rm-mgmt-targets, ocm-rm-mgmt-targets__row"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/kind-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-kind-", "ocm-rm-kind-wizard, ocm-rm-kind-wizard__step"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/settings-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-settings-", "ocm-rm-settings-nav-order, ocm-rm-settings-nav-order__item"),
      },
    },
    {
      files: ["mock-ui-plugins/src/plugins/addon-demo-plugin/**/*.{scss,css}"],
      rules: {
        "selector-class-pattern": pluginPattern("ocm-rm-addon-", "ocm-rm-addon-security, ocm-rm-addon-security__severity-badges"),
      },
    },
  ],
};
