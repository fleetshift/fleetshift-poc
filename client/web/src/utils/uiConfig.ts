import { createSharedStore } from "@scalprum/core";
import { AuthProviderNoUserManagerProps } from "react-oidc-context";

import { UIConfig } from "../auth/oidcConfig";

const IDP_SELECTED_KEY = "@@ome/idp_selected";

export const UI_CONFIG_EVENTS = ["INIT", "SELECT_IDP", "SET_FETCHING"] as const;
let IDP_SELECTED;

try {
  IDP_SELECTED = JSON.parse(localStorage.getItem(IDP_SELECTED_KEY) ?? "null");
} catch (error) {
  console.error("Failed to parse IDP_SELECTED from localStorage:", error);
  IDP_SELECTED = null;
}

interface LocalUIConfig extends UIConfig {
  fetching: boolean;
  ready: boolean;
  selectedIDP?: AuthProviderNoUserManagerProps;
}

export const uiConfigStore = createSharedStore<
  LocalUIConfig,
  typeof UI_CONFIG_EVENTS
>({
  onEventChange(prevState, event, payload) {
    if (event === "INIT") {
      return {
        ...prevState,
        ...payload,
        ready: true,
      };
    }
    if (event === "SELECT_IDP") {
      localStorage.setItem(IDP_SELECTED_KEY, JSON.stringify(payload));
      return {
        ...prevState,
        selectedIDP: payload,
      };
    }
    if (event === "SET_FETCHING") {
      return {
        ...prevState,
        fetching: payload,
      };
    }
    return prevState;
  },
  events: UI_CONFIG_EVENTS,
  initialState: {
    fetching: false,
    ready: false,
    selectedIDP: IDP_SELECTED,
    oidc: [],
  },
});
