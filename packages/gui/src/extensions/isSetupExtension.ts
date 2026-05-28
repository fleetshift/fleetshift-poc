import { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import { ComponentType } from "react";

export interface SetupComponentProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

export type SetupExtension = Extension<
  "fleetshift.setup",
  {
    id: string;
    label: string;
    path: string;
    component: CodeRef<ComponentType<SetupComponentProps>>;
    requires: string[];
    requiresAuth: boolean;
    priority?: number;
  }
>;

export function isSetupExtension(e: Extension): e is SetupExtension {
  return e.type === "fleetshift.setup";
}
