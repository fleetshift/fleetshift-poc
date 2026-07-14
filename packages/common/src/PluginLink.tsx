import { useScalprum } from "@scalprum/react-core";
import { forwardRef, type ReactNode } from "react";
import { Link, type LinkProps, type To } from "react-router-dom";

import type { FleetShiftApi } from "./scalprum.js";

export interface PluginLinkProps extends Omit<LinkProps, "to"> {
  scope: string;
  module: string;
  to?: To;
  fallback?: ReactNode;
  children?: ReactNode;
}

const PluginLink = forwardRef<HTMLAnchorElement, PluginLinkProps>(
  ({ scope, module, to, fallback = null, children, ...rest }, ref) => {
    const { api } = useScalprum<{ api: FleetShiftApi }>();
    const basePath = api.fleetshift.getPluginPagePath(scope, module);

    if (!basePath) {
      return <>{fallback}</>;
    }

    const resolvedTo: To = !to
      ? basePath
      : typeof to === "string"
        ? to
          ? `${basePath}/${to}`
          : basePath
        : {
            ...to,
            pathname: to.pathname ? `${basePath}/${to.pathname}` : basePath,
          };

    return (
      <Link to={resolvedTo} {...rest} ref={ref}>
        {children}
      </Link>
    );
  },
);

export default PluginLink;
