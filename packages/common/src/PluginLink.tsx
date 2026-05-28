import type { ReactNode } from "react";
import { Link, type LinkProps, type To } from "react-router-dom";
import { useScalprum } from "@scalprum/react-core";
import type { FleetShiftApi } from "./scalprum.js";

export interface PluginLinkProps extends Omit<LinkProps, "to"> {
  scope: string;
  module: string;
  to?: To;
  fallback?: ReactNode;
  children: ReactNode;
}

const PluginLink = ({
  scope,
  module,
  to,
  fallback = null,
  children,
  ...rest
}: PluginLinkProps) => {
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
    <Link to={resolvedTo} {...rest}>
      {children}
    </Link>
  );
};

export default PluginLink;
