import { useExtensionInstall } from "@fleetshift/common";
import { Icon, NavItem, Tooltip } from "@patternfly/react-core";
import { PuzzlePieceIcon } from "@patternfly/react-icons";
import clsx from "clsx";
import type { ComponentType } from "react";
import { Link, useLocation } from "react-router-dom";

import type { PluginPage } from "../../contexts/AppConfigContext";

interface AppNavItemProps {
  page: PluginPage;
  iconMap: Map<string, ComponentType>;
}

const AppNavItem = ({ page, iconMap }: AppNavItemProps) => {
  const location = useLocation();
  const { isInstalled } = useExtensionInstall();

  const fullPath = `/${page.path}`;
  const NavIcon = iconMap.get(page.title);
  const enabled = isInstalled(page.scope);
  const DisplayIcon = enabled ? NavIcon : PuzzlePieceIcon;

  const link = (
    <Link to={fullPath} className={clsx("pf-v6-c-nav__link")}>
      {DisplayIcon && (
        <Icon isInline className="pf-v6-u-mr-sm">
          <DisplayIcon />
        </Icon>
      )}
      {page.title}
    </Link>
  );

  return (
    <NavItem
      key={page.id}
      isActive={
        location.pathname === fullPath ||
        location.pathname.startsWith(fullPath + "/")
      }
    >
      {enabled ? (
        link
      ) : (
        <Tooltip
          position="right"
          content="This extension is not enabled. Click to enable it."
        >
          {link}
        </Tooltip>
      )}
    </NavItem>
  );
};

export default AppNavItem;
