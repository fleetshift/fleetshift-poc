import {
  getCachedPfIcon,
  loadPfIcon,
  useExtensionInstall,
} from "@fleetshift/common";
import { Icon, NavItem, Tooltip } from "@patternfly/react-core";
import { PuzzlePieceIcon } from "@patternfly/react-icons";
import clsx from "clsx";
import type { ComponentType } from "react";
import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import type { PluginPage } from "../../contexts/AppConfigContext";

interface AppNavItemProps {
  page: PluginPage;
  iconMap: Map<string, ComponentType>;
  /** PF icon name override (e.g. "CogIcon"). Takes priority over plugin icon. */
  iconOverride?: string;
}

const AppNavItem = ({ page, iconMap, iconOverride }: AppNavItemProps) => {
  const location = useLocation();
  const { isInstalled } = useExtensionInstall();

  // Dynamic icon loading for overrides
  const [OverrideIcon, setOverrideIcon] = useState<ComponentType | null>(() =>
    iconOverride ? (getCachedPfIcon(iconOverride) ?? null) : null,
  );

  useEffect(() => {
    let active = true;
    if (!iconOverride) {
      setOverrideIcon(null);
      return;
    }
    const cached = getCachedPfIcon(iconOverride);
    if (cached) {
      setOverrideIcon(() => cached);
      return;
    }
    loadPfIcon(iconOverride).then((comp) => {
      if (active) setOverrideIcon(() => comp);
    });
    return () => {
      active = false;
    };
  }, [iconOverride]);

  const fullPath = `/${page.path}`;
  const NavIcon = iconMap.get(page.title);
  const enabled = isInstalled(page.scope);

  // Uninstalled extensions always show puzzle icon (higher priority than overrides)
  const DisplayIcon = enabled ? (OverrideIcon ?? NavIcon) : PuzzlePieceIcon;

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
