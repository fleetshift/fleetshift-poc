import { useMemo, useState } from "react";
import { Outlet, Link, useLocation } from "react-router-dom";
import {
  Divider,
  Dropdown,
  DropdownItem,
  DropdownList,
  Masthead,
  MastheadBrand,
  MastheadContent,
  MastheadLogo,
  MastheadMain,
  MastheadToggle,
  MenuToggle,
  Nav,
  NavItem,
  NavList,
  Page,
  PageSection,
  PageSidebar,
  PageSidebarBody,
  PageToggleButton,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
} from "@patternfly/react-core";
import { BarsIcon, BugIcon } from "@patternfly/react-icons";
import { useAuth } from "../contexts/AuthContext";
import { useAppConfig } from "../contexts/AppConfigContext";
import type { PluginPage } from "../contexts/AppConfigContext";
import ThemeDropdown from "../components/Themes/ThemeDropdown";
import logo from "../assets/masthead.png";

const AppMasthead = () => {
  const { user, logout } = useAuth();
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  return (
    <Masthead>
      <MastheadMain>
        <MastheadToggle>
          <PageToggleButton aria-label="Navigation toggle">
            <BarsIcon />
          </PageToggleButton>
        </MastheadToggle>
        <MastheadBrand>
          <MastheadLogo component="a" href="/">
            <img
              src={logo}
              alt="FleetShift"
              className="fs-masthead-logo"
              style={{ height: 36 }}
            />
          </MastheadLogo>
        </MastheadBrand>
      </MastheadMain>
      <MastheadContent>
        <Toolbar isFullHeight>
          <ToolbarContent>
            <ToolbarGroup align={{ default: "alignEnd" }}>
              <ToolbarItem>
                <ThemeDropdown />
              </ToolbarItem>
              <ToolbarItem>
                <Dropdown
                  isOpen={isMenuOpen}
                  onSelect={() => setIsMenuOpen(false)}
                  onOpenChange={setIsMenuOpen}
                  toggle={(toggleRef) => (
                    <MenuToggle
                      ref={toggleRef}
                      onClick={() => setIsMenuOpen((prev) => !prev)}
                      isExpanded={isMenuOpen}
                      isFullHeight
                    >
                      {user?.display_name ?? user?.username}
                    </MenuToggle>
                  )}
                >
                  <DropdownList>
                    <DropdownItem
                      icon={<BugIcon />}
                      component={(
                        props: React.HTMLAttributes<HTMLAnchorElement>,
                      ) => <Link to="/debug" {...props} />}
                    >
                      Debug
                    </DropdownItem>
                    <Divider />
                    <DropdownItem onClick={logout}>Log out</DropdownItem>
                  </DropdownList>
                </Dropdown>
              </ToolbarItem>
            </ToolbarGroup>
          </ToolbarContent>
        </Toolbar>
      </MastheadContent>
    </Masthead>
  );
};

const AppNav = () => {
  const location = useLocation();
  const { pluginPages, navLayout } = useAppConfig();

  const pageMap = useMemo(() => {
    const map = new Map<string, PluginPage>();
    for (const page of pluginPages) {
      map.set(page.id, page);
    }
    return map;
  }, [pluginPages]);

  const navItems = useMemo(() => {
    const items: PluginPage[] = [];
    for (const entry of navLayout) {
      if (entry.type === "page") {
        const page = pageMap.get(entry.pageId);
        if (page) items.push(page);
      }
    }
    return items.sort((a, b) => a.title.localeCompare(b.title));
  }, [navLayout, pageMap]);

  return (
    <Nav>
      <NavList>
        {navItems.map((page) => {
          const fullPath = `/${page.path}`;
          return (
            <NavItem
              key={page.id}
              isActive={
                location.pathname === fullPath ||
                location.pathname.startsWith(fullPath + "/")
              }
            >
              <Link to={fullPath}>{page.title}</Link>
            </NavItem>
          );
        })}
      </NavList>
    </Nav>
  );
};

const Sidebar = () => (
  <PageSidebar>
    <PageSidebarBody>
      <AppNav />
    </PageSidebarBody>
  </PageSidebar>
);

export const AppLayout = () => (
  <Page masthead={<AppMasthead />} sidebar={<Sidebar />} isManagedSidebar>
    <PageSection isFilled hasOverflowScroll>
      <Outlet />
    </PageSection>
  </Page>
);
