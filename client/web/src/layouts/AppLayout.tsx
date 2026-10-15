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
import { useState } from "react";
import { Link, Outlet } from "react-router-dom";

import logo from "../assets/masthead.png";
import AppNav from "../components/AppNav/AppNav";
import FleetSearch from "../components/Search/FleetSearch";
import { SearchProvider } from "../components/Search/SearchProvider";
import ThemeDropdown from "../components/Themes/ThemeDropdown";
import { useAuth } from "../contexts/AuthContext";

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
          <MastheadLogo
            component="a"
            href="/"
            className="ocm-rm-masthead-brand"
          >
            <img src={logo} alt="" className="ocm-rm-masthead-logo" />
            <span>
              OCM Resource Manager <small>Experimental</small>
            </span>
          </MastheadLogo>
        </MastheadBrand>
      </MastheadMain>
      <MastheadContent>
        <Toolbar isFullHeight>
          <ToolbarContent>
            <ToolbarGroup
              className="pf-v6-u-flex-grow-1"
              variant="filter-group"
            >
              <FleetSearch />
            </ToolbarGroup>
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

const Sidebar = () => (
  <PageSidebar>
    <PageSidebarBody>
      <AppNav />
    </PageSidebarBody>
  </PageSidebar>
);

export const AppLayout = () => (
  <SearchProvider>
    <Page
      masthead={<AppMasthead />}
      sidebar={<Sidebar />}
      isManagedSidebar
      className="ocm-rm-app"
    >
      <PageSection isFilled hasOverflowScroll>
        <Outlet />
      </PageSection>
    </Page>
  </SearchProvider>
);
