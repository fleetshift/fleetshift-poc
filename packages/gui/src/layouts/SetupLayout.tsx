import { useState, useCallback } from "react";
import { Outlet } from "react-router-dom";
import {
  Button,
  Masthead,
  MastheadBrand,
  MastheadContent,
  MastheadLogo,
  MastheadMain,
  Page,
  PageSection,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
} from "@patternfly/react-core";
import { MoonIcon, SunIcon } from "@patternfly/react-icons";
import logo from "../assets/masthead.png";
import "./AppLayout.scss";

const DARK_MODE_KEY = "fleetshift_dark_mode";
const DARK_CLASS = "pf-v6-theme-dark";

function initDarkMode(): boolean {
  const stored = localStorage.getItem(DARK_MODE_KEY);
  const isDark = stored === "true";
  if (isDark) {
    document.documentElement.classList.add(DARK_CLASS);
  }
  return isDark;
}

const DarkModeToggle = () => {
  const [dark, setDark] = useState(initDarkMode);

  const toggle = useCallback(() => {
    setDark((prev) => {
      const next = !prev;
      document.documentElement.classList.toggle(DARK_CLASS, next);
      localStorage.setItem(DARK_MODE_KEY, String(next));
      return next;
    });
  }, []);

  return (
    <Button variant="plain" aria-label="Toggle dark mode" onClick={toggle}>
      {dark ? <SunIcon /> : <MoonIcon />}
    </Button>
  );
};

const SetupMasthead = () => (
  <Masthead>
    <MastheadMain>
      <MastheadBrand>
        <MastheadLogo component="span">
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
      <Toolbar>
        <ToolbarContent>
          <ToolbarGroup align={{ default: "alignEnd" }}>
            <ToolbarItem>
              <DarkModeToggle />
            </ToolbarItem>
          </ToolbarGroup>
        </ToolbarContent>
      </Toolbar>
    </MastheadContent>
  </Masthead>
);

export const SetupLayout = () => (
  <Page masthead={<SetupMasthead />}>
    <PageSection isFilled hasOverflowScroll>
      <Outlet />
    </PageSection>
  </Page>
);
