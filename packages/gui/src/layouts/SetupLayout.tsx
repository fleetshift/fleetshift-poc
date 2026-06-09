import { Outlet } from "react-router-dom";
import {
  Masthead,
  MastheadBrand,
  MastheadContent,
  MastheadLogo,
  MastheadMain,
  Toolbar,
  ToolbarContent,
  ToolbarGroup,
  ToolbarItem,
} from "@patternfly/react-core";
import { MoonIcon, SunIcon, WineGlassIcon } from "@patternfly/react-icons";
import logo from "../assets/masthead.png";
import ThemeToggle, { ThemeToggleType } from "../components/Themes/ThemeToggle";

import "./SetupLayout.scss";

const SetupMasthead = () => (
  <Masthead>
    <MastheadMain>
      <MastheadBrand>
        <MastheadLogo component="span">
          <img src={logo} alt="FleetShift" className="ome-masthead-logo" />
        </MastheadLogo>
      </MastheadBrand>
    </MastheadMain>
    <MastheadContent>
      <Toolbar>
        <ToolbarContent>
          <ToolbarGroup align={{ default: "alignEnd" }}>
            <ToolbarItem>
              <ThemeToggle
                theme={ThemeToggleType.Glass}
                OnIcon={WineGlassIcon}
                OffIcon={WineGlassIcon}
              />
            </ToolbarItem>
            <ToolbarItem>
              <ThemeToggle
                theme={ThemeToggleType.Dark}
                OnIcon={MoonIcon}
                OffIcon={SunIcon}
              />
            </ToolbarItem>
          </ToolbarGroup>
        </ToolbarContent>
      </Toolbar>
    </MastheadContent>
  </Masthead>
);

export const SetupLayout = () => (
  <div className="ome-app ome-setup-layout">
    <SetupMasthead />
    <div className="ome-setup-layout__scroll">
      <div className="ome-setup-layout__content">
        <Outlet />
      </div>
    </div>
  </div>
);
