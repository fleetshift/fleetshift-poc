import "./SetupLayout.scss";

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
import { Outlet } from "react-router-dom";

import logo from "../assets/masthead.png";
import ThemeDropdown from "../components/Themes/ThemeDropdown";

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
              <ThemeDropdown />
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
