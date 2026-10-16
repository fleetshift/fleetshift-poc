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
        <MastheadLogo component="span" className="ocm-rm-masthead-brand">
          <img src={logo} alt="" className="ocm-rm-masthead-logo" />
          <span>
            OCM Resource Manager <small>Experimental</small>
          </span>
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
  <div className="ocm-rm-app ocm-rm-setup-layout">
    <SetupMasthead />
    <div className="ocm-rm-setup-layout__scroll">
      <div className="ocm-rm-setup-layout__content">
        <Outlet />
      </div>
    </div>
  </div>
);
