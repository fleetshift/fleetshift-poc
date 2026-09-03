import {
  ActionGroup,
  Button,
  Form,
  FormGroup,
  FormGroupLabelHelp,
  FormHelperText,
  HelperText,
  HelperTextItem,
  Masthead,
  MastheadContent,
  Menu,
  MenuContent,
  MenuItem,
  MenuList,
  Page,
  PageSection,
  Popover,
  TextInput,
  ToolbarGroup,
  ToolbarItem,
} from "@patternfly/react-core";
import { RhUiErrorFillIcon } from "@patternfly/react-icons";
import { useGetState } from "@scalprum/react-core";
import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  APP_BASENAME,
  AUTH_CALLBACK_PATH,
  isAuthCallbackPath,
  SILENT_RENEW_PATH,
  stripAppBasename,
  toBrowserPath,
} from "../../appBase";
import { oidcClientScope, OIDCConfig, UIConfig } from "../../auth/oidcConfig";
import { uiConfigStore } from "../../utils/uiConfig";
import ThemeDropdown from "../Themes/ThemeDropdown";

const DomainInput = ({
  setDomain,
}: {
  setDomain: (domain: string) => void;
}) => {
  const domainRef = useRef<HTMLInputElement>(null);
  const labelHelpRef = useRef<HTMLElement>(null);
  const [domainError, setDomainError] = useState(false);

  useEffect(() => {
    if (domainRef.current) {
      domainRef.current.value = "@";
      domainRef.current.focus();
    }
  }, []);

  const isValidDomain = (value: string) =>
    /^@[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/i.test(
      value,
    );

  return (
    <Form
      onSubmit={(e) => {
        e.preventDefault();
        const value = domainRef.current?.value.trim() ?? "";
        const valid = isValidDomain(value);
        setDomainError(!valid);
        if (valid) setDomain(value);
      }}
    >
      <FormGroup
        label="What is your domain"
        labelHelp={
          <Popover
            triggerRef={labelHelpRef}
            headerContent={<div>Your organization's domain</div>}
            bodyContent={
              <p>
                Your organization's domain is typically in the format of an
                email like @foobar.com.
              </p>
            }
          >
            <FormGroupLabelHelp
              ref={labelHelpRef}
              aria-label="More info for name field"
            />
          </Popover>
        }
        isRequired
        fieldId="domain-input"
      >
        <TextInput
          ref={domainRef}
          isRequired
          type="text"
          id="domain-input"
          name="domain-input"
          aria-describedby="domain-input-helper"
          validated={domainError ? "error" : "default"}
        />
        <FormHelperText id="domain-input-helper">
          <HelperText>
            <HelperTextItem
              icon={<RhUiErrorFillIcon />}
              variant={domainError ? "error" : "default"}
            >
              {domainError
                ? "Enter a valid domain like @foobar.com."
                : "Please enter your domain."}
            </HelperTextItem>
          </HelperText>
        </FormHelperText>
      </FormGroup>

      <ActionGroup>
        <Button type="submit" variant="primary">
          Submit
        </Button>
      </ActionGroup>
    </Form>
  );
};

const IDPRedirect = ({
  idpConfig,
  domain,
}: {
  idpConfig: UIConfig["oidc"];
  domain: string;
}) => {
  const navigate = useNavigate();
  if (!idpConfig || idpConfig.length === 0) {
    return <div>No OIDC config (should be empty state component)</div>;
  }

  function handleSelectIDP(selected: OIDCConfig) {
    if (!selected) {
      throw new Error("No OIDC auth methods configured");
    }
    const scope = oidcClientScope(selected.scope);

    const config = {
      authority: selected.authority,
      client_id: selected.clientId,
      redirect_uri: window.location.origin + AUTH_CALLBACK_PATH,
      silent_redirect_uri: window.location.origin + SILENT_RENEW_PATH,
      post_logout_redirect_uri: window.location.origin + APP_BASENAME + "/",
      response_type: "code",
      ...(scope !== undefined ? { scope } : {}),
      automaticSilentRenew: true,
      onSigninCallback: () => {
        let postLoginRedirect = window.sessionStorage.getItem(
          "post_login_redirect_pathname",
        );

        if (!postLoginRedirect || isAuthCallbackPath(postLoginRedirect)) {
          postLoginRedirect = `${APP_BASENAME}/`;
        }

        const browserPath = toBrowserPath(postLoginRedirect);
        window.history.replaceState({}, document.title, browserPath);
        navigate(stripAppBasename(browserPath), { replace: true });
      },
    };
    uiConfigStore.updateState("SELECT_IDP", config);
  }
  return (
    <div>
      <Menu>
        <MenuContent>
          <MenuList>
            {idpConfig
              .filter((idp) => idp.emailDomain === domain)
              .map((idp) => (
                <MenuItem
                  onClick={() => handleSelectIDP(idp)}
                  key={idp.clientId}
                >
                  {idp.name ?? idp.clientId}
                </MenuItem>
              ))}
          </MenuList>
        </MenuContent>
      </Menu>
    </div>
  );
};

const IDPSelection = () => {
  const [domain, setDomain] = useState("");
  const state = useGetState(uiConfigStore);
  return (
    <Page
      masthead={
        <Masthead>
          <MastheadContent>
            <ToolbarGroup align={{ default: "alignEnd" }}>
              <ToolbarItem>
                <ThemeDropdown />
              </ToolbarItem>
            </ToolbarGroup>
          </MastheadContent>
        </Masthead>
      }
      isManagedSidebar
      className="ome-app"
    >
      <PageSection isFilled hasOverflowScroll>
        {domain.length > 0 ? (
          <IDPRedirect idpConfig={state.oidc} domain={domain} />
        ) : (
          <DomainInput setDomain={setDomain} />
        )}
      </PageSection>
    </Page>
  );
};

export default IDPSelection;
