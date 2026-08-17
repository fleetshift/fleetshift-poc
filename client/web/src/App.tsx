import { AnimationsProvider } from "@patternfly/react-core";
import { BrowserRouter } from "react-router-dom";

import { APP_BASENAME } from "./appBase";
import ScopeInitializer from "./components/Root/ScopeInitializer";
import Routes from "./routes/Routes";

export const App = () => (
  <AnimationsProvider config={{ hasAnimations: true }}>
    <ScopeInitializer>
      <BrowserRouter basename={APP_BASENAME}>
        <Routes />
      </BrowserRouter>
    </ScopeInitializer>
  </AnimationsProvider>
);
