import { AnimationsProvider } from "@patternfly/react-core";
import { BrowserRouter } from "react-router-dom";

import ScopeInitializer from "./components/Root/ScopeInitializer";
import Routes from "./routes/Routes";

export const App = () => (
  <AnimationsProvider config={{ hasAnimations: true }}>
    <ScopeInitializer>
      <BrowserRouter>
        <Routes />
      </BrowserRouter>
    </ScopeInitializer>
  </AnimationsProvider>
);
