import "@patternfly/react-core/dist/styles/base.css";
import "@patternfly/patternfly/patternfly-addons.css";

import { createBrowserRouter, RouterProvider } from "react-router-dom";

import Home from "./Home";
import Login from "./Login";

const router = createBrowserRouter([
  {
    Component: Login,
    path: "/login",
  },
  {
    Component: Home,
    path: "/",
  },
]);

const App = () => {
  return <RouterProvider router={router} />;
};

export default App;
