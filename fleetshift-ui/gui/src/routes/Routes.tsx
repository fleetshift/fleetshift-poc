import { useLocation } from "react-router-dom";

import ConsoleRoutes from "./ConsoleRoutes";
import SetupRoutes from "./SetupRoutes";

const Routes = () => {
  const location = useLocation();

  if (location.pathname.startsWith("/setup")) {
    return <SetupRoutes />;
  }

  return <ConsoleRoutes />;
};

export default Routes;
