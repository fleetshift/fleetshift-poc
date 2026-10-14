import { memo } from "react";

import src from "./assets/gcp-icon.png";

const GcpHcpIcon = memo(() => {
  return <img src={src} width={16} height={16} />;
});

GcpHcpIcon.displayName = "GcpHcpIcon";

export default GcpHcpIcon;
