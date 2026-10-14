import { memo } from "react";

import src from "./assets/kind-logo.png";

const KindIcon = memo(() => {
  return <img src={src} width={16} height={16} />;
});

KindIcon.displayName = "KindIcon";

export default KindIcon;
