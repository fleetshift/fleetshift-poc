import { useCallback, useEffect, useState } from "react";

import {
  getSigningKeyStatus,
  signDeployment as signDeploy,
} from "./signingKeyApi";

export function useSigningKey() {
  const [enrolled, setEnrolled] = useState(false);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getSigningKeyStatus().then((s) => {
      if (!cancelled) {
        setEnrolled(s.enrolled);
        setLoaded(true);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const signDeployment = useCallback(
    (envelopeBytes: Uint8Array): Promise<string> => signDeploy(envelopeBytes),
    [],
  );

  return { loaded, enrolled, signDeployment };
}
