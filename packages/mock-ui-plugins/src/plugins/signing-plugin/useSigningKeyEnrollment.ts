import { useCallback, useEffect, useRef, useState } from "react";
import { useAuth as useOidcAuth } from "react-oidc-context";
import {
  generateSigningKey,
  getSigningKeyStatus,
  getStoredPublicKey,
  exportPublicKeyDER,
  removeSigningKey,
} from "./signingKeyApi";
import { createSignerEnrollment, getAuthMethod } from "./api";
import type { AuthMethod } from "./api";
import {
  detectRegistry,
  refreshAndGetIdToken,
  testSign,
} from "./enrollmentUtils";
import type { RegistryType } from "./enrollmentUtils";

export type EnrollStep =
  | "loading"
  | "generating"
  | "register-key"
  | "enrolling"
  | "verifying"
  | "enrolled"
  | "error";

function useGitHubKeyPolling(
  username: string | null,
  sshPublicKey: string | null,
  enabled: boolean,
) {
  const [found, setFound] = useState(false);

  useEffect(() => {
    setFound(false);
  }, [username, sshPublicKey]);

  useEffect(() => {
    if (!enabled || !username || !sshPublicKey || found) return;
    const pollIncrement = 1000; // 1 second
    let pollInterval = pollIncrement; // increase if not found to avoid hitting GitHub rate limits

    const keyData = sshPublicKey.split(" ")[1];

    const poll = async () => {
      try {
        const res = await fetch(
          `/api/ui/github-signing-keys/${encodeURIComponent(username)}`,
        );
        if (!res.ok) {
          throw "unable to fetch GitHub keys, retrying...";
        }
        const keys: string[] = await res.json();
        if (keys.some((k) => k.includes(keyData))) {
          setFound(true);
        }
      } catch {
        // ignore fetch errors
        pollInterval += pollIncrement;
        setTimeout(poll, pollInterval);
      }
    };
    const id = setTimeout(poll, pollInterval);
    return () => {
      clearTimeout(id);
    };
  }, [enabled, username, sshPublicKey, found]);

  return found;
}

export function useSigningKeyEnrollment() {
  const auth = useOidcAuth();
  const [step, setStep] = useState<EnrollStep>("loading");
  const [sshPublicKey, setSshPublicKey] = useState<string | null>(null);
  const [registry, setRegistry] = useState<RegistryType>("oidc");
  const [authMethod, setAuthMethod] = useState<AuthMethod | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [enrollmentName, setEnrollmentName] = useState<string | null>(null);
  const initialized = useRef(false);
  const [ghPollEnabled, setGhPollEnabled] = useState(false);

  const githubUsername = authMethod?.oidcConfig?.registrySubjectMapping
    ? (((auth.user?.profile as Record<string, unknown>)?.github_username as
        | string
        | undefined) ?? null)
    : null;

  const keyFound = useGitHubKeyPolling(
    githubUsername,
    sshPublicKey,
    ghPollEnabled && step === "register-key" && registry === "github",
  );

  const initialize = useCallback(async () => {
    try {
      const method = await getAuthMethod("default");
      setAuthMethod(method);
      const reg = detectRegistry(method);
      setRegistry(reg);

      const status = await getSigningKeyStatus();
      if (status.enrolled && status.sshPublicKey) {
        setSshPublicKey(status.sshPublicKey);
        setStep("register-key");
      } else {
        setStep("generating");
        const key = await generateSigningKey();
        setSshPublicKey(key);
        setStep("register-key");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Initialization failed");
      setStep("error");
    }
  }, []);

  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    initialize();
  }, [initialize]);

  const enrollOidc = useCallback(async () => {
    setStep("enrolling");
    try {
      const pub = await getStoredPublicKey();
      if (!pub) throw new Error("No signing key in IndexedDB");
      const derB64 = await exportPublicKeyDER(pub);

      const authority = (auth.settings as { authority?: string }).authority;
      if (!authority) throw new Error("OIDC authority not available");

      const token = auth.user?.access_token;
      if (!token) throw new Error("No access token — log in first");

      const headers = {
        "Content-Type": "application/json",
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
      };

      const profileResp = await fetch(`${authority}/account`, { headers });
      if (!profileResp.ok) {
        throw new Error(
          `IdP GET profile returned ${profileResp.status}: ${await profileResp.text()}`,
        );
      }
      const profile = await profileResp.json();
      profile.attributes = {
        ...profile.attributes,
        signing_public_key: [derB64],
      };
      const updateResp = await fetch(`${authority}/account`, {
        method: "POST",
        headers,
        body: JSON.stringify(profile),
      });
      if (!updateResp.ok) {
        throw new Error(
          `IdP returned ${updateResp.status}: ${await updateResp.text()}`,
        );
      }

      const clientId = (auth.settings as { client_id?: string }).client_id;
      if (!clientId) throw new Error("OIDC client_id not available");

      const freshIdToken = await refreshAndGetIdToken(authority, clientId);

      const enrollment = await createSignerEnrollment({
        signerEnrollmentId: `browser-${Date.now()}`,
        identityToken: freshIdToken,
      });
      setEnrollmentName(enrollment.name);

      setStep("verifying");
      await testSign(freshIdToken);
      setStep("enrolled");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Enrollment failed");
      setStep("error");
    }
  }, [auth]);

  const enrollGithub = useCallback(async () => {
    setStep("enrolling");
    try {
      const idToken = auth.user?.id_token;
      if (!idToken) throw new Error("No ID token — log in first");

      const enrollment = await createSignerEnrollment({
        signerEnrollmentId: `browser-${Date.now()}`,
        identityToken: idToken,
        registryId: "github.com",
      });
      setEnrollmentName(enrollment.name);

      setStep("verifying");
      const currentIdToken = auth.user?.id_token;
      if (!currentIdToken) throw new Error("No ID token for test sign");
      await testSign(currentIdToken);
      setStep("enrolled");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Enrollment failed");
      setStep("error");
    }
  }, [auth]);

  const handleReenroll = useCallback(async () => {
    await removeSigningKey();
    setSshPublicKey(null);
    setEnrollmentName(null);
    setStep("loading");
    await initialize();
  }, [initialize]);

  const retry = useCallback(async () => {
    setError(null);
    if (sshPublicKey) {
      setStep("register-key");
    } else {
      setStep("loading");
      await initialize();
    }
  }, [sshPublicKey, initialize]);

  const isSetupFlow = window.location.pathname.startsWith("/setup");

  useEffect(() => {
    if (keyFound && step === "register-key" && registry === "github") {
      enrollGithub();
    }
  }, [keyFound, step, registry, enrollGithub]);

  const simulateSuccess = useCallback(() => {
    setEnrollmentName("browser-fake-1234567890");
    setStep("enrolled");
  }, []);

  return {
    step,
    sshPublicKey,
    registry,
    error,
    enrollmentName,
    githubUsername,
    isSetupFlow,
    enrollOidc,
    retry,
    handleReenroll,
    setGhPollEnabled,
    simulateSuccess,
  };
}
