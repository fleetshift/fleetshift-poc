import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { useAuth as useOidcAuth } from "react-oidc-context";
import {
  generateSigningKey,
  getSigningKeyStatus,
  getStoredPublicKey,
  exportPublicKeyDER,
  removeSigningKey,
} from "./signingKeyApi";
import { createSignerEnrollment, getAuthMethod } from "./api";
import {
  detectRegistry,
  refreshAndGetIdToken,
  testSign,
} from "./enrollmentUtils";
import {
  Action,
  EnrollStep,
  enrollmentReducer,
  initialState,
} from "./enrollmentReducer";
export { EnrollStep } from "./enrollmentReducer";

const POLL_LIMIT = 6;

function useGitHubKeyPolling(
  username: string | null,
  sshPublicKey: string | null,
  enabled: boolean,
) {
  const [found, setFound] = useState(false);
  const [error, setError] = useState<string | undefined>();

  const clearPollingError = useCallback(() => setError(undefined), [setError]);

  useEffect(() => {
    setFound(false);
  }, [username, sshPublicKey]);

  useEffect(() => {
    if (!enabled || !username || !sshPublicKey || found) return;
    const pollIncrement = 1000;
    let pollInterval = pollIncrement;
    let pollingAttempts = 0;

    const keyData = sshPublicKey.split(" ")[1];

    let timer: ReturnType<typeof setTimeout>;
    let cancelled = false;
    const poll = async () => {
      if (pollingAttempts >= POLL_LIMIT) {
        console.error("GitHub key polling exceeded maximum attempts");
        setFound(false);
        clearTimeout(timer);
        setError(
          "Unable to verify GitHub signing key. Please ensure the signing key is added to GitHub and try again.",
        );
        return;
      }
      pollInterval += pollIncrement;
      pollingAttempts += 1;
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
          return;
        }
      } catch {
        pollInterval += pollIncrement;
        if (pollingAttempts >= POLL_LIMIT / 2) {
          console.error(
            `GitHub key API is not responding. Attempt ${pollingAttempts}.`,
          );
          setError(
            "Unable to verify GitHub signing key due to network issues. Please check your connection and try again.",
          );
          clearTimeout(timer);
          return;
        }
      }
      if (!cancelled) {
        timer = setTimeout(poll, pollInterval);
      }
    };
    timer = setTimeout(poll, pollInterval);
    return () => {
      cancelled = true;
      clearTimeout(timer);
      pollingAttempts = 0;
    };
  }, [enabled, username, sshPublicKey, found]);

  return { found, error, clearPollingError };
}

// --- Main hook ---

export function useSigningKeyEnrollment() {
  const auth = useOidcAuth();
  const [state, dispatch] = useReducer(enrollmentReducer, initialState);
  const initialized = useRef(false);

  const githubUsername = state.authMethod?.oidcConfig?.registrySubjectMapping
    ? (((auth.user?.profile as Record<string, unknown>)?.github_username as
        | string
        | undefined) ?? null)
    : null;

  const {
    found: keyFound,
    error: ghKeyError,
    clearPollingError,
  } = useGitHubKeyPolling(
    githubUsername,
    state.sshPublicKey,
    state.ghPollEnabled &&
      state.step === EnrollStep.RegisterKey &&
      state.registry === "github",
  );

  useEffect(() => {
    if (keyFound || state.ghPollEnabled) {
      clearPollingError();
    }
  }, [keyFound, state.ghPollEnabled]);

  useEffect(() => {
    if (ghKeyError) {
      dispatch({ type: Action.SetGhPoll, enabled: false });
    }
  }, [keyFound, ghKeyError]);

  const initialize = useCallback(async () => {
    try {
      clearPollingError();
      const method = await getAuthMethod("default");
      const reg = detectRegistry(method);
      dispatch({ type: Action.InitAuth, authMethod: method, registry: reg });

      const status = await getSigningKeyStatus();
      if (status.enrolled && status.sshPublicKey) {
        dispatch({
          type: Action.ExistingKey,
          sshPublicKey: status.sshPublicKey,
        });
      } else {
        dispatch({ type: Action.Generating });
        const key = await generateSigningKey();
        dispatch({ type: Action.KeyGenerated, sshPublicKey: key });
      }
    } catch (err) {
      dispatch({
        type: Action.Error,
        error: err instanceof Error ? err.message : "Initialization failed",
      });
    }
  }, [clearPollingError]);

  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    initialize();
  }, [initialize]);

  const enrollOidc = useCallback(async () => {
    dispatch({ type: Action.EnrollStart });
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
      dispatch({
        type: Action.EnrollSuccess,
        enrollmentName: enrollment.name,
      });

      await testSign(freshIdToken);
      dispatch({ type: Action.VerifySuccess });
    } catch (err) {
      dispatch({
        type: Action.Error,
        error: err instanceof Error ? err.message : "Enrollment failed",
      });
    }
  }, [auth]);

  const enrollGithub = useCallback(async () => {
    dispatch({ type: Action.EnrollStart });
    try {
      const idToken = auth.user?.id_token;
      if (!idToken) throw new Error("No ID token — log in first");

      const enrollment = await createSignerEnrollment({
        signerEnrollmentId: `browser-${Date.now()}`,
        identityToken: idToken,
        registryId: "github.com",
      });
      dispatch({
        type: Action.EnrollSuccess,
        enrollmentName: enrollment.name,
      });

      const currentIdToken = auth.user?.id_token;
      if (!currentIdToken) throw new Error("No ID token for test sign");
      await testSign(currentIdToken);
      dispatch({ type: Action.VerifySuccess });
    } catch (err) {
      dispatch({
        type: Action.Error,
        error: err instanceof Error ? err.message : "Enrollment failed",
      });
    }
  }, [auth]);

  const handleReenroll = useCallback(async () => {
    await removeSigningKey();
    dispatch({ type: Action.ReEnroll });
    await initialize();
  }, [initialize]);

  const retry = useCallback(async () => {
    dispatch({ type: Action.Retry });
    if (!state.sshPublicKey) {
      await initialize();
    }
  }, [state.sshPublicKey, initialize]);

  useEffect(() => {
    if (
      keyFound &&
      state.step === "register-key" &&
      state.registry === "github"
    ) {
      enrollGithub();
    }
  }, [keyFound, state.step, state.registry, enrollGithub]);

  const setGhPollEnabled = useCallback(
    (enabled: boolean) => dispatch({ type: Action.SetGhPoll, enabled }),
    [],
  );

  return {
    step: state.step,
    sshPublicKey: state.sshPublicKey,
    registry: state.registry,
    error: state.error,
    enrollmentName: state.enrollmentName,
    githubUsername,
    ghPollEnabled: state.ghPollEnabled,
    ghKeyError,
    enrollOidc,
    retry,
    handleReenroll,
    setGhPollEnabled,
  };
}
