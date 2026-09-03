import { useGetState } from "@scalprum/react-core";
import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import {
  AuthProvider as OidcAuthProvider,
  useAuth as useOidcAuth,
} from "react-oidc-context";

import {
  installFetchInterceptor,
  setOnUnauthorized,
} from "../auth/fetchInterceptor";
import { uiConfigStore } from "../utils/uiConfig";

export interface User {
  id: string;
  username: string;
  display_name: string;
  role: string;
  navLayout: Array<{ path: string; label: string }>;
}

interface AuthContextValue {
  user: User | null;
  loading: boolean;
  token: string | undefined;
  email: string | undefined;
  authError: boolean;
  login: () => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

interface OidcProfile {
  preferred_username?: string;
  email?: string;
  realm_access?: { roles?: string[] };
}

function KeycloakAuthInner({ children }: { children: ReactNode }) {
  const oidc = useOidcAuth();
  const oidcRef = useRef(oidc);
  oidcRef.current = oidc;

  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [authError, setAuthError] = useState(false);
  const fetchedForToken = useRef<string | undefined>(undefined);

  const accessToken = oidc.user?.access_token;
  const email = (oidc.user?.profile as OidcProfile | undefined)?.email;

  // Keep the fetch interceptor token in sync
  useEffect(() => {
    installFetchInterceptor(oidcRef);
  }, [accessToken]);

  useEffect(() => {
    if (oidc.isLoading) return;

    if (!oidc.isAuthenticated || !accessToken) {
      setLoading(false);
      return;
    }

    // Prevent re-fetching for the same token
    if (fetchedForToken.current === accessToken) return;
    fetchedForToken.current = accessToken;

    const profile = oidc.user!.profile as OidcProfile;
    const username = profile.preferred_username ?? "unknown";
    const roles = profile.realm_access?.roles ?? [];
    const role = roles.includes("ops") ? "ops" : "dev";

    setUser({
      id: `user-${username}`,
      username,
      display_name: username.charAt(0).toUpperCase() + username.slice(1),
      role,
      navLayout: [],
    });
    setLoading(false);
  }, [oidc.isLoading, oidc.isAuthenticated, oidc.user, accessToken]);

  useEffect(() => {
    setOnUnauthorized(() => setAuthError(true));
  }, []);

  const login = useCallback(() => {
    oidcRef.current.signinRedirect();
  }, []);

  const logout = useCallback(() => {
    setUser(null);
    fetchedForToken.current = undefined;
    void oidcRef.current.removeUser();
  }, []);

  if (oidc.isLoading) {
    return null;
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        loading,
        token: accessToken,
        email,
        authError,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const state = useGetState(uiConfigStore);

  if (!state.selectedIDP?.authority) {
    return <>{children}</>;
  }

  return (
    <OidcAuthProvider {...state.selectedIDP}>
      <KeycloakAuthInner>{children}</KeycloakAuthInner>
    </OidcAuthProvider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  const state = useGetState(uiConfigStore);
  // TODO: Needs better flow and gating, just a dirty mock to get around async rendering issues
  if (!ctx && !state.selectedIDP) {
    return {
      user: null,
      loading: false,
      token: undefined,
      email: undefined,
      authError: false,
      login: () => {},
      logout: () => {},
    };
  }
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
