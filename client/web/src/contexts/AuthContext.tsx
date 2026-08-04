import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import type { AuthProviderProps } from "react-oidc-context";
import {
  AuthProvider as OidcAuthProvider,
  useAuth as useOidcAuth,
} from "react-oidc-context";

import {
  installFetchInterceptor,
  setOnUnauthorized,
} from "../auth/fetchInterceptor";
import { fetchOidcConfig } from "../auth/oidcConfig";

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
    oidcRef.current.signoutRedirect();
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

export function AuthProvider({
  children,
  requireAuth = true,
}: {
  children: ReactNode;
  requireAuth?: boolean;
}) {
  const [oidcProps, setOidcProps] = useState<AuthProviderProps | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchOidcConfig()
      .then(setOidcProps)
      .catch((err) => setError(err.message));
  }, []);

  if (error) {
    if (requireAuth) {
      return (
        <div className="ome-oidc-error">
          Failed to load OIDC config: {error}
        </div>
      );
    }
    return <>{children}</>;
  }

  if (!oidcProps) {
    // When auth is not required (e.g. setup mode), render children
    // immediately instead of blocking while OIDC config loads.
    // Auth-required routes keep the blocking behavior so the OIDC
    // provider is ready before any authenticated component renders.
    if (!requireAuth) return <>{children}</>;
    return null;
  }

  if (!oidcProps.authority) {
    return <>{children}</>;
  }

  return (
    <OidcAuthProvider {...oidcProps}>
      <KeycloakAuthInner>{children}</KeycloakAuthInner>
    </OidcAuthProvider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
