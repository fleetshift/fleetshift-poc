import { PropsWithChildren, useCallback, useEffect, useRef } from "react";

import { useAuth } from "../../contexts/AuthContext";
import { AuthErrorState } from "../AuthErrorState";

const AuthGate = ({ children }: PropsWithChildren) => {
  const { loading, user, authError, login } = useAuth();
  const loginTriggered = useRef(false);

  const startLogin = useCallback(() => {
    if (!loading && !user && !authError && !loginTriggered.current) {
      loginTriggered.current = true;
      window.sessionStorage.setItem(
        "post_login_redirect_pathname",
        window.location.pathname,
      );
      login();
    }
  }, [loading, user, authError, login]);

  useEffect(() => {
    startLogin();
  }, [startLogin]);

  if (authError) {
    return <AuthErrorState onSignIn={startLogin} />;
  }

  if (loading || !user) return null;
  return <>{children}</>;
};

export default AuthGate;
