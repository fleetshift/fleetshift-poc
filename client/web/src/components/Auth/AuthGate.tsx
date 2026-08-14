import { PropsWithChildren, useCallback, useEffect, useRef } from "react";

import { useAuth } from "../../contexts/AuthContext";
import { AuthErrorState } from "../AuthErrorState";
import { beginLogin, shouldAutoStartLogin } from "./authGateLogin";

const AuthGate = ({ children }: PropsWithChildren) => {
  const { loading, user, authError, login } = useAuth();
  const loginTriggered = useRef(false);

  const startLogin = useCallback(() => {
    beginLogin(login, window.location.pathname);
  }, [login]);

  useEffect(() => {
    if (
      shouldAutoStartLogin({
        loading,
        hasUser: Boolean(user),
        authError,
        alreadyTriggered: loginTriggered.current,
      })
    ) {
      loginTriggered.current = true;
      window.sessionStorage.setItem(
        "post_login_redirect_pathname",
        window.location.pathname,
      );
      login();
    }
  }, [loading, user, authError, startLogin]);

  if (authError) {
    return <AuthErrorState onSignIn={startLogin} />;
  }

  if (loading || !user) return null;
  return <>{children}</>;
};

export default AuthGate;
