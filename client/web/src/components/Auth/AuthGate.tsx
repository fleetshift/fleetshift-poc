import { PropsWithChildren, useEffect, useRef } from "react";

import { useAuth } from "../../contexts/AuthContext";
import { AuthErrorState } from "../AuthErrorState";

const AuthGate = ({ children }: PropsWithChildren) => {
  const { loading, user, authError, login } = useAuth();
  const loginTriggered = useRef(false);

  useEffect(() => {
    if (!loading && !user && !authError && !loginTriggered.current) {
      loginTriggered.current = true;
      login();
    }
  }, [loading, user, authError, login]);

  if (authError) {
    return <AuthErrorState onSignIn={login} />;
  }

  if (loading || !user) return null;
  return <>{children}</>;
};

export default AuthGate;
