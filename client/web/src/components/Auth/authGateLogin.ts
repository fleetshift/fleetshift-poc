export function persistPostLoginRedirect(pathname: string): void {
  window.sessionStorage.setItem("post_login_redirect_pathname", pathname);
}

export function beginLogin(login: () => void, pathname: string): void {
  persistPostLoginRedirect(pathname);
  login();
}

export function shouldAutoStartLogin(input: {
  loading: boolean;
  hasUser: boolean;
  authError: boolean;
  alreadyTriggered: boolean;
}): boolean {
  return (
    !input.loading &&
    !input.hasUser &&
    !input.authError &&
    !input.alreadyTriggered
  );
}
