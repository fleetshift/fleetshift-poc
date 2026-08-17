export const APP_BASENAME = "/app";

export const AUTH_CALLBACK_PATH = `${APP_BASENAME}/auth/callback`;
export const SILENT_RENEW_PATH = `${APP_BASENAME}/silent-renew.html`;

export function isAuthCallbackPath(pathname: string): boolean {
  return pathname === AUTH_CALLBACK_PATH || pathname === "/auth/callback";
}

export function stripAppBasename(pathname: string): string {
  if (pathname === APP_BASENAME || pathname === `${APP_BASENAME}/`) {
    return "/";
  }
  if (pathname.startsWith(`${APP_BASENAME}/`)) {
    return pathname.slice(APP_BASENAME.length);
  }
  return pathname;
}

export function toBrowserPath(pathname: string): string {
  if (
    pathname === APP_BASENAME ||
    pathname.startsWith(`${APP_BASENAME}/`)
  ) {
    return pathname === APP_BASENAME ? `${APP_BASENAME}/` : pathname;
  }
  if (!pathname.startsWith("/")) {
    pathname = `/${pathname}`;
  }
  if (pathname === "/") {
    return `${APP_BASENAME}/`;
  }
  return `${APP_BASENAME}${pathname}`;
}
