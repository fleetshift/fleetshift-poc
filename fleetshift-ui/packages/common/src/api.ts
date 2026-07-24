import type { PluginRegistry } from "./types.js";

export async function makeRequest<T>(
  url: string,
  init?: RequestInit,
): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export function fetchPluginRegistry(apiBase: string): Promise<PluginRegistry> {
  return makeRequest(`${apiBase}/plugin-registry`);
}
