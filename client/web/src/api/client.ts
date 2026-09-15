import { client } from "@fleetshift/common/dynamic/client/generated/client.gen";
const apiClient = client;

client.setConfig({
  baseUrl: window.location.origin,
});

type ApiPromise<T> = Promise<{ data?: T; error?: unknown }>;

export async function unwrap<T>(promise: ApiPromise<T>): Promise<T> {
  const result = await promise;
  if (result.error) {
    throw result.error;
  }
  if (result.data === undefined) {
    throw new Error("No data returned from API");
  }
  return result.data;
}

export default apiClient;
