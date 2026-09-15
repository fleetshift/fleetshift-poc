import {
  authMethodServiceGetAuthMethod,
  signerEnrollmentServiceCreateSignerEnrollment,
} from "@fleetshift/common";
import { client } from "@fleetshift/common/dynamic/client/generated/client.gen";
import type {
  V1AuthMethod as AuthMethod,
  V1CreateSignerEnrollmentRequest,
  V1SignerEnrollment as SignerEnrollment,
} from "@fleetshift/common/dynamic/client/generated/types.gen";

client.setConfig({ baseUrl: window.location.origin });

export type { AuthMethod, SignerEnrollment };

export async function getAuthMethod(name: string): Promise<AuthMethod> {
  const result = await authMethodServiceGetAuthMethod({
    client,
    path: { name },
  });
  if (result.error) throw result.error;
  if (!result.data) throw new Error("Auth method response missing data");
  return result.data;
}

export function createSignerEnrollment(
  req: V1CreateSignerEnrollmentRequest,
): Promise<SignerEnrollment> {
  return signerEnrollmentServiceCreateSignerEnrollment({
    client,
    body: req,
  }).then((result) => {
    if (result.error) throw result.error;
    if (!result.data)
      throw new Error("Signer enrollment response missing data");
    return result.data;
  });
}
