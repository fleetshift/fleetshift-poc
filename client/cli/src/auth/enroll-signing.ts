import { generateKeyPairSync, randomBytes } from "node:crypto";

import { flagString, parseArgs } from "../argv";
import { serverForArgs } from "../commands/context";
import { loadAuthConfig, saveSigningKey } from "../config";
import { oidcRequest } from "./helpers";
import { runOIDCFlow } from "./login";

export async function runAuthEnrollSigning(
  args: ReturnType<typeof parseArgs>,
): Promise<string> {
  const directory = flagString(args, "config-dir") || undefined;
  const config = await loadAuthConfig(directory);
  if (!config.key_enrollment_client_id) {
    throw new Error(
      "no key enrollment client ID configured (set --key-enrollment-client-id during auth setup)",
    );
  }
  const { privateKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
  const privateKeyPEM = privateKey
    .export({ type: "sec1", format: "pem" })
    .toString();
  const token = await runOIDCFlow(args, config.key_enrollment_client_id, [
    "openid",
    "profile",
    "email",
  ]);
  if (!token.id_token) throw new Error("no id_token in enrollment response");
  const enrollmentID = randomBytes(16).toString("hex");
  const enrollment = await oidcRequest<{ name?: string }>(
    `${serverForArgs(args).replace(/\/$/, "")}/v1/signerEnrollments`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token.access_token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        signerEnrollmentId: enrollmentID,
        identityToken: token.id_token,
      }),
    },
    config.oidc_ca_file,
  );
  await saveSigningKey(directory, privateKeyPEM);
  return `Signer enrolled successfully.\n  Enrollment: ${enrollment.name ?? `signerEnrollments/${enrollmentID}`}`;
}
