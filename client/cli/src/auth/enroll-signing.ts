import { generateKeyPairSync, randomBytes } from "node:crypto";

import { signerEnrollmentServiceCreateSignerEnrollment } from "@fleetshift/common/dynamic/client/generated/sdk.gen";

import { flagString, parseArgs } from "../argv";
import { clientForArgs, unwrap } from "../commands/context";
import { loadAuthConfig, saveSigningKey } from "../config";
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
  await clientForArgs(args, token.access_token);
  const enrollment = await unwrap(
    signerEnrollmentServiceCreateSignerEnrollment({
      body: {
        signerEnrollmentId: enrollmentID,
        identityToken: token.id_token,
      },
    }),
  );
  await saveSigningKey(directory, privateKeyPEM);
  return `Signer enrolled successfully.\n  Enrollment: ${enrollment.name ?? `signerEnrollments/${enrollmentID}`}`;
}
