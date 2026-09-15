import {
  authMethodServiceGetAuthMethod,
  deploymentServiceCreateDeployment,
  deploymentServiceListDeployments,
  signerEnrollmentServiceCreateSignerEnrollment,
} from "@fleetshift/common";
import { client } from "@fleetshift/common/dynamic/client/generated/client.gen";
import {
  type V1AuthMethod as AuthMethod,
  type V1CreateSignerEnrollmentRequest,
  type V1Deployment,
  type V1DeploymentWritable,
  type V1ListDeploymentsResponse as ListDeploymentsResponse,
  type V1SignerEnrollment as SignerEnrollment,
} from "@fleetshift/common/dynamic/client/generated/types.gen";

client.setConfig({ baseUrl: window.location.origin });

export type MgmtDeployment = V1Deployment;
export type { AuthMethod, ListDeploymentsResponse, SignerEnrollment };

export async function getAuthMethod(name: string): Promise<AuthMethod> {
  const result = await authMethodServiceGetAuthMethod({
    client,
    path: { name },
  });
  if (result.error) throw result.error;
  if (!result.data) throw new Error("Auth method response missing data");
  return result.data;
}

export async function listDeployments(): Promise<ListDeploymentsResponse> {
  const result = await deploymentServiceListDeployments({ client });
  if (result.error) throw result.error;
  if (!result.data) throw new Error("Deployment response missing data");
  return result.data;
}

export interface CreateDeploymentRequest {
  deploymentId: string;
  deployment: V1DeploymentWritable;
  userSignature?: string;
  validUntil?: string;
}

export async function createDeployment(
  req: CreateDeploymentRequest,
): Promise<V1Deployment> {
  const result = await deploymentServiceCreateDeployment({
    client,
    query: {
      deploymentId: req.deploymentId,
      ...(req.userSignature ? { userSignature: req.userSignature } : {}),
      ...(req.validUntil ? { validUntil: req.validUntil } : {}),
    },
    body: req.deployment,
  });
  if (result.error) throw result.error;
  if (!result.data) throw new Error("Deployment response missing data");
  return result.data;
}

export async function createSignerEnrollment(
  req: V1CreateSignerEnrollmentRequest,
): Promise<SignerEnrollment> {
  const result = await signerEnrollmentServiceCreateSignerEnrollment({
    client,
    body: req,
  });
  if (result.error) throw result.error;
  if (!result.data) throw new Error("Signer enrollment response missing data");
  return result.data;
}
