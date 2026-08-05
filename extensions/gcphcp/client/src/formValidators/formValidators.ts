import { createResourceApi } from "@fleetshift/common";

const CLUSTER_ID_PATTERN = /^[a-z][-a-z0-9]*$/;
const CLUSTER_ID_MAX_LENGTH = 15;

const gcpHcpClient = createResourceApi("-");

// DDF async validators must throw the error message string.
// composeValidators catches thrown values and uses them as field errors.
export const clusterIdValidator = async (name = "") => {
  if (name.length === 0) {
    throw "Required";
  }
  if (name.length > CLUSTER_ID_MAX_LENGTH) {
    throw `Must be ${CLUSTER_ID_MAX_LENGTH} characters or less (currently ${name.length}).`;
  }
  if (!CLUSTER_ID_PATTERN.test(name)) {
    throw "Must start with a lowercase letter and contain only lowercase letters, digits, and hyphens.";
  }
  const query = `resourceType == "gcphcp.fleetshift.io/Cluster" &&  resource.name == "clusters/${name}"`;
  const result = await gcpHcpClient.search({ filter: query, pageSize: 1 });

  if (result.resources.length > 0) {
    throw `Cluster with ID "${name}" already exists.`;
  }
};
