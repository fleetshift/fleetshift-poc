import os
import re
import json
import shutil
import subprocess
from typing import Dict, Any, Optional

MAX_INFRA_ID_LENGTH = 15
INFRA_ID_PATTERN = r"^[a-z][-a-z0-9]*$"

SERVICE_ACCOUNT_MAP = {
    "ctrlplane-op": "controlPlaneEmail",
    "nodepool-mgmt": "nodePoolEmail",
    "cloud-controller": "cloudControllerEmail",
    "gcp-pd-csi": "storageEmail",
    "image-registry": "imageRegistryEmail",
    "cloud-network": "networkEmail",
}


class HypershiftError(Exception):
    pass


def get_hypershift_binary(binary_path: Optional[str] = None) -> str:
    if binary_path:
        return binary_path
    env_path = os.environ.get("HYPERSHIFT_BINARY")
    if env_path:
        return env_path
    found = shutil.which("hypershift")
    if found:
        return found
    raise HypershiftError(
        "hypershift CLI not found. Set HYPERSHIFT_BINARY env var or add to PATH."
    )


def validate_infra_id(infra_id: str) -> None:
    if len(infra_id) > MAX_INFRA_ID_LENGTH:
        raise ValueError(
            f"Infra ID '{infra_id}' exceeds {MAX_INFRA_ID_LENGTH} characters"
        )
    if not re.match(INFRA_ID_PATTERN, infra_id):
        raise ValueError(
            f"Infra ID '{infra_id}' must start with a lowercase letter "
            f"and contain only lowercase letters, digits, and hyphens"
        )


def create_iam_gcp(
    infra_id: str,
    project_id: str,
    oidc_jwks_file: str,
    binary_path: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> Dict[str, Any]:
    binary = get_hypershift_binary(binary_path)
    cmd = [
        binary, "create", "iam", "gcp",
        "--infra-id", infra_id,
        "--project-id", project_id,
        "--oidc-jwks-file", oidc_jwks_file,
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300, env=env
    )
    if result.returncode != 0:
        raise HypershiftError(
            f"hypershift create iam gcp failed (exit {result.returncode}):\n{result.stderr}"
        )
    try:
        iam_config = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise HypershiftError(f"Failed to parse IAM config JSON: {e}\nOutput: {result.stdout}")

    required = ["projectId", "projectNumber", "infraId", "workloadIdentityPool", "serviceAccounts"]
    for field in required:
        if field not in iam_config:
            raise HypershiftError(f"IAM config missing required field: {field}")

    return iam_config


def create_infra_gcp(
    infra_id: str,
    project_id: str,
    region: str,
    binary_path: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> Dict[str, Any]:
    binary = get_hypershift_binary(binary_path)
    cmd = [
        binary, "create", "infra", "gcp",
        "--infra-id", infra_id,
        "--project-id", project_id,
        "--region", region,
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300, env=env
    )
    if result.returncode != 0:
        raise HypershiftError(
            f"hypershift create infra gcp failed (exit {result.returncode}):\n{result.stderr}"
        )
    try:
        infra_config = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise HypershiftError(f"Failed to parse infra config JSON: {e}\nOutput: {result.stdout}")

    required = ["projectId", "infraId", "region", "networkName", "subnetName"]
    for field in required:
        if field not in infra_config:
            raise HypershiftError(f"Infra config missing required field: {field}")

    return infra_config


def iam_config_to_wif_spec(iam_config: Dict[str, Any]) -> Dict[str, Any]:
    wif_pool = iam_config["workloadIdentityPool"]
    service_accounts = iam_config["serviceAccounts"]
    sa_ref = {}
    for sa_key, spec_key in SERVICE_ACCOUNT_MAP.items():
        if sa_key not in service_accounts:
            raise HypershiftError(f"IAM config missing service account: {sa_key}")
        sa_ref[spec_key] = service_accounts[sa_key]

    return {
        "projectNumber": iam_config["projectNumber"],
        "poolID": wif_pool["poolId"],
        "providerID": wif_pool["providerId"],
        "serviceAccountsRef": sa_ref,
    }


def destroy_iam_gcp(
    infra_id: str,
    project_id: str,
    binary_path: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> None:
    binary = get_hypershift_binary(binary_path)
    cmd = [
        binary, "destroy", "iam", "gcp",
        "--infra-id", infra_id,
        "--project-id", project_id,
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300, env=env
    )
    if result.returncode != 0:
        raise HypershiftError(
            f"hypershift destroy iam gcp failed (exit {result.returncode}):\n{result.stderr}"
        )
    print("IAM infrastructure destroyed.")


def destroy_infra_gcp(
    infra_id: str,
    project_id: str,
    region: str,
    binary_path: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> None:
    binary = get_hypershift_binary(binary_path)
    cmd = [
        binary, "destroy", "infra", "gcp",
        "--infra-id", infra_id,
        "--project-id", project_id,
        "--region", region,
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300, env=env
    )
    if result.returncode != 0:
        raise HypershiftError(
            f"hypershift destroy infra gcp failed (exit {result.returncode}):\n{result.stderr}"
        )
    print("Network infrastructure destroyed.")
