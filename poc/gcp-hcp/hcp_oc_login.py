#!/usr/bin/env python3
"""Resolve a cluster API endpoint and log into it with oc.

Usage:
    python hcp_oc_login.py <cluster-name>
    python hcp_oc_login.py --id <cluster-id>
    python hcp_oc_login.py <cluster-name> --server https://api.example.com:6443
"""

import argparse
import shutil
import subprocess
import sys
import warnings
from typing import Any

import requests
import urllib3

import hcp_lifecycle as hl

JsonDict = dict[str, Any]


class ClusterLoginError(Exception):
    """Raised when cluster login preparation or execution fails."""


def fetch_json(path: str, token: str, email: str, config: hl.Config) -> JsonDict:
    """Fetch one API resource and return parsed JSON."""
    resp = hl.api_request("GET", path, token, email, config)
    if resp.status_code >= 400:
        raise ClusterLoginError(f"{path} failed (HTTP {resp.status_code}): {resp.text}")
    return resp.json()


def resolve_cluster_endpoint(
    cluster_identifier: str,
    identifier_is_id: bool,
    server: str | None,
    config: hl.Config,
    token: str,
    email: str,
) -> str:
    """Resolve the cluster API server endpoint."""
    if server:
        return server
    cluster_id = (
        cluster_identifier
        if identifier_is_id
        else hl.resolve_cluster_id(cluster_identifier, token, email, config)
    )

    cluster = fetch_json(f"/api/v1/clusters/{cluster_id}", token, email, config)
    cluster_status = fetch_json(
        f"/api/v1/clusters/{cluster_id}/status", token, email, config
    )

    for controller in cluster_status.get("controller_status", []):
        for condition in controller.get("conditions", []):
            message = condition.get("message", "")
            if condition.get("type") == "APIServer" and message.startswith("https://"):
                return message

    api_endpoint = cluster.get("api_endpoint")
    if isinstance(api_endpoint, str) and api_endpoint.startswith("https://"):
        return api_endpoint

    raise ClusterLoginError(
        "Could not resolve cluster API endpoint from CLS backend status. "
        "Pass --server explicitly."
    )


def validate_token(
    server: str,
    token: str,
    insecure_skip_tls_verify: bool,
) -> None:
    """Validate the token against the cluster API server."""
    try:
        with warnings.catch_warnings():
            if insecure_skip_tls_verify:
                warnings.filterwarnings(
                    "ignore", category=urllib3.exceptions.InsecureRequestWarning
                )
            response = requests.get(
                f"{server}/api",
                headers={"Authorization": f"Bearer {token}"},
                verify=not insecure_skip_tls_verify,
                timeout=10,
            )
    except requests.exceptions.SSLError as e:
        raise ClusterLoginError(f"SSL error: {e}")
    except requests.exceptions.ConnectionError:
        raise ClusterLoginError(f"Connection error: Cannot reach {server}")
    except requests.exceptions.Timeout:
        raise ClusterLoginError(f"Connection timeout: {server}")

    if response.status_code == 401:
        raise ClusterLoginError("Authentication failed: invalid or expired token")
    if response.status_code == 403:
        return
    if response.status_code >= 400:
        raise ClusterLoginError(
            f"Connection failed: HTTP {response.status_code} {response.reason}"
        )


def login_with_oc(
    server: str,
    token: str,
    kubeconfig_path: str | None,
    insecure_skip_tls_verify: bool,
) -> str:
    """Run oc login using the given token and endpoint."""
    oc_bin = shutil.which("oc")
    if not oc_bin:
        raise ClusterLoginError("oc CLI not found")

    cmd = [
        oc_bin,
        "login",
        "--token",
        token,
        "--server",
        server,
    ]
    if insecure_skip_tls_verify:
        cmd.append("--insecure-skip-tls-verify")
    if kubeconfig_path:
        cmd.extend(["--kubeconfig", kubeconfig_path])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        error_msg = result.stderr.strip() or result.stdout.strip()
        raise ClusterLoginError(f"oc login failed: {error_msg}")
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Log into a hosted cluster with oc")
    parser.add_argument(
        "cluster",
        help="Cluster name by default, or cluster ID if --id is set",
    )
    parser.add_argument(
        "--id",
        action="store_true",
        help="Treat the positional cluster argument as a cluster ID",
    )
    parser.add_argument(
        "--server",
        help="Cluster API server URL. If set, skip CLS endpoint resolution.",
    )
    parser.add_argument(
        "--kubeconfig",
        help="Optional kubeconfig path. Defaults to the normal oc behavior.",
    )
    parser.add_argument(
        "--no-insecure-skip-tls-verify",
        action="store_false",
        dest="insecure_skip_tls_verify",
        help="Do not skip TLS verification when validating the endpoint or running oc login.",
    )
    parser.set_defaults(insecure_skip_tls_verify=True)
    args = parser.parse_args()

    config = hl.load_config()

    try:
        token, email, _ = hl.authenticate(config)
        endpoint = resolve_cluster_endpoint(
            cluster_identifier=args.cluster,
            identifier_is_id=args.id,
            server=args.server,
            config=config,
            token=token,
            email=email,
        )
        print(f"Resolved cluster API endpoint: {endpoint}")
        print(f"Got broker ID token for: {config['broker_sa_email']}")

        validate_token(endpoint, token, args.insecure_skip_tls_verify)
        print("Cluster API accepted the token.")

        result = login_with_oc(
            endpoint,
            token,
            args.kubeconfig,
            args.insecure_skip_tls_verify,
        )
        if result:
            print(result)
        print("oc login completed.")
    except ClusterLoginError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
