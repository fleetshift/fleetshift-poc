#!/usr/bin/env python3
"""HCP Cluster Lifecycle — create and delete GCP Hosted Control Plane clusters.

Usage:
    python hcp_lifecycle.py create <cluster-name>
    python hcp_lifecycle.py delete <cluster-name>
"""

import argparse
import base64
import json
import sys
import tempfile
import time
import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, cast
import requests
import yaml
from authlib.integrations.requests_client import OAuth2Session
from authlib.common.security import generate_token
from lib.crypto import generate_cluster_keypair
from lib.hypershift import (create_iam_gcp, create_infra_gcp, destroy_iam_gcp, destroy_infra_gcp, iam_config_to_wif_spec, validate_infra_id, HypershiftError,)

Config = dict[str, str]
IamConfig = dict[str, str | dict[str, str]]
InfraConfig = dict[str, str]
ClusterSpec = dict[str, Any]
OIDC_CALLBACK_URL = "http://localhost:8888/callback"
OIDC_CALLBACK_PORT = 8888

def load_config(config_path: str = "config.yaml") -> Config:
    path = Path(config_path)
    if not path.exists():
        print(f"Error: {config_path} not found. Copy config.yaml.example to config.yaml and fill in values.")
        sys.exit(1)
    with open(path) as f:
        config: Config = yaml.safe_load(f)
    required = [
        "oidc_issuer_url", "oidc_client_id",
        "gcp_project", "workforce_pool", "workforce_provider",
        "broker_sa_email", "gateway_url", "gateway_audience",
        "project", "region",
    ]
    missing = [k for k in required if not config.get(k)]
    if missing:
        print(f"Error: missing required config keys: {', '.join(missing)}")
        sys.exit(1)
    return config


def discover_oidc_endpoints(issuer_url: str) -> tuple[str, str]:
    """Fetch OIDC discovery document and return (authorize_url, token_url)."""
    discovery_url = f"{issuer_url.rstrip('/')}/.well-known/openid-configuration"
    resp = requests.get(discovery_url, timeout=10)
    if resp.status_code >= 400:
        print(f"Error: OIDC discovery failed (HTTP {resp.status_code}): {discovery_url}")
        sys.exit(1)
    doc: dict[str, str] = resp.json()
    authorize = doc.get("authorization_endpoint", "")
    token = doc.get("token_endpoint", "")
    if not authorize or not token:
        print("Error: OIDC discovery response missing authorization_endpoint or token_endpoint")
        sys.exit(1)
    return authorize, token


def open_browser_for_login(uri: str) -> None:
    """Open the login URL in a browser and always print a manual fallback."""
    print("Opening browser for OIDC login...")
    print("If the browser does not open automatically, open this URL manually:")
    print(uri)
    try:
        opened = webbrowser.open(uri)
    except Exception as e:
        print(f"Warning: automatic browser launch failed: {e}")
        return

    if not opened:
        print("Warning: automatic browser launch did not report success.")


def oidc_login(config: Config) -> tuple[str, str]:
    """OIDC PKCE login. Opens browser, returns (id_token_jwt, user_email)."""
    authorize_url, token_url = discover_oidc_endpoints(config["oidc_issuer_url"])

    code_verifier = generate_token(48)

    session = OAuth2Session(
        client_id=config["oidc_client_id"],
        code_challenge_method="S256",
    )

    uri, state = cast(
        tuple[str, str],
        session.create_authorization_url(
            authorize_url,
            code_verifier=code_verifier,
            redirect_uri=OIDC_CALLBACK_URL,
            scope="openid email",
        ),
    )

    open_browser_for_login(uri)

    print(f"Waiting for login callback on {OIDC_CALLBACK_URL} ...")
    callback_url = _wait_for_callback()

    token_response = cast(
        dict[str, str],
        session.fetch_token(
            token_url,
            authorization_response=callback_url,
            code_verifier=code_verifier,
            redirect_uri=OIDC_CALLBACK_URL,
        ),
    )

    id_token_val: str | None = token_response.get("id_token")
    if not id_token_val:
        print("Error: no id_token in OIDC token response")
        sys.exit(1)
    id_token: str = str(id_token_val)

    payload: dict[str, str] = json.loads(
        base64.urlsafe_b64decode(id_token.split(".")[1] + "==")
    )
    email: str | None = payload.get("email")
    if not email:
        print("Error: no email claim in OIDC token. Ensure the IdP includes an email claim.")
        sys.exit(1)

    print(f"Logged in as: {email}")
    return id_token, email


def _wait_for_callback(port: int = OIDC_CALLBACK_PORT, timeout: int = 120) -> str:
    """Run a one-shot HTTP server to catch the OAuth callback."""
    callback_url = None

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal callback_url
            callback_url = f"http://localhost:{port}{self.path}"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Login successful. You can close this tab.</h2></body></html>")

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = HTTPServer(("localhost", port), CallbackHandler)
    server.timeout = timeout
    server.handle_request()
    server.server_close()

    if not callback_url:
        print(f"Error: no callback received within {timeout} seconds")
        sys.exit(1)

    return callback_url


def sts_exchange(oidc_jwt: str, config: Config) -> str:
    """Exchange an OIDC JWT for a Google Workforce access token via STS."""
    audience = (
        f"//iam.googleapis.com/locations/global/workforcePools/"
        f"{config['workforce_pool']}/providers/{config['workforce_provider']}"
    )
    resp = requests.post(
        "https://sts.googleapis.com/v1/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "audience": audience,
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": "https://www.googleapis.com/auth/cloud-platform",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "subject_token": oidc_jwt,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if resp.status_code >= 400:
        print(f"Error: STS exchange failed (HTTP {resp.status_code})")
        print(resp.text)
        sys.exit(1)

    sts_response: dict[str, str] = resp.json()
    token = sts_response.get("access_token")
    if not token:
        print("Error: no access_token in STS response")
        sys.exit(1)

    print("STS exchange succeeded.")
    return token


def generate_broker_id_token(workforce_token: str, config: Config) -> str:
    """Mint a Google-signed ID token for the broker service account."""
    url = (
        f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
        f"{config['broker_sa_email']}:generateIdToken"
    )
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {workforce_token}",
            "Content-Type": "application/json",
            "x-goog-user-project": config["gcp_project"],
        },
        json={
            "audience": config["gateway_audience"],
            "includeEmail": True,
        },
    )
    if resp.status_code >= 400:
        print(f"Error: generateIdToken failed (HTTP {resp.status_code})")
        print(resp.text)
        sys.exit(1)

    id_token_response: dict[str, str] = resp.json()
    token = id_token_response.get("token")
    if not token:
        print("Error: no token in generateIdToken response")
        sys.exit(1)

    print("Broker ID token generated.")
    return token


def authenticate(config: Config) -> tuple[str, str]:
    """Full auth chain. Returns (broker_id_token, user_email)."""
    print("\n=== Authentication ===")
    oidc_jwt, email = oidc_login(config)
    workforce_token = sts_exchange(oidc_jwt, config)
    broker_token = generate_broker_id_token(workforce_token, config)
    return broker_token, email


def api_request(method: str, path: str, token: str, email: str, config: Config, json_data: ClusterSpec | None = None) -> requests.Response:
    """Make an authenticated request to the CLS Backend API."""
    url = f"{config['gateway_url'].rstrip('/')}{path}"
    resp = requests.request(
        method,
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-User-Email": email,
            "Content-Type": "application/json",
        },
        json=json_data,
    )
    return resp


def build_cluster_spec(cluster_name: str, config: Config, iam_config: IamConfig, infra_config: InfraConfig, signing_key_base64: str) -> ClusterSpec:
    """Build the cluster creation request body."""
    wif_spec = iam_config_to_wif_spec(iam_config)
    return {
        "name": cluster_name,
        "target_project_id": config["project"],
        "spec": {
            "infraID": infra_config["infraId"],
            "issuerURL": f"https://hypershift-{infra_config['infraId']}-oidc",
            "serviceAccountSigningKey": signing_key_base64,
            "platform": {
                "type": "GCP",
                "gcp": {
                    "projectID": config["project"],
                    "region": config["region"],
                    "network": infra_config["networkName"],
                    "subnet": infra_config["subnetName"],
                    "endpointAccess": config.get("endpoint_access", "PublicAndPrivate"),
                    "workloadIdentity": wif_spec,
                },
            },
        },
    }


def build_nodepool_spec(cluster_name: str, cluster_id: str, config: Config) -> ClusterSpec:
    """Build the nodepool creation request body."""
    return {
        "name": f"{cluster_name}-nodepool-1",
        "cluster_id": cluster_id,
        "spec": {
            "replicas": int(config.get("replicas", "2")),
            "platform": {
                "type": "GCP",
                "gcp": {
                    "instanceType": "n1-standard-4",
                    "rootVolume": {
                        "size": 128,
                        "type": "pd-standard",
                    },
                },
            },
            "management": {
                "autoRepair": True,
                "upgradeType": "Replace",
            },
        },
    }


def poll_cluster_ready(cluster_id: str, token: str, email: str, config: Config) -> None:
    """Poll until cluster reaches Ready or Failed. Timeout after 20 minutes."""
    max_polls = 80
    interval = 15
    print(f"\nPolling cluster {cluster_id} every {interval}s (timeout: {max_polls * interval // 60} min)...")

    for i in range(max_polls):
        resp = api_request("GET", f"/api/v1/clusters/{cluster_id}", token, email, config)
        if resp.status_code == 404:
            print("Error: cluster disappeared (404)")
            sys.exit(1)
        if resp.status_code >= 400:
            print(f"Warning: poll returned HTTP {resp.status_code}, retrying...")
            time.sleep(interval)
            continue

        data: ClusterSpec = resp.json()
        phase: str = data.get("status", {}).get("phase", "Unknown")
        message: str = data.get("status", {}).get("message", "")

        status_line = f"  [{i+1}/{max_polls}] Phase: {phase}"
        if message:
            status_line += f" — {message}"
        print(status_line)

        if phase == "Ready":
            print(f"\nCluster {cluster_id} is Ready!")
            return
        if phase == "Failed":
            reason = data.get("status", {}).get("reason", "unknown")
            print(f"\nError: cluster creation failed. Reason: {reason}")
            if message:
                print(f"  Message: {message}")
            sys.exit(1)

        time.sleep(interval)

    print("\nError: timed out waiting for cluster to become Ready")
    sys.exit(1)


def cmd_create(cluster_name: str, config: Config) -> None:
    """Create a cluster end-to-end: infra → cluster → nodepool → poll."""
    token, email = authenticate(config)

    infra_id = cluster_name
    try:
        validate_infra_id(infra_id)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print("\n=== Generating RSA Keypair ===")
    keypair = generate_cluster_keypair()
    print(f"Generated keypair (kid: {keypair.kid})")

    jwks_tmp = None
    try:
        jwks_fd, jwks_tmp = tempfile.mkstemp(suffix=".json", prefix="jwks-")
        os.write(jwks_fd, keypair.jwks_json.encode("utf-8"))
        os.close(jwks_fd)

        print("\n=== Creating IAM Infrastructure ===")
        iam_config = create_iam_gcp(infra_id, config["project"], jwks_tmp)

        print("\n=== Creating Network Infrastructure ===")
        infra_config = create_infra_gcp(infra_id, config["project"], config["region"])
    finally:
        if jwks_tmp and os.path.exists(jwks_tmp):
            os.unlink(jwks_tmp)

    print("\n=== Creating Cluster ===")
    cluster_data = build_cluster_spec(
        cluster_name, config, iam_config, infra_config, keypair.private_key_pem_base64,
    )
    resp = api_request("POST", "/api/v1/clusters", token, email, config, json_data=cluster_data)
    if resp.status_code >= 400:
        print(f"Error: cluster create failed (HTTP {resp.status_code})")
        print(resp.text)
        sys.exit(1)

    cluster: dict[str, str] = resp.json()
    cluster_id: str = cluster["id"]
    print(f"Cluster created: {cluster_name} (ID: {cluster_id})")

    print("\n=== Creating NodePool ===")
    nodepool_data = build_nodepool_spec(cluster_name, cluster_id, config)
    resp = api_request("POST", "/api/v1/nodepools", token, email, config, json_data=nodepool_data)
    if resp.status_code >= 400:
        print(f"Error: nodepool create failed (HTTP {resp.status_code})")
        print(resp.text)
        sys.exit(1)

    nodepool: dict[str, str] = resp.json()
    print(f"NodePool created: {nodepool.get('name', 'unknown')} (ID: {nodepool.get('id', 'unknown')})")

    poll_cluster_ready(cluster_id, token, email, config)


def resolve_cluster_id(cluster_name: str, token: str, email: str, config: Config) -> str:
    """Resolve a cluster name to its ID by listing clusters."""
    resp = api_request("GET", "/api/v1/clusters", token, email, config)
    if resp.status_code >= 400:
        print(f"Error: failed to list clusters (HTTP {resp.status_code})")
        print(resp.text)
        sys.exit(1)

    clusters: list[dict[str, str]] = resp.json().get("clusters", [])
    for c in clusters:
        if c.get("name") == cluster_name:
            return c["id"]

    print(f"Error: cluster '{cluster_name}' not found")
    sys.exit(1)


def poll_cluster_deleted(cluster_id: str, token: str, email: str, config: Config) -> None:
    """Poll until cluster returns 404 (deleted). Timeout after 20 minutes."""
    max_polls = 80
    interval = 15
    print(f"Polling cluster deletion every {interval}s (timeout: {max_polls * interval // 60} min)...")

    for i in range(max_polls):
        resp = api_request("GET", f"/api/v1/clusters/{cluster_id}", token, email, config)
        if resp.status_code == 404:
            print(f"Cluster {cluster_id} deleted.")
            return

        phase = "Unknown"
        if resp.status_code < 400:
            phase = resp.json().get("status", {}).get("phase", "Unknown")
        print(f"  [{i+1}/{max_polls}] Status: {resp.status_code}, Phase: {phase}")
        time.sleep(interval)

    print("Error: timed out waiting for cluster deletion")
    sys.exit(1)


def destroy_infra_with_retry(
    infra_id: str,
    project_id: str,
    region: str,
    max_retries: int = 40,
    interval: int = 30,
) -> None:
    """Retry hypershift destroy infra until success (nodepools must be fully deleted first)."""
    print(f"Destroying network infrastructure (retrying every {interval}s, timeout: {max_retries * interval // 60} min)...")
    for i in range(max_retries):
        try:
            destroy_infra_gcp(infra_id, project_id, region)
            return
        except HypershiftError as e:
            print(f"  [{i+1}/{max_retries}] Infra destroy not ready yet, retrying... ({e})")
            time.sleep(interval)

    print("Error: timed out waiting for infrastructure destroy")
    sys.exit(1)


def cmd_delete(cluster_name: str, config: Config) -> None:
    """Delete a cluster end-to-end: API delete → poll → hypershift destroy."""
    token, email = authenticate(config)

    print("\n=== Resolving Cluster ===")
    cluster_id = resolve_cluster_id(cluster_name, token, email, config)
    print(f"Found cluster: {cluster_name} (ID: {cluster_id})")

    print("\n=== Deleting Cluster ===")
    resp = api_request("DELETE", f"/api/v1/clusters/{cluster_id}?force=true", token, email, config)
    if resp.status_code >= 400 and resp.status_code != 404:
        print(f"Error: cluster delete failed (HTTP {resp.status_code})")
        print(resp.text)
        sys.exit(1)

    if resp.status_code == 404:
        print("Cluster already deleted from API.")
    else:
        print(f"Cluster deletion initiated (HTTP {resp.status_code})")
        poll_cluster_deleted(cluster_id, token, email, config)

    print("\n=== Destroying IAM Infrastructure ===")
    try:
        destroy_iam_gcp(cluster_name, config["project"])
    except HypershiftError as e:
        print(f"Warning: IAM destroy failed: {e}")

    print("\n=== Destroying Network Infrastructure ===")
    destroy_infra_with_retry(cluster_name, config["project"], config["region"])

    print(f"\nCluster {cluster_name} fully deleted.")


def main() -> None:
    parser = argparse.ArgumentParser(description="HCP Cluster Lifecycle")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create an HCP cluster")
    create_parser.add_argument("name", help="Cluster name (also used as infra ID, max 15 chars)")

    delete_parser = subparsers.add_parser("delete", help="Delete an HCP cluster")
    delete_parser.add_argument("name", help="Cluster name to delete")

    args = parser.parse_args()
    config = load_config()

    if args.command == "create":
        cmd_create(args.name, config)
    elif args.command == "delete":
        cmd_delete(args.name, config)


if __name__ == "__main__":
    main()
