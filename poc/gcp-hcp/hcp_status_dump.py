#!/usr/bin/env python3
"""Dump full cluster and nodepool status as pretty JSON.

Usage:
    python hcp_status_dump.py <cluster-name>
    python hcp_status_dump.py --id <cluster-id>
"""

import argparse
import json
import sys
from typing import Any, cast

import hcp_lifecycle as hl

JsonDict = dict[str, Any]


def fetch_json(path: str, token: str, email: str, config: hl.Config) -> JsonDict:
    """Fetch one API resource and return the parsed JSON body."""
    resp = hl.api_request("GET", path, token, email, config)
    if resp.status_code >= 400:
        raise RuntimeError(f"{path} failed (HTTP {resp.status_code}): {resp.text}")
    return resp.json()


def list_nodepools(cluster_id: str, token: str, email: str, config: hl.Config) -> list[JsonDict]:
    """List nodepools for a cluster."""
    data = fetch_json(f"/api/v1/nodepools?clusterId={cluster_id}", token, email, config)
    raw_nodepools = data.get("nodepools")
    if raw_nodepools is None:
        return []
    if not isinstance(raw_nodepools, list):
        raise RuntimeError("Nodepool list response missing nodepools array")

    raw_nodepool_items = cast(list[object], raw_nodepools)
    nodepools: list[JsonDict] = []
    for item in raw_nodepool_items:
        if not isinstance(item, dict):
            raise RuntimeError("Nodepool list response contains a non-object entry")
        nodepools.append(cast(JsonDict, item))
    return nodepools


def collect_status_bundle(
    cluster_identifier: str,
    identifier_is_id: bool,
    token: str,
    email: str,
    config: hl.Config,
) -> JsonDict:
    """Collect cluster details, cluster status, and nodepool statuses."""
    cluster_id = (
        cluster_identifier
        if identifier_is_id
        else hl.resolve_cluster_id(cluster_identifier, token, email, config)
    )

    cluster = fetch_json(f"/api/v1/clusters/{cluster_id}", token, email, config)
    cluster_status = fetch_json(f"/api/v1/clusters/{cluster_id}/status", token, email, config)

    nodepool_entries: list[JsonDict] = []
    for nodepool in list_nodepools(cluster_id, token, email, config):
        nodepool_id = nodepool.get("id")
        if not nodepool_id:
            nodepool_entries.append(
                {
                    "nodepool": nodepool,
                    "status": {"error": "nodepool entry missing id"},
                }
            )
            continue

        nodepool_status = fetch_json(
            f"/api/v1/nodepools/{nodepool_id}/status", token, email, config
        )
        nodepool_entries.append({"nodepool": nodepool, "status": nodepool_status})

    return {
        "cluster_id": cluster_id,
        "cluster_name": cluster.get("name"),
        "summary": {
            "cluster_phase": cluster_status.get("status", {}).get("phase"),
            "cluster_message": cluster_status.get("status", {}).get("message"),
            "nodepool_count": len(nodepool_entries),
        },
        "cluster": cluster,
        "cluster_status": cluster_status,
        "nodepools": nodepool_entries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump HCP cluster status as pretty JSON")
    parser.add_argument(
        "cluster",
        help="Cluster name by default, or cluster ID if --id is set",
    )
    parser.add_argument(
        "--id",
        action="store_true",
        help="Treat the positional cluster argument as a cluster ID",
    )
    args = parser.parse_args()

    config = hl.load_config()
    token, email = hl.authenticate(config)

    try:
        bundle = collect_status_bundle(args.cluster, args.id, token, email, config)
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(json.dumps(bundle, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
