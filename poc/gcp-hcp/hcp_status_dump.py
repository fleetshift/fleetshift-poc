#!/usr/bin/env python3
"""Dump full cluster and nodepool status as pretty JSON.

Usage:
    python hcp_status_dump.py                   # dump all clusters
    python hcp_status_dump.py <cluster-name>    # dump one cluster by name
    python hcp_status_dump.py --id <cluster-id> # dump one cluster by ID
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


def list_clusters(token: str, email: str, config: hl.Config) -> list[JsonDict]:
    """List all clusters."""
    data = fetch_json("/api/v1/clusters", token, email, config)
    raw_clusters = data.get("clusters")
    if raw_clusters is None:
        return []
    if not isinstance(raw_clusters, list):
        raise RuntimeError("Cluster list response missing clusters array")

    raw_cluster_items = cast(list[object], raw_clusters)
    clusters: list[JsonDict] = []
    for item in raw_cluster_items:
        if not isinstance(item, dict):
            raise RuntimeError("Cluster list response contains a non-object entry")
        clusters.append(cast(JsonDict, item))
    return clusters


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
    cluster_id: str,
    token: str,
    email: str,
    config: hl.Config,
) -> JsonDict:
    """Collect cluster details, cluster status, and nodepool statuses."""
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


def resolve_cluster_id(name: str, token: str, email: str, config: hl.Config) -> str:
    """Resolve a cluster name to its ID."""
    for c in list_clusters(token, email, config):
        if c.get("name") == name:
            cid = c.get("id")
            if isinstance(cid, str):
                return cid
            raise RuntimeError(f"Cluster '{name}' has no id field")
    raise RuntimeError(f"Cluster '{name}' not found")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump HCP cluster status as pretty JSON")
    parser.add_argument(
        "cluster",
        nargs="?",
        default=None,
        help="Cluster name (omit to dump all clusters)",
    )
    parser.add_argument(
        "--id",
        action="store_true",
        help="Treat the positional cluster argument as a cluster ID",
    )
    args = parser.parse_args()

    if args.id and args.cluster is None:
        parser.error("--id requires a cluster argument")

    config = hl.load_config()
    token, email, _ = hl.authenticate(config)

    try:
        if args.cluster is not None:
            cluster_id = (
                args.cluster
                if args.id
                else resolve_cluster_id(args.cluster, token, email, config)
            )
            result: Any = collect_status_bundle(cluster_id, token, email, config)
        else:
            clusters = list_clusters(token, email, config)
            result = []
            for c in clusters:
                cid = c.get("id")
                name = c.get("name", "<unknown>")
                if not cid:
                    print(f"Warning: skipping cluster '{name}' (no id)", file=sys.stderr)
                    continue
                print(f"Fetching status for cluster '{name}' ...", file=sys.stderr)
                result.append(collect_status_bundle(cid, token, email, config))
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
