from __future__ import annotations

import hashlib
import json
import os
import uuid

MANAGED_BY_TAG = {"ManagedBy": "lab9-cli"}
CLUSTER_TAGS = {"Project": "petroleum-pipeline", **MANAGED_BY_TAG}


def run_key() -> str | None:
    """Identify one CI attempt; a retry of the same workflow run gets a new attempt number"""
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id:
        return None
    return f"{run_id}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"


def cluster_tags() -> dict[str, str]:
    """Tags for cost tracking; in CI they also let `cleanup` find the clusters of one run"""
    key = run_key()
    return {**CLUSTER_TAGS, "CiRun": key} if key else dict(CLUSTER_TAGS)


def idempotency_token(command: str, target: str, params: dict[str, str] | None) -> str | None:
    """Token for one intended run; Databricks returns the old run for a repeated token"""
    key = run_key()
    if key is None:
        return None
    # Different steps, commands or parameters must not share a token
    scope = json.dumps(
        [
            os.environ.get("GITHUB_JOB"),
            os.environ.get("GITHUB_ACTION"),
            command,
            target,
            sorted((params or {}).items()),
        ]
    )
    return f"{key}-{hashlib.sha256(scope.encode()).hexdigest()[:12]}"


def unique_suffix() -> str:
    """Readable and unique cluster name suffix, also across matrix legs"""
    key = run_key()
    if key is None:
        return uuid.uuid4().hex[:8]
    return f"{os.environ.get('GITHUB_JOB', 'job')}-{key}-{uuid.uuid4().hex[:4]}"