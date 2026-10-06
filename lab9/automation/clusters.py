from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import ClusterDetails, DataSecurityMode

logger = logging.getLogger(__name__)


def _request_cluster(
    w: WorkspaceClient,
    name: str,
    autotermination_minutes: int,
    node_type_id: str | None,
    custom_tags: dict[str, str] | None,
    policy_id: str | None,
) -> str:
    node_type = node_type_id or w.clusters.select_node_type(local_disk=True, min_memory_gb=16)
    waiter = w.clusters.create(
        cluster_name=name,
        spark_version=w.clusters.select_spark_version(long_term_support=True),
        node_type_id=node_type,
        num_workers=0,
        autotermination_minutes=autotermination_minutes,
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=w.current_user.me().user_name,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        custom_tags={"ResourceClass": "SingleNode", **(custom_tags or {})},
        policy_id=policy_id,
    )
    logger.info("Cluster '%s' requested, id=%s", name, waiter.cluster_id)
    return waiter.cluster_id


def _wait_until_running(w: WorkspaceClient, cluster_id: str, timeout_minutes: int) -> None:
    w.clusters.wait_get_cluster_running(cluster_id, timeout=timedelta(minutes=timeout_minutes))
    logger.info("Cluster %s is running", cluster_id)


def delete_cluster(w: WorkspaceClient, cluster_id: str) -> None:
    try:
        # permanent_delete removes the cluster; a plain delete only stops it
        # and keeps it in the list for 30 days
        w.clusters.permanent_delete(cluster_id)
    except NotFound:
        logger.info("Cluster %s is already deleted", cluster_id)
        return
    logger.info("Cluster %s deleted", cluster_id)


def _delete_cluster_quietly(w: WorkspaceClient, cluster_id: str) -> None:
    """Delete a cluster in cleanup code: a failure is logged, never raised"""
    try:
        delete_cluster(w, cluster_id)
    # broad on purpose: cleanup must not replace the original error
    except Exception:
        logger.exception(
            "Could not delete cluster %s; it will stop by autotermination", cluster_id
        )


def find_clusters_by_tags(w: WorkspaceClient, tags: dict[str, str]) -> list[ClusterDetails]:
    """Return clusters that have all the given tags (the API cannot filter by tags)."""
    if not tags:
        raise ValueError("At least one tag is required")
    return [
        cluster
        for cluster in w.clusters.list()
        if tags.items() <= (cluster.custom_tags or {}).items()
    ]


def create_cluster(
    w: WorkspaceClient,
    name: str,
    autotermination_minutes: int = 15,
    timeout_minutes: int = 30,
    node_type_id: str | None = None,
    custom_tags: dict[str, str] | None = None,
    policy_id: str | None = None,
) -> str:
    cluster_id = _request_cluster(
        w, name, autotermination_minutes, node_type_id, custom_tags, policy_id
    )
    try:
        _wait_until_running(w, cluster_id, timeout_minutes)
    except BaseException:
        # Not just Exception: Ctrl+C and SIGTERM during the wait must delete the cluster too.
        # The error is re-raised below, so nothing is swallowed.
        _delete_cluster_quietly(w, cluster_id)
        raise
    return cluster_id


@contextmanager
def temporary_cluster(
    w: WorkspaceClient,
    name: str,
    autotermination_minutes: int = 15,
    timeout_minutes: int = 30,
    node_type_id: str | None = None,
    custom_tags: dict[str, str] | None = None,
    policy_id: str | None = None,
) -> Iterator[str]:
    cluster_id = _request_cluster(
        w, name, autotermination_minutes, node_type_id, custom_tags, policy_id
    )
    try:
        _wait_until_running(w, cluster_id, timeout_minutes)
        yield cluster_id
    finally:
        _delete_cluster_quietly(w, cluster_id)
        