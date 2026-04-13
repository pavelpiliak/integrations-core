# (C) Datadog, Inc. 2026-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
"""
Collects MergeTree storage health data from ClickHouse system tables.

Queries system.parts, system.merges, system.mutations, and system.replication_queue
and emits a single parts_and_merges_snapshot payload per collection cycle.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from clickhouse_connect.driver.exceptions import OperationalError

if TYPE_CHECKING:
    from datadog_checks.clickhouse import ClickhouseCheck
    from datadog_checks.clickhouse.config_models.instance import PartsAndMerges

try:
    import datadog_agent
except ImportError:
    from datadog_checks.base.stubs import datadog_agent

from datadog_checks.base.utils.common import to_native_string
from datadog_checks.base.utils.db.utils import (
    DBMAsyncJob,
    default_json_event_encoding,
    obfuscate_sql_with_metadata,
)
from datadog_checks.base.utils.serialization import json
from datadog_checks.base.utils.tracking import tracked_method

# Uses {parts_table} placeholder to support clusterAllReplicas() for ClickHouse Cloud
PARTS_QUERY = """\
SELECT
    database,
    table,
    partition,
    hostName() AS server_node,
    count()                          AS active_part_count,
    sum(rows)                        AS total_rows,
    sum(bytes_on_disk)               AS bytes_on_disk,
    sum(data_compressed_bytes)       AS compressed_bytes,
    sum(data_uncompressed_bytes)     AS uncompressed_bytes,
    max(level)                       AS max_merge_level,
    avg(level)                       AS avg_merge_level,
    min(modification_time)           AS oldest_part_time,
    max(modification_time)           AS newest_part_time
FROM {parts_table}
WHERE active = 1
  AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
GROUP BY database, table, partition, server_node
ORDER BY active_part_count DESC
LIMIT {max_parts_rows}
"""

MERGES_QUERY = """\
SELECT
    database,
    table,
    partition_id,
    hostName() AS server_node,
    elapsed,
    progress,
    num_parts,
    is_mutation,
    merge_type,
    merge_algorithm,
    total_size_bytes_compressed,
    bytes_read_uncompressed,
    rows_read,
    bytes_written_uncompressed,
    rows_written,
    memory_usage,
    source_part_names,
    result_part_name
FROM {merges_table}
"""

MUTATIONS_QUERY = """\
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    is_done,
    parts_to_do,
    latest_failed_part,
    latest_fail_time,
    latest_fail_reason
FROM {mutations_table}
WHERE NOT is_done
ORDER BY create_time ASC
LIMIT {max_mutations_rows}
"""

REPLICATION_QUEUE_QUERY = """\
SELECT
    database,
    table,
    type,
    position,
    is_currently_executing,
    num_tries,
    last_exception,
    last_exception_time,
    num_postponed,
    postpone_reason,
    parts_to_merge
FROM {replication_queue_table}
ORDER BY position ASC
LIMIT 1000
"""


def agent_check_getter(self):
    return self._check


class ClickhousePartsAndMerges(DBMAsyncJob):
    """
    Monitors MergeTree storage health by collecting snapshot data from:
    - system.parts: per-table part counts and storage stats
    - system.merges: currently executing background merges
    - system.mutations: pending ALTER UPDATE/DELETE operations
    - system.replication_queue: replication task backlog (ReplicatedMergeTree only)

    Emits a single dbm_type="parts_and_merges_snapshot" payload per collection cycle.
    """

    def __init__(self, check: ClickhouseCheck, config: PartsAndMerges):
        collection_interval = config.collection_interval

        super(ClickhousePartsAndMerges, self).__init__(
            check,
            rate_limit=1 / collection_interval,
            run_sync=config.run_sync,
            enabled=config.enabled,
            dbms="clickhouse",
            min_collection_interval=check.check_interval if hasattr(check, 'check_interval') else 15,
            expected_db_exceptions=(Exception,),
            job_name="parts-and-merges",
        )
        self._check = check
        self._config = config
        self._collection_interval = collection_interval
        self._tags_no_db: list[str] | None = None
        self.tags: list[str] | None = None

        # Dedicated client for this job (uses shared connection pool)
        self._db_client = None

        # Obfuscator options for mutation commands (strip literals, no metadata needed)
        obfuscate_options = {
            'return_json_metadata': False,
            'collect_tables': False,
            'collect_commands': False,
            'collect_comments': False,
        }
        self._obfuscate_options = to_native_string(json.dumps(obfuscate_options))

    def cancel(self):
        """Cancel the job and clean up the dedicated client."""
        super(ClickhousePartsAndMerges, self).cancel()
        self._close_db_client()

    def _close_db_client(self):
        """Close the dedicated database client if it exists."""
        if self._db_client:
            try:
                self._db_client.close()
            except Exception as e:
                self._log.debug("Error closing parts-and-merges client: %s", e)
            self._db_client = None

    def _get_debug_tags(self) -> list[str]:
        return list(self._tags_no_db) if self._tags_no_db else []

    def _execute_query(self, query: str) -> list:
        """Execute a query via the dedicated client, reconnecting on OperationalError."""
        if self._db_client is None:
            self._db_client = self._check.create_dbm_client()
        try:
            return self._db_client.query(query).result_rows
        except OperationalError as e:
            self._log.warning("Connection error, will reconnect on next query: %s", e)
            self._close_db_client()
            raise

    def _collect_parts(self) -> list[dict]:
        """Collect per-table part inventory from system.parts."""
        parts_table = self._check.get_system_table('parts')
        query = PARTS_QUERY.format(
            parts_table=parts_table,
            max_parts_rows=self._config.max_parts_rows,
        )
        try:
            rows = self._execute_query(query)
            result = []
            for row in rows:
                (
                    database, table, partition, server_node,
                    active_part_count, total_rows, bytes_on_disk,
                    compressed_bytes, uncompressed_bytes,
                    max_merge_level, avg_merge_level,
                    oldest_part_time, newest_part_time,
                ) = row
                result.append({
                    'database': database,
                    'table': table,
                    'partition': partition,
                    'server_node': server_node,
                    'active_part_count': int(active_part_count),
                    'total_rows': int(total_rows),
                    'bytes_on_disk': int(bytes_on_disk),
                    'compressed_bytes': int(compressed_bytes),
                    'uncompressed_bytes': int(uncompressed_bytes),
                    'max_merge_level': int(max_merge_level),
                    'avg_merge_level': float(avg_merge_level),
                    'oldest_part_time': int(oldest_part_time.timestamp()) if oldest_part_time else None,
                    'newest_part_time': int(newest_part_time.timestamp()) if newest_part_time else None,
                })
            self._log.debug("Collected %d rows from %s", len(result), parts_table)
            return result
        except Exception as e:
            self._log.warning("Failed to collect parts: %s", e)
            self._check.count(
                "dd.clickhouse.parts_and_merges.error",
                1,
                tags=self.tags + ["error:collect-parts"] + self._get_debug_tags(),
                raw=True,
            )
            return []

    def _collect_merges(self) -> list[dict]:
        """Collect currently executing background merges from system.merges."""
        merges_table = self._check.get_system_table('merges')
        query = MERGES_QUERY.format(merges_table=merges_table)
        try:
            rows = self._execute_query(query)
            result = []
            for row in rows:
                (
                    database, table, partition_id, server_node,
                    elapsed, progress, num_parts,
                    is_mutation, merge_type, merge_algorithm,
                    total_size_bytes_compressed,
                    bytes_read_uncompressed, rows_read,
                    bytes_written_uncompressed, rows_written,
                    memory_usage, source_part_names, result_part_name,
                ) = row
                result.append({
                    'database': database,
                    'table': table,
                    'partition_id': partition_id,
                    'server_node': server_node,
                    'elapsed': float(elapsed) if elapsed is not None else 0.0,
                    'progress': float(progress) if progress is not None else 0.0,
                    'num_parts': int(num_parts) if num_parts else 0,
                    'is_mutation': bool(is_mutation),
                    'merge_type': str(merge_type) if merge_type else None,
                    'merge_algorithm': str(merge_algorithm) if merge_algorithm else None,
                    'total_size_bytes_compressed': int(total_size_bytes_compressed) if total_size_bytes_compressed else 0,
                    'bytes_read_uncompressed': int(bytes_read_uncompressed) if bytes_read_uncompressed else 0,
                    'rows_read': int(rows_read) if rows_read else 0,
                    'bytes_written_uncompressed': int(bytes_written_uncompressed) if bytes_written_uncompressed else 0,
                    'rows_written': int(rows_written) if rows_written else 0,
                    'memory_usage': int(memory_usage) if memory_usage else 0,
                    'source_part_names': list(source_part_names) if source_part_names else [],
                    'result_part_name': str(result_part_name) if result_part_name else None,
                })
            self._log.debug("Collected %d rows from %s", len(result), merges_table)
            return result
        except Exception as e:
            self._log.warning("Failed to collect merges: %s", e)
            self._check.count(
                "dd.clickhouse.parts_and_merges.error",
                1,
                tags=self.tags + ["error:collect-merges"] + self._get_debug_tags(),
                raw=True,
            )
            return []

    def _collect_mutations(self) -> list[dict]:
        """Collect pending ALTER mutations from system.mutations."""
        mutations_table = self._check.get_system_table('mutations')
        query = MUTATIONS_QUERY.format(
            mutations_table=mutations_table,
            max_mutations_rows=self._config.max_mutations_rows,
        )
        try:
            rows = self._execute_query(query)
            result = []
            for row in rows:
                (
                    database, table, mutation_id, command,
                    create_time, is_done, parts_to_do,
                    latest_failed_part, latest_fail_time, latest_fail_reason,
                ) = row
                result.append({
                    'database': database,
                    'table': table,
                    'mutation_id': mutation_id,
                    'command': self._obfuscate_mutation_command(command),
                    'create_time': int(create_time.timestamp()) if create_time else None,
                    'is_done': bool(is_done),
                    'parts_to_do': int(parts_to_do) if parts_to_do else 0,
                    'latest_failed_part': str(latest_failed_part) if latest_failed_part else None,
                    'latest_fail_time': int(latest_fail_time.timestamp()) if latest_fail_time else None,
                    'latest_fail_reason': str(latest_fail_reason) if latest_fail_reason else None,
                })
            self._log.debug("Collected %d rows from %s", len(result), mutations_table)
            return result
        except Exception as e:
            self._log.warning("Failed to collect mutations: %s", e)
            self._check.count(
                "dd.clickhouse.parts_and_merges.error",
                1,
                tags=self.tags + ["error:collect-mutations"] + self._get_debug_tags(),
                raw=True,
            )
            return []

    def _collect_replication_queue(self) -> list[dict]:
        """Collect replication task backlog from system.replication_queue."""
        replication_queue_table = self._check.get_system_table('replication_queue')
        query = REPLICATION_QUEUE_QUERY.format(replication_queue_table=replication_queue_table)
        try:
            rows = self._execute_query(query)
            result = []
            for row in rows:
                (
                    database, table, task_type, position,
                    is_currently_executing, num_tries,
                    last_exception, last_exception_time,
                    num_postponed, postpone_reason, parts_to_merge,
                ) = row
                result.append({
                    'database': database,
                    'table': table,
                    'type': str(task_type) if task_type else None,
                    'position': int(position) if position else 0,
                    'is_currently_executing': bool(is_currently_executing),
                    'num_tries': int(num_tries) if num_tries else 0,
                    'last_exception': str(last_exception) if last_exception else None,
                    'last_exception_time': int(last_exception_time.timestamp()) if last_exception_time else None,
                    'num_postponed': int(num_postponed) if num_postponed else 0,
                    'postpone_reason': str(postpone_reason) if postpone_reason else None,
                    'parts_to_merge': list(parts_to_merge) if parts_to_merge else [],
                })
            self._log.debug("Collected %d rows from %s", len(result), replication_queue_table)
            return result
        except Exception as e:
            self._log.warning("Failed to collect replication queue: %s", e)
            self._check.count(
                "dd.clickhouse.parts_and_merges.error",
                1,
                tags=self.tags + ["error:collect-replication-queue"] + self._get_debug_tags(),
                raw=True,
            )
            return []

    def _obfuscate_mutation_command(self, command: str) -> str | None:
        """Obfuscate literal values from a mutation ALTER command."""
        if not command:
            return None
        try:
            result = obfuscate_sql_with_metadata(command, self._obfuscate_options)
            return result['query']
        except Exception:
            return None

    @tracked_method(agent_check_getter=agent_check_getter)
    def _collect_and_submit(self):
        """Collect from all four system tables and emit a single snapshot payload."""
        start_time = time.time()

        # Each collection is independent — partial results are still submitted on failure
        parts = self._collect_parts()
        merges = self._collect_merges()
        mutations = self._collect_mutations()
        replication_queue = self._collect_replication_queue()

        event = {
            "host": self._check.reported_hostname,
            "database_instance": self._check.database_identifier,
            "ddagentversion": datadog_agent.get_version(),
            "ddsource": "clickhouse",
            "dbm_type": "parts_and_merges_snapshot",
            "collection_interval": self._collection_interval,
            "ddtags": self._tags_no_db,
            "timestamp": time.time() * 1000,
            "service": getattr(self._check._config, 'service', None),
            "clickhouse_parts": parts,
            "clickhouse_merges": merges,
            "clickhouse_mutations": mutations,
            "clickhouse_replication_queue": replication_queue,
        }

        self._check.database_monitoring_metadata(json.dumps(event, default=default_json_event_encoding))

        elapsed_ms = (time.time() - start_time) * 1000
        self._check.histogram(
            "dd.clickhouse.parts_and_merges.collect.time",
            elapsed_ms,
            tags=self.tags + self._get_debug_tags(),
            raw=True,
        )
        self._log.debug(
            "Snapshot submitted: parts=%d merges=%d mutations=%d replication_queue=%d elapsed_ms=%.2f",
            len(parts),
            len(merges),
            len(mutations),
            len(replication_queue),
            elapsed_ms,
        )

    def run_job(self):
        """Main job execution method called by DBMAsyncJob."""
        self.tags = [t for t in self._tags if not t.startswith('dd.internal')]
        self._tags_no_db = [t for t in self.tags if not t.startswith('db:')]

        try:
            self._collect_and_submit()
        except Exception as e:
            self._log.exception("Failed to collect parts and merges snapshot: %s", e)
            self._check.count(
                "dd.clickhouse.parts_and_merges.error",
                1,
                tags=self.tags + ["error:run-job"] + self._get_debug_tags(),
                raw=True,
            )
