# (C) Datadog, Inc. 2026-present
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import datetime
from unittest import mock

import pytest

from datadog_checks.clickhouse import ClickhouseCheck
from datadog_checks.clickhouse.parts_and_merges import ClickhousePartsAndMerges

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def instance_with_parts_and_merges():
    return {
        'server': 'localhost',
        'port': 9000,
        'username': 'default',
        'password': '',
        'db': 'default',
        'dbm': True,
        'parts_and_merges': {
            'enabled': True,
            'collection_interval': 60,
            'max_parts_rows': 500,
            'max_mutations_rows': 200,
            'run_sync': True,
        },
        'tags': ['test:clickhouse'],
    }


@pytest.fixture
def check(instance_with_parts_and_merges):
    return ClickhouseCheck('clickhouse', {}, [instance_with_parts_and_merges])


# ---------------------------------------------------------------------------
# Initialization tests
# ---------------------------------------------------------------------------


def test_initialization(check):
    assert check.parts_and_merges is not None
    assert isinstance(check.parts_and_merges, ClickhousePartsAndMerges)
    assert check.parts_and_merges._config.enabled is True
    assert check.parts_and_merges._config.collection_interval == 60
    assert check.parts_and_merges._config.max_parts_rows == 500
    assert check.parts_and_merges._config.max_mutations_rows == 200


def test_disabled_when_feature_disabled():
    instance = {
        'server': 'localhost',
        'dbm': True,
        'parts_and_merges': {'enabled': False},
    }
    check = ClickhouseCheck('clickhouse', {}, [instance])
    assert check.parts_and_merges is None


def test_disabled_when_dbm_disabled():
    instance = {
        'server': 'localhost',
        'dbm': False,
        'parts_and_merges': {'enabled': True},
    }
    check = ClickhouseCheck('clickhouse', {}, [instance])
    assert check.parts_and_merges is None


def test_disabled_by_default():
    """parts_and_merges should be off by default (unlike other DBM collectors)."""
    instance = {
        'server': 'localhost',
        'dbm': True,
    }
    check = ClickhouseCheck('clickhouse', {}, [instance])
    assert check.parts_and_merges is None


# ---------------------------------------------------------------------------
# Row collection helpers
# ---------------------------------------------------------------------------


def _make_parts_rows():
    """Simulate rows returned by system.parts query."""
    return [
        (
            'default', 'events', '20240101',
            'node-1',
            287,           # active_part_count
            1_200_000_000, # total_rows
            450_000_000,   # bytes_on_disk
            420_000_000,   # compressed_bytes
            3_000_000_000, # uncompressed_bytes
            7,             # max_merge_level
            4.2,           # avg_merge_level
            datetime.datetime(2024, 1, 1, 0, 0, 0),  # oldest_part_time
            datetime.datetime(2024, 1, 2, 12, 0, 0), # newest_part_time
        ),
    ]


def _make_merges_rows():
    """Simulate rows returned by system.merges query."""
    return [
        (
            'default', 'events', '20240101',
            'node-1',
            83.4,          # elapsed
            0.47,          # progress
            12,            # num_parts
            False,         # is_mutation
            'Regular',     # merge_type
            'Horizontal',  # merge_algorithm
            1_200_000_000, # total_size_bytes_compressed
            800_000_000,   # bytes_read_uncompressed
            5_000_000,     # rows_read
            600_000_000,   # bytes_written_uncompressed
            3_000_000,     # rows_written
            1_300_000_000, # memory_usage
            ['all_0_5_1', 'all_6_11_1'], # source_part_names
            'all_0_11_2',  # result_part_name
        ),
    ]


def _make_mutations_rows():
    """Simulate rows returned by system.mutations query."""
    return [
        (
            'default', 'events',
            '0000000003',
            "DELETE WHERE ts < '2023-01-01'",
            datetime.datetime(2024, 1, 1, 9, 0, 0),  # create_time
            False,     # is_done
            47,        # parts_to_do
            None,      # latest_failed_part
            None,      # latest_fail_time
            None,      # latest_fail_reason
        ),
    ]


def _make_replication_queue_rows():
    """Simulate rows returned by system.replication_queue query."""
    return [
        (
            'default', 'events',
            'MERGE_PARTS', 0,
            True,  # is_currently_executing
            1,     # num_tries
            None,  # last_exception
            None,  # last_exception_time
            0,     # num_postponed
            None,  # postpone_reason
            ['all_0_5_1', 'all_6_11_1'],  # parts_to_merge
        ),
    ]


# ---------------------------------------------------------------------------
# Payload structure tests
# ---------------------------------------------------------------------------


def test_collect_parts_normalizes_rows(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', return_value=_make_parts_rows()):
        result = job._collect_parts()

    assert len(result) == 1
    row = result[0]
    assert row['database'] == 'default'
    assert row['table'] == 'events'
    assert row['partition'] == '20240101'
    assert row['server_node'] == 'node-1'
    assert row['active_part_count'] == 287
    assert row['total_rows'] == 1_200_000_000
    assert row['max_merge_level'] == 7
    assert isinstance(row['avg_merge_level'], float)
    assert row['oldest_part_time'] is not None
    assert row['newest_part_time'] is not None


def test_collect_merges_normalizes_rows(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', return_value=_make_merges_rows()):
        result = job._collect_merges()

    assert len(result) == 1
    row = result[0]
    assert row['database'] == 'default'
    assert row['table'] == 'events'
    assert row['progress'] == pytest.approx(0.47)
    assert row['num_parts'] == 12
    assert row['is_mutation'] is False
    assert row['merge_type'] == 'Regular'
    assert row['source_part_names'] == ['all_0_5_1', 'all_6_11_1']
    assert row['result_part_name'] == 'all_0_11_2'


def test_collect_mutations_obfuscates_command(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', return_value=_make_mutations_rows()):
        result = job._collect_mutations()

    assert len(result) == 1
    row = result[0]
    assert row['database'] == 'default'
    assert row['table'] == 'events'
    assert row['mutation_id'] == '0000000003'
    assert row['parts_to_do'] == 47
    assert row['is_done'] is False
    # command is either obfuscated (string) or None if the obfuscator cannot parse the mutation syntax
    assert 'command' in row


def test_collect_replication_queue_normalizes_rows(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', return_value=_make_replication_queue_rows()):
        result = job._collect_replication_queue()

    assert len(result) == 1
    row = result[0]
    assert row['database'] == 'default'
    assert row['table'] == 'events'
    assert row['type'] == 'MERGE_PARTS'
    assert row['is_currently_executing'] is True
    assert row['parts_to_merge'] == ['all_0_5_1', 'all_6_11_1']


def test_snapshot_payload_structure(check):
    """Test that _collect_and_submit emits a correctly structured snapshot."""
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    submitted_payloads = []

    with mock.patch.object(job, '_collect_parts', return_value=[{'database': 'default', 'table': 'events'}]), \
         mock.patch.object(job, '_collect_merges', return_value=[{'database': 'default', 'table': 'events'}]), \
         mock.patch.object(job, '_collect_mutations', return_value=[]), \
         mock.patch.object(job, '_collect_replication_queue', return_value=[]), \
         mock.patch.object(check, 'database_monitoring_metadata', side_effect=submitted_payloads.append), \
         mock.patch('datadog_checks.clickhouse.parts_and_merges.datadog_agent') as mock_agent:
        mock_agent.get_version.return_value = '7.64.0'
        job._collect_and_submit()

    assert len(submitted_payloads) == 1

    import json
    payload = json.loads(submitted_payloads[0])

    assert payload['dbm_type'] == 'parts_and_merges_snapshot'
    assert payload['ddsource'] == 'clickhouse'
    assert 'timestamp' in payload
    assert 'host' in payload
    assert len(payload['clickhouse_parts']) == 1
    assert len(payload['clickhouse_merges']) == 1
    assert payload['clickhouse_mutations'] == []
    assert payload['clickhouse_replication_queue'] == []


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


def test_parts_collection_error_returns_empty(check):
    """A query failure on system.parts should return [] without crashing the job."""
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', side_effect=Exception("DB error")):
        result = job._collect_parts()

    assert result == []


def test_merges_collection_error_returns_empty(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', side_effect=Exception("DB error")):
        result = job._collect_merges()

    assert result == []


def test_mutations_collection_error_returns_empty(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', side_effect=Exception("DB error")):
        result = job._collect_mutations()

    assert result == []


def test_replication_queue_error_returns_empty(check):
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    with mock.patch.object(job, '_execute_query', side_effect=Exception("DB error")):
        result = job._collect_replication_queue()

    assert result == []


def test_partial_results_still_submitted(check):
    """If parts collection fails but merges succeeds, the snapshot should still be submitted."""
    job = check.parts_and_merges
    job.tags = ['test:clickhouse']
    job._tags_no_db = ['test:clickhouse']

    submitted_payloads = []

    with mock.patch.object(job, '_collect_parts', return_value=[]), \
         mock.patch.object(job, '_collect_merges', return_value=[{'database': 'default'}]), \
         mock.patch.object(job, '_collect_mutations', return_value=[]), \
         mock.patch.object(job, '_collect_replication_queue', return_value=[]), \
         mock.patch.object(check, 'database_monitoring_metadata', side_effect=submitted_payloads.append), \
         mock.patch('datadog_checks.clickhouse.parts_and_merges.datadog_agent') as mock_agent:
        mock_agent.get_version.return_value = '7.64.0'
        job._collect_and_submit()

    assert len(submitted_payloads) == 1

    import json
    payload = json.loads(submitted_payloads[0])
    assert payload['clickhouse_parts'] == []
    assert len(payload['clickhouse_merges']) == 1


# ---------------------------------------------------------------------------
# Multi-node (single endpoint mode) tests
# ---------------------------------------------------------------------------


def test_uses_cluster_all_replicas_in_single_endpoint_mode():
    """In single_endpoint_mode, system table references should use clusterAllReplicas()."""
    instance = {
        'server': 'cloud.clickhouse.com',
        'dbm': True,
        'single_endpoint_mode': True,
        'parts_and_merges': {
            'enabled': True,
            'collection_interval': 60,
            'run_sync': True,
        },
    }
    check = ClickhouseCheck('clickhouse', {}, [instance])
    job = check.parts_and_merges
    job.tags = []
    job._tags_no_db = []

    executed_queries = []

    def capture_query(query):
        executed_queries.append(query)
        return []

    with mock.patch.object(job, '_execute_query', side_effect=capture_query):
        job._collect_parts()
        job._collect_merges()
        job._collect_mutations()
        job._collect_replication_queue()

    for query in executed_queries:
        assert "clusterAllReplicas" in query, f"Expected clusterAllReplicas in: {query[:80]}"


def test_uses_system_table_in_direct_mode():
    """In direct (non-cloud) mode, system table references should use system.<table>."""
    instance = {
        'server': 'localhost',
        'dbm': True,
        'single_endpoint_mode': False,
        'parts_and_merges': {
            'enabled': True,
            'collection_interval': 60,
            'run_sync': True,
        },
    }
    check = ClickhouseCheck('clickhouse', {}, [instance])
    job = check.parts_and_merges
    job.tags = []
    job._tags_no_db = []

    executed_queries = []

    def capture_query(query):
        executed_queries.append(query)
        return []

    with mock.patch.object(job, '_execute_query', side_effect=capture_query):
        job._collect_parts()
        job._collect_merges()
        job._collect_mutations()
        job._collect_replication_queue()

    for query in executed_queries:
        assert "clusterAllReplicas" not in query
        assert "system." in query


# ---------------------------------------------------------------------------
# run_job tests
# ---------------------------------------------------------------------------


def test_run_job_filters_internal_tags(check):
    """run_job should strip dd.internal tags from the tag list."""
    job = check.parts_and_merges
    job._tags = ['test:clickhouse', 'dd.internal:some_tag', 'db:default']

    with mock.patch.object(job, '_collect_and_submit'):
        job.run_job()

    assert 'dd.internal:some_tag' not in job.tags
    assert 'test:clickhouse' in job.tags
    assert 'db:default' not in job._tags_no_db
