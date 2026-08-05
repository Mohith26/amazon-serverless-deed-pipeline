"""LocalStack integration test: proves reconciliation + idempotency end-to-end.

Requires a running LocalStack on http://localhost:4566. Skipped automatically if
it is unreachable, so `pytest` stays green without LocalStack.
"""

import os
import urllib.request

import boto3
import pytest

pytestmark = pytest.mark.integration

ENDPOINT = "http://localhost:4566"


def _localstack_up():
    try:
        with urllib.request.urlopen(f"{ENDPOINT}/_localstack/health", timeout=3) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture(scope="module", autouse=True)
def _require_localstack():
    if not _localstack_up():
        pytest.skip("LocalStack not running on :4566")


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", ENDPOINT)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    # Unique table/bucket/queue per run so this test is self-contained.
    suffix = str(os.getpid())
    monkeypatch.setenv("TABLE_NAME", f"deedstream-it-{suffix}")
    monkeypatch.setenv("BRONZE_BUCKET", f"deedstream-it-bronze-{suffix}")
    monkeypatch.setenv("QUEUE_NAME", f"deedstream-it-{suffix}")
    monkeypatch.setenv("DLQ_NAME", f"deedstream-it-dlq-{suffix}")


def test_reconciliation_and_idempotency(env):
    import importlib

    from deedstream import config as config_mod

    importlib.reload(config_mod)  # pick up per-run resource names
    from deedstream import fetcher, parser as ds_parser
    import provision_local
    import run_pipeline

    importlib.reload(provision_local)
    importlib.reload(run_pipeline)

    provision_local.provision()
    kw = {"region_name": "us-east-1", "endpoint_url": ENDPOINT}
    s3 = boto3.client("s3", **kw)
    sqs = boto3.client("sqs", **kw)
    ddb = boto3.client("dynamodb", **kw)
    res = boto3.resource("dynamodb", **kw)
    queue_url = sqs.get_queue_url(QueueName=config_mod.QUEUE_NAME)["QueueUrl"]
    table = res.Table(config_mod.TABLE_NAME)

    day = "2026-08-01"
    count = 40
    staged = sum(
        fetcher.fetch_and_stage(c, day, count, s3=s3, sqs=sqs, queue_url=queue_url)
        for c in ("HARRIS", "DALLAS")
    )

    durations = []
    run_pipeline.drain_queue(sqs, queue_url, table, staged, durations)

    s3_count = run_pipeline.count_s3_objects(s3)
    ddb_count = run_pipeline.count_ddb_items(ddb)
    assert s3_count == ddb_count == staged  # zero dropped

    # Replay the same day -> item count unchanged (idempotent).
    before = ddb_count
    for c in ("HARRIS", "DALLAS"):
        fetcher.fetch_and_stage(c, day, count, s3=s3, sqs=sqs, queue_url=queue_url)
    run_pipeline.drain_queue(sqs, queue_url, table, staged, [])
    assert run_pipeline.count_ddb_items(ddb) == before
