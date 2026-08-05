"""Fetcher Lambda: pulls a day's filings and stages them.

Writes one raw payload per record to S3 (bronze) and enqueues one SQS message
per record. Triggered by an EventBridge scheduled rule on real AWS; invoked
directly by the harness / tests locally.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, timedelta

from . import config, data_gen

_SQS_BATCH = 10  # SQS SendMessageBatch hard limit


def fetch_and_stage(
    county: str,
    day,
    count: int,
    *,
    s3=None,
    sqs=None,
    queue_url: str | None = None,
) -> int:
    """Generate a day's synthetic records, write each to S3 and SQS.

    Returns the number of records staged. Idempotent on S3 (deterministic keys);
    duplicate SQS messages are harmless because the parser writes idempotently.
    """
    s3 = s3 or config.client("s3")
    sqs = sqs or config.client("sqs")
    if queue_url is None:
        queue_url = sqs.get_queue_url(QueueName=config.QUEUE_NAME)["QueueUrl"]

    records = [asdict(r) for r in data_gen.generate_day(county, day, count)]

    for rec in records:
        s3.put_object(
            Bucket=config.BRONZE_BUCKET,
            Key=config.bronze_key(rec["county"], rec["filing_date"], rec["doc_id"]),
            Body=json.dumps(rec).encode("utf-8"),
        )

    for start in range(0, len(records), _SQS_BATCH):
        chunk = records[start : start + _SQS_BATCH]
        entries = [
            {"Id": str(idx), "MessageBody": json.dumps(rec)}
            for idx, rec in enumerate(chunk)
        ]
        sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)

    return len(records)


def lambda_handler(event, context):
    """EventBridge-scheduled entrypoint.

    Event overrides (all optional): ``date`` (ISO string), ``counties`` (list),
    ``count`` (records per county). Defaults to yesterday's filings for every
    configured county.
    """
    event = event or {}
    day = event.get("date") or (date.today() - timedelta(days=1)).isoformat()
    counties = event.get("counties") or list(data_gen.COUNTIES)
    count = int(event.get("count", config.DEFAULT_RECORDS_PER_DAY))

    total = sum(fetch_and_stage(c, day, count) for c in counties)
    return {"staged": total, "date": day, "counties": counties, "count_per_county": count}
