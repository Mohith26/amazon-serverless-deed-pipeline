"""DeedStream measurement harness (runs against LocalStack).

Drives the real fetcher/parser/query code paths against LocalStack-hosted S3,
SQS and DynamoDB, and records measured numbers to results/*.json:

  * backfill volume (records/day, total)
  * zero-dropped reconciliation (S3 object count == DynamoDB item count)
  * idempotency (replay a day -> DynamoDB item count unchanged)
  * parser per-record latency p50/p95/p99 (normalize + conditional PutItem)
  * DLQ quarantine of malformed records (real SQS redrive)
  * query latency p50/p95/p99 (GSI party lookups)

Transport note: the fetcher stages to S3 + SQS and the parser drains SQS (real
queue) for every record, so this exercises the same AWS APIs the deployed
Lambdas use. The docker-Lambda execution transport itself is exercised
separately by scripts/smoke_e2e.py against the SAM-deployed stack.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta

import boto3

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "src"))
sys.path.insert(0, HERE)

from deedstream import config, data_gen, fetcher  # noqa: E402
from deedstream import parser as ds_parser  # noqa: E402
from deedstream import query as ds_query  # noqa: E402
import provision_local  # noqa: E402

RESULTS_DIR = os.path.join(HERE, "..", "results")


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def latency_stats(values):
    return {
        "n": len(values),
        "p50_ms": round(percentile(values, 50), 3) if values else None,
        "p95_ms": round(percentile(values, 95), 3) if values else None,
        "p99_ms": round(percentile(values, 99), 3) if values else None,
        "mean_ms": round(sum(values) / len(values), 3) if values else None,
        "max_ms": round(max(values), 3) if values else None,
    }


def _common_prefixes(s3, prefix):
    resp = s3.list_objects_v2(Bucket=config.BRONZE_BUCKET, Prefix=prefix, Delimiter="/")
    return [c["Prefix"] for c in resp.get("CommonPrefixes", [])]


def count_s3_objects(s3):
    """Count all bronze objects by walking county/date prefixes.

    A flat paginated list is unreliable on LocalStack community S3 at scale (the
    continuation token stops advancing past ~1000 keys, though the objects exist
    and are individually retrievable). Each county/date prefix holds < 1000
    objects, so listing per prefix enumerates every object reliably.
    """
    total = 0
    for county_prefix in _common_prefixes(s3, "bronze/"):
        for date_prefix in _common_prefixes(s3, county_prefix):
            token = None
            while True:
                kw = {"Bucket": config.BRONZE_BUCKET, "Prefix": date_prefix}
                if token:
                    kw["ContinuationToken"] = token
                resp = s3.list_objects_v2(**kw)
                total += resp.get("KeyCount", 0)
                if resp.get("IsTruncated") and resp.get("NextContinuationToken"):
                    token = resp["NextContinuationToken"]
                else:
                    break
    return total


def count_ddb_items(ddb):
    total = 0
    kw = {"TableName": config.TABLE_NAME, "Select": "COUNT"}
    while True:
        resp = ddb.scan(**kw)
        total += resp.get("Count", 0)
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            return total
        kw["ExclusiveStartKey"] = lek


def drain_queue(sqs, queue_url, table, expected, durations, *, delete=True):
    """Drain exactly `expected` messages, processing each via the parser."""
    handled = written = duplicates = 0
    empty_polls = 0
    while handled < expected and empty_polls < 100:
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
        )
        messages = resp.get("Messages", [])
        if not messages:
            empty_polls += 1
            continue
        empty_polls = 0
        entries = []
        for idx, msg in enumerate(messages):
            raw = json.loads(msg["Body"])
            start = time.perf_counter()
            is_new = ds_parser.process_record(table, raw)
            durations.append((time.perf_counter() - start) * 1000.0)
            written += 1 if is_new else 0
            duplicates += 0 if is_new else 1
            handled += 1
            entries.append({"Id": str(idx), "ReceiptHandle": msg["ReceiptHandle"]})
        if delete and entries:
            sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
    return {"handled": handled, "written": written, "duplicates": duplicates}


def run_backfill(clients, days, count_per_county, durations):
    s3, sqs, ddb, res, queue_url = clients
    table = res.Table(config.TABLE_NAME)
    counties = list(data_gen.COUNTIES)
    today = date.today()
    day_list = [(today - timedelta(days=days - i)).isoformat() for i in range(days)]

    total_staged = 0
    total_written = 0
    for day in day_list:
        staged_today = 0
        for county in counties:
            staged_today += fetcher.fetch_and_stage(
                county, day, count_per_county, s3=s3, sqs=sqs, queue_url=queue_url
            )
        result = drain_queue(sqs, queue_url, table, staged_today, durations)
        total_staged += staged_today
        total_written += result["written"]
    return {
        "days": days,
        "counties": counties,
        "records_per_day_per_county": count_per_county,
        "records_per_day_total": count_per_county * len(counties),
        "total_staged": total_staged,
        "total_written": total_written,
        "first_day": day_list[0],
        "last_day": day_list[-1],
    }


def prove_idempotency(clients, replay_day, count_per_county, durations):
    s3, sqs, ddb, res, queue_url = clients
    table = res.Table(config.TABLE_NAME)
    before = count_ddb_items(ddb)
    staged = 0
    for county in data_gen.COUNTIES:
        staged += fetcher.fetch_and_stage(
            county, replay_day, count_per_county, s3=s3, sqs=sqs, queue_url=queue_url
        )
    result = drain_queue(sqs, queue_url, table, staged, durations)
    after = count_ddb_items(ddb)
    return {
        "replay_day": replay_day,
        "replayed_records": staged,
        "ddb_before": before,
        "ddb_after": after,
        "item_count_unchanged": before == after,
        "duplicates_detected": result["duplicates"],
        "new_writes_on_replay": result["written"],
    }


def prove_dlq(clients, k=25, timeout=45):
    s3, sqs, ddb, res, queue_url = clients
    table = res.Table(config.TABLE_NAME)
    dlq_url = sqs.get_queue_url(QueueName=config.DLQ_NAME)["QueueUrl"]

    # Missing-field records (valid JSON) + one non-JSON body -> all malformed.
    bodies = [json.dumps({"county": "HARRIS", "doc_id": f"BAD-{i}"}) for i in range(k - 1)]
    bodies.append("{not valid json")
    for start in range(0, len(bodies), 10):
        chunk = bodies[start : start + 10]
        sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[{"Id": str(i), "MessageBody": b} for i, b in enumerate(chunk)],
        )

    injected = len(bodies)
    deadline = time.time() + timeout
    depth = 0
    while time.time() < deadline:
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=1
        )
        for msg in resp.get("Messages", []):
            try:
                ds_parser.process_record(table, json.loads(msg["Body"]))
            except Exception:  # noqa: BLE001 - malformed: leave in queue for redrive
                pass
        attrs = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]
        depth = int(attrs["ApproximateNumberOfMessages"])
        if depth >= injected:
            break
        time.sleep(1.0)
    return {"injected": injected, "dlq_depth": depth, "all_quarantined": depth >= injected}


def measure_query_latency(clients, sample_day, count_per_county, iterations=300):
    _, _, _, res, _ = clients
    table = res.Table(config.TABLE_NAME)
    parties = []
    for county in data_gen.COUNTIES:
        for rec in data_gen.generate_day(county, sample_day, count_per_county):
            parties.append(rec.grantor)
            parties.append(rec.grantee)
    parties = parties[: max(iterations, 1)]

    durations = []
    hits = 0
    for i in range(iterations):
        party = parties[i % len(parties)]
        start = time.perf_counter()
        results = ds_query.query_records(party=party, table=table)
        durations.append((time.perf_counter() - start) * 1000.0)
        hits += 1 if results else 0
    stats = latency_stats(durations)
    stats["iterations"] = iterations
    stats["nonempty_results"] = hits
    return stats


def write_json(name, payload):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, name)
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    return path


def localstack_version():
    import urllib.request

    try:
        with urllib.request.urlopen("http://localhost:4566/_localstack/health", timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception:  # noqa: BLE001
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--count-per-county", type=int, default=config.DEFAULT_RECORDS_PER_DAY)
    ap.add_argument("--query-iters", type=int, default=300)
    args = ap.parse_args()

    if not config.endpoint_url():
        print("ERROR: set AWS_ENDPOINT_URL=http://localhost:4566 to target LocalStack")
        sys.exit(2)

    provision_local.provision()
    kw = {"region_name": config.AWS_REGION, "endpoint_url": config.endpoint_url()}
    s3 = boto3.client("s3", **kw)
    sqs = boto3.client("sqs", **kw)
    ddb = boto3.client("dynamodb", **kw)
    res = boto3.resource("dynamodb", **kw)
    queue_url = sqs.get_queue_url(QueueName=config.QUEUE_NAME)["QueueUrl"]
    clients = (s3, sqs, ddb, res, queue_url)

    parser_durations = []
    wall_start = time.time()

    print(f"[1/5] backfill {args.days} days @ {args.count_per_county}/county/day ...")
    backfill = run_backfill(clients, args.days, args.count_per_county, parser_durations)
    backfill_seconds = round(time.time() - wall_start, 2)

    print("[2/5] reconciliation (S3 vs DynamoDB) ...")
    s3_count = count_s3_objects(s3)
    ddb_count = count_ddb_items(ddb)
    reconciliation = {
        "s3_object_count": s3_count,
        "dynamodb_item_count": ddb_count,
        "reconciled": s3_count == ddb_count,
        "zero_dropped": s3_count == ddb_count == backfill["total_staged"],
        "expected_total": backfill["total_staged"],
    }

    print("[3/5] idempotency (replay first day) ...")
    idempotency = prove_idempotency(
        clients, backfill["first_day"], args.count_per_county, parser_durations
    )

    print("[4/5] DLQ quarantine (malformed records) ...")
    dlq = prove_dlq(clients)

    print("[5/5] query latency (GSI party lookups) ...")
    query_stats = measure_query_latency(
        clients, backfill["last_day"], args.count_per_county, args.query_iters
    )

    parser_stats = latency_stats(parser_durations)
    total_seconds = round(time.time() - wall_start, 2)
    throughput = round(backfill["total_staged"] / backfill_seconds, 1) if backfill_seconds else None

    write_json("backfill.json", {**backfill, "backfill_seconds": backfill_seconds,
                                 "records_per_second": throughput})
    write_json("reconciliation.json", reconciliation)
    write_json("idempotency.json", idempotency)
    write_json("parser_latency.json", {**parser_stats,
                                       "description": "normalize + conditional PutItem vs LocalStack DynamoDB"})
    write_json("dlq.json", dlq)
    write_json("query_latency.json", {**query_stats,
                                      "description": "GSI grantor/grantee party lookup vs LocalStack DynamoDB"})

    summary = {
        "generated_at": date.today().isoformat(),
        "target": "LocalStack (emulated AWS) - community edition",
        "localstack_health": localstack_version(),
        "backfill": {**backfill, "backfill_seconds": backfill_seconds, "records_per_second": throughput},
        "reconciliation": reconciliation,
        "idempotency": idempotency,
        "parser_latency": parser_stats,
        "query_latency": query_stats,
        "dlq": dlq,
        "total_wall_seconds": total_seconds,
        "cost_per_month_usd": None,
        "cost_status": "UNMEASURED/BLOCKED - LocalStack is free/local; needs a real AWS deploy",
    }
    write_json("summary.json", summary)

    print("\n=== SUMMARY ===")
    print(json.dumps({
        "records_per_day_total": backfill["records_per_day_total"],
        "total_records": backfill["total_staged"],
        "reconciled_zero_dropped": reconciliation["zero_dropped"],
        "idempotent": idempotency["item_count_unchanged"],
        "parser_p95_ms": parser_stats["p95_ms"],
        "query_p95_ms": query_stats["p95_ms"],
        "dlq_quarantined": dlq["all_quarantined"],
    }, indent=2))


if __name__ == "__main__":
    main()
