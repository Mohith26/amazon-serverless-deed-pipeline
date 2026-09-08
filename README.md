# DeedStream

An event-driven serverless pipeline for county deed and foreclosure records. A scheduled fetcher pulls a day of filings, stages one raw object per record in S3 and one message per record in SQS, a parser Lambda normalizes each record into a single-table DynamoDB design, and a query Lambda behind API Gateway serves lookups by county and date or by party name. Malformed records land in a dead-letter queue with a redrive runbook. The whole stack is one SAM template.

Two things to know up front. It runs on LocalStack, a Docker-based AWS emulator, with no real AWS deployment behind these numbers. And the data is synthetic (`src/deedstream/data_gen.py`), not scraped filings. I wanted the correctness story (nothing dropped, replays are safe, bad input is quarantined) without paying for a cloud account, and LocalStack was enough for that.

## Design

The DynamoDB table uses `PK = COUNTY#filing_date` and `SK = doc_id`, with two GSIs (`grantor-index` and `grantee-index`, each keyed on a normalized party name plus filing date) so a party lookup queries both indexes and merges the results.

Idempotency is a conditional write: `ConditionExpression = attribute_not_exists(PK) AND attribute_not_exists(SK)`, so replaying a day writes nothing twice. Reconciliation is the simplest possible check: one S3 object per record and one DynamoDB item per record, so if the counts match nothing was dropped.

The parser consumes SQS through an event-source mapping with batch size 10 and `ReportBatchItemFailures`, so one malformed record fails alone instead of poisoning the batch. After three receives the redrive policy moves it to the DLQ. `docs/RUNBOOK.md` covers getting it back out.

## Results

Measured on 2026-08-05 against LocalStack community 3.8.1. Raw JSON is in `results/`, and `RESULTS.md` has the full write-up.

| what | value |
|---|---|
| records per day | 1,000 (500 each for two counties) |
| backfill | 30 days, 30,000 records, 63.4 records/s (472.9 s) |
| reconciliation | S3 30,000, DynamoDB 30,000, expected 30,000 |
| replay of one day | 1,000 duplicates detected, 0 new writes, item count unchanged |
| parser latency (n=31,000) | p50 8.9 ms, p95 24.2 ms, p99 47.0 ms |
| query latency (n=500 party lookups) | p50 23.1 ms, p95 37.4 ms, p99 49.8 ms |
| DLQ | 25 malformed injected, 25 quarantined |

Those numbers come from `scripts/run_pipeline.py`, which drives the fetcher, parser and query code against LocalStack S3, SQS and DynamoDB at volume. Parser latency is `normalize()` plus the conditional `PutItem`, timed client-side over the backfill and the replay together.

A separate smoke test (`scripts/smoke_e2e.py`) runs against the SAM stack deployed to LocalStack so the Lambda, event-source mapping and API Gateway wiring is exercised: the fetcher staged 40 records, the parser wrote 40 items, `GET /records?county=HARRIS&date=2026-08-04` returned 40, and a malformed message reached the DLQ after roughly two minutes (3 receives at a 60 s visibility timeout). Docker-backed Lambda invocation is too slow locally for tens of thousands of records, which is why the scale run uses the harness.

Monthly cost is not measured; LocalStack emits no billing data.

## Trying it out

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

docker run -d --name deedstream-localstack -p 4566:4566 \
  -e SERVICES=s3,sqs,dynamodb,lambda,events,apigateway,cloudformation,logs,iam,sts,cloudwatch \
  -v /var/run/docker.sock:/var/run/docker.sock localstack/localstack:3.8.1

export AWS_ENDPOINT_URL=http://localhost:4566 AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test

pytest -q                                                          # moto unit tests plus one LocalStack integration test
python scripts/run_pipeline.py --days 30 --count-per-county 500    # the measured backfill, writes results/*.json
bash scripts/deploy_local.sh                                       # samlocal build && samlocal deploy
python scripts/smoke_e2e.py
```

The Lambda code only reads `AWS_ENDPOINT_URL` when present, so the same template should deploy to a real account with `sam build && sam deploy --guided`. I have not done that.

```
template.yaml        the whole stack as SAM
src/deedstream/      fetcher.py, parser.py, query.py, data_gen.py, config.py
scripts/             run_pipeline.py, smoke_e2e.py, deploy_local.sh, provision_local.py, cost_report.py
tests/               moto unit tests, LocalStack integration test
docs/RUNBOOK.md      DLQ redrive procedure
results/             benchmark JSON
```

## Things that broke

LocalStack community's S3 `list_objects_v2` stops advancing its continuation token past roughly 1,000 keys even though every object exists and can be fetched individually. The reconciliation counter walks `county=/date=` prefixes instead, each holding fewer than 1,000 objects. See `count_s3_objects` in `scripts/run_pipeline.py`.

## What's missing

No CloudWatch alarms: LocalStack community does not meaningfully emulate alarm state, so DLQ-depth and error-rate alarms were never built or measured. The API Gateway path is tested functionally, not load-tested. And again: emulated AWS, synthetic data, no cost figure.
