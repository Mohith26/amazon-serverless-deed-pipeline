# DeedStream — Serverless County Records Pipeline

Event-driven, fully **serverless** AWS pipeline that ingests, normalizes and
indexes county deed/foreclosure records: **EventBridge → Lambda → S3 + SQS →
Lambda → DynamoDB (single-table + GSIs) → API Gateway**, with a DLQ on the
records queue and **one-command SAM deploys**. Engineered for zero dropped
records and idempotent replays.

> ### ⚠️ Runs on LocalStack (emulated AWS), not real AWS
> This project is built, run and **measured entirely on [LocalStack](https://localstack.cloud/)**
> (a free, Docker-based AWS emulator) — there is **no real AWS account or cloud
> deploy**. Data is **100% synthetic** and seeded (see `src/deedstream/data_gen.py`);
> no real county records are scraped. **Monthly cost is unmeasured** because
> LocalStack is free/local and emits no billing data. See
> [Limitations](#limitations) and [`RESULTS.md`](RESULTS.md).

Target role framing: **Amazon SDE** — AWS-native architecture (Lambda, SQS,
DynamoDB single-table, EventBridge, S3, API Gateway), an ownership/operational-
excellence story (idempotency, DLQ + redrive runbook, S3↔DynamoDB reconciliation),
and frugality (serverless, pay-per-request).

## Architecture

```
                 ┌───────────────────────┐
 EventBridge     │   Fetcher Lambda      │        S3 (bronze)  one raw object/record
 rate(1 day) ───▶│  (synthetic day pull) │──┬────▶ s3://.../bronze/county=/date=/doc.json
                 └───────────────────────┘  │
                                            └────▶ SQS  deedstream-records  (1 msg/record)
                                                       │
                                                       ▼  (event-source mapping, batch 10,
                                                       │   ReportBatchItemFailures)
                                          ┌────────────────────────┐
                                          │     Parser Lambda      │  normalize +
                                          │  idempotent cond. Put  │  attribute_not_exists
                                          └────────────────────────┘
                                                       │                 malformed ▲
                                                       ▼                   redrive  │
                          DynamoDB  deedstream-records (single table)     SQS DLQ ──┘
                          PK = COUNTY#date   SK = doc_id                deedstream-records-dlq
                          GSI grantor-index (grantor_key, filing_date)
                          GSI grantee-index (grantee_key, filing_date)
                                                       ▲
                          API Gateway  GET /records ───┘
                          ?county=&date=&party=  ──▶  Query Lambda  ──▶  base table / GSIs
```

Idempotency: the parser writes with `ConditionExpression=attribute_not_exists(PK)
AND attribute_not_exists(SK)`, so replaying a day produces **zero** duplicates.
Reconciliation: one S3 object per record + one DynamoDB item per record ⇒ counts
must match ("zero dropped").

## Tech stack
Python 3.12 Lambdas · **AWS SAM** (`template.yaml`, one-command deploy) · DynamoDB
single-table + 2 GSIs · SQS + DLQ · S3 · EventBridge schedule · API Gateway ·
moto (unit tests) · LocalStack (integration + measurement) · GitHub Actions CI.

## Layout
```
template.yaml            SAM IaC — EventBridge, 3 Lambdas, S3, SQS+DLQ, DynamoDB+GSIs, API GW
src/deedstream/          fetcher.py · parser.py · query.py · data_gen.py (synthetic) · config.py
scripts/                 provision_local.py · run_pipeline.py (measurement) · smoke_e2e.py
                         deploy_local.sh · cost_report.py
tests/                   moto unit tests + LocalStack integration test (idempotency + reconciliation)
docs/RUNBOOK.md          DLQ redrive runbook
results/*.json           committed measured numbers (dated 2026-08-05)
RESULTS.md / BULLETS.md  measured write-up + resume bullets
```

## Quickstart (LocalStack)

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt awscli aws-sam-cli

# free LocalStack community (3.x needs no account)
docker run -d --name deedstream-localstack -p 4566:4566 \
  -e SERVICES=s3,sqs,dynamodb,lambda,events,apigateway,cloudformation,logs,iam,sts,cloudwatch \
  -v /var/run/docker.sock:/var/run/docker.sock localstack/localstack:3.8.1

export AWS_ENDPOINT_URL=http://localhost:4566 AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test

pytest -q                                                    # unit + integration tests
python scripts/run_pipeline.py --days 30 --count-per-county 500   # measured backfill -> results/*.json
bash scripts/deploy_local.sh                                 # one-command SAM deploy to LocalStack
python scripts/smoke_e2e.py                                  # deployed-stack E2E proof
python scripts/cost_report.py                                # cost: BLOCKED on LocalStack
```

### One-command deploy
`bash scripts/deploy_local.sh` runs `samlocal build && samlocal deploy` against
LocalStack. On real AWS the identical template deploys with
`sam build && sam deploy --guided` (the Lambda code is endpoint-agnostic — it
only reads `AWS_ENDPOINT_URL` when present).

### Query API
```
GET /records?county=HARRIS&date=2026-08-04     # base-table partition
GET /records?party=ACME%20LLC                   # GSI grantor+grantee lookup, merged
```

## Measured results (LocalStack, 2026-08-05)

| Metric | Value |
|---|---|
| Records/day | 1,000 (2 counties × 500) |
| Backfill | 30 days · 30,000 records · 63 rec/s |
| Zero-dropped | S3 30,000 == DynamoDB 30,000 ✅ |
| Idempotency | replay 1 day → item count unchanged (0 new writes) ✅ |
| Parser p95 | 24.2 ms |
| Query p95 | 37.4 ms |
| DLQ | 25/25 malformed quarantined; deployed-stack redrive verified |
| Cost/mo | `___` (BLOCKED — LocalStack free/local) |

Full detail + exact reproduce steps in [`RESULTS.md`](RESULTS.md); raw numbers in
`results/*.json`; resume bullets in [`BULLETS.md`](BULLETS.md).

## Limitations
- **LocalStack (emulated AWS), not real AWS.** No real cloud deployment exists; do
  not read these numbers as production cloud metrics.
- **Synthetic data** — deterministic, generator-produced; not real county filings.
- **Cost not measured** — LocalStack emits no billing data; needs a real AWS deploy.
- **CloudWatch alarms not built** — LocalStack community doesn't meaningfully
  emulate alarm state, so DLQ-depth/error-rate alarms and the
  "recovery-to-N-minutes" metric are unmeasured (`___`). DLQ **redrive** is proven;
  runbook in `docs/RUNBOOK.md`.
- **Scale via harness** — the 30,000-record run uses the same handler code against
  the LocalStack data-plane; docker-Lambda per-record execution is only exercised
  at small scale in the E2E smoke.
