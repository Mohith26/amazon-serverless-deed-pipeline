# DeedStream — Measured Results

**Date measured:** 2026-08-05
**Target:** LocalStack community edition **3.8.1** (emulated AWS, running in Docker) — **NOT real AWS**.
**Data:** 100% **synthetic**, seeded, reproducible (see `src/deedstream/data_gen.py`). No real county records were scraped or used.

All numbers below come from real runs against LocalStack-hosted S3, SQS and
DynamoDB. Raw machine-readable values are committed under `results/*.json`.
Anything not measured is left as a literal `___`.

---

## How the numbers were produced

Two complementary paths, both real, both on LocalStack:

1. **Measurement harness** (`scripts/run_pipeline.py`) — drives the real
   fetcher/parser/query **code** against LocalStack S3 + SQS + DynamoDB at scale.
   The fetcher stages each record to S3 (bronze) **and** SQS; the parser drains
   the real SQS queue and does an idempotent conditional `PutItem`. This is where
   volume, reconciliation, idempotency and latency percentiles come from.
2. **Deployed-stack E2E smoke** (`scripts/smoke_e2e.py`) — runs against the
   **SAM stack deployed to LocalStack** so the actual Lambda/EventBridge/SQS-ESM/
   API-Gateway wiring executes: deployed Fetcher Lambda → S3 + SQS → deployed
   Parser Lambda (SQS event-source mapping) → DynamoDB → deployed Query Lambda via
   API Gateway; a malformed message redrives to the DLQ.

### Reproduce (exact commands)

```bash
# 0. one-time setup
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt awscli aws-sam-cli

# 1. start free LocalStack community (no account needed on 3.x)
docker run -d --name deedstream-localstack -p 4566:4566 \
  -e SERVICES=s3,sqs,dynamodb,lambda,events,apigateway,cloudformation,logs,iam,sts,cloudwatch \
  -v /var/run/docker.sock:/var/run/docker.sock localstack/localstack:3.8.1
curl -s localhost:4566/_localstack/health   # wait until "services" present

export AWS_ENDPOINT_URL=http://localhost:4566 AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test

# 2. unit + integration tests
pytest -q

# 3. measured 30-day backfill (writes results/*.json)  ~8 min
python scripts/run_pipeline.py --days 30 --count-per-county 500 --query-iters 500

# 4. deployed-stack IaC path + E2E smoke
bash scripts/deploy_local.sh          # samlocal build && samlocal deploy
python scripts/smoke_e2e.py

# 5. cost report (blocked on LocalStack — see below)
python scripts/cost_report.py
```

---

## Measured numbers (LocalStack, 2026-08-05)

| Metric | Value | Source |
|---|---|---|
| Records / day | **1,000** (500 × 2 counties: HARRIS, DALLAS) | `results/backfill.json` |
| Backfill span | **30 days** (2026-07-06 → 2026-08-04) | `results/backfill.json` |
| Total records backfilled | **30,000** | `results/backfill.json` |
| Ingest throughput | **63.4 records/s** (30,000 in 472.9 s) | `results/backfill.json` |
| Zero-dropped reconciliation | **S3 30,000 == DynamoDB 30,000 == expected 30,000 → TRUE** | `results/reconciliation.json` |
| Idempotency (replay 1 day) | before **30,000** → after **30,000**, **unchanged**; 1,000 duplicates detected, **0** new writes | `results/idempotency.json` |
| Parser p95 | **24.2 ms** (p50 8.9 / p99 47.0 / mean 11.4; n=31,000) | `results/parser_latency.json` |
| Query p95 | **37.4 ms** (p50 23.1 / p99 49.8; n=500, GSI party lookups) | `results/query_latency.json` |
| DLQ quarantine (harness) | 25 malformed injected → **25 in DLQ** (all quarantined) | `results/dlq.json` |
| **Cost / month** | **`___` — UNMEASURED / BLOCKED** | `results/cost.json` |

### Deployed-stack E2E smoke (SAM stack on LocalStack) — `results/e2e_smoke.json`
- Deployed **Fetcher Lambda** invoked → staged **40** records to S3 + SQS. ✅
- **SQS event-source mapping** triggered the deployed **Parser Lambda** → **40** items in DynamoDB. ✅
- **API Gateway** `GET /records?county=HARRIS&date=2026-08-04` → returned **40**. ✅
- Malformed message → **DLQ depth 1** via the deployed queue's redrive policy (~2 min: 3 receives × 60 s VisibilityTimeout). ✅

### Parser p95 — what it measures
Per-record parser work = `normalize()` + conditional `PutItem` against LocalStack
DynamoDB, timed client-side over the full 30-day backfill **plus** the idempotency
replay (n=31,000). The deployed Parser Lambda runs the identical code (proven in
the E2E smoke); CloudWatch `REPORT` Duration was not extracted at scale.

### Query p95 — what it measures
The `query` handler's GSI path (grantor-index + grantee-index, merged/deduped)
against LocalStack DynamoDB, 500 party lookups. The full API-Gateway HTTP path is
proven functionally in the E2E smoke (count=40) but was not load-tested for p95.

---

## Cost — UNMEASURED / BLOCKED

`cost_per_month_usd = ___`. LocalStack is free and local and emits **no billing
data**, so a real monthly cost cannot be measured here. It requires deploying to a
real AWS account and reading Cost Explorer for tag `Project=deedstream`
(see `scripts/cost_report.py`). We do **not** invent a number.

---

## Honest limitations / gaps

- **Runs on LocalStack (emulated AWS), not real AWS.** No real cloud deploy exists.
- **Synthetic data**, generated deterministically. Not real county filings.
- **Cost is not measured** (see above).
- **CloudWatch alarms not built.** LocalStack community does not meaningfully
  emulate CloudWatch alarm state transitions, so DLQ-depth / error-rate **alarms**
  and the "simulated-failure recovery → ___ min" metric are **not measured**. The
  underlying signal (DLQ redrive) *is* proven, and the redrive runbook is in
  `docs/RUNBOOK.md`.
- **LocalStack S3 flat-list quirk:** `list_objects_v2` stops advancing past ~1,000
  keys on this build even though every object exists (verified via `head_object`).
  The reconciliation counter walks county/date prefixes (each < 1,000 objects) to
  count reliably — see `count_s3_objects` in `scripts/run_pipeline.py`.
- **Deployed-stack scale:** the deployed-Lambda E2E is a small functional proof
  (40 records); the 30,000-record scale run uses the harness (same handler code,
  same LocalStack data-plane) because docker-Lambda execution per record is too
  slow for tens of thousands of invocations locally.
