# Runbook — DLQ Redrive

The records queue (`deedstream-records`) has a redrive policy
(`maxReceiveCount=3`) to the DLQ (`deedstream-records-dlq`). A message lands in
the DLQ when the Parser Lambda reports it as a batch-item failure 3 times —
i.e. it is malformed (missing required fields or invalid JSON).

> Commands below target LocalStack via `awslocal`. On real AWS use `aws` with the
> real queue URLs. On LocalStack community, CloudWatch **alarms** on DLQ depth are
> not emulated; poll depth manually (below) or wire an EventBridge rule.

## 1. Detect

```bash
awslocal sqs get-queue-attributes \
  --queue-url "$(awslocal sqs get-queue-url --queue-name deedstream-records-dlq --query QueueUrl --output text)" \
  --attribute-names ApproximateNumberOfMessages
```

## 2. Inspect a sample

```bash
DLQ=$(awslocal sqs get-queue-url --queue-name deedstream-records-dlq --query QueueUrl --output text)
awslocal sqs receive-message --queue-url "$DLQ" --max-number-of-messages 5 --visibility-timeout 0
```

Look at the message body: which required field is missing / is it valid JSON?

## 3. Fix root cause
- Malformed upstream payload → fix the fetcher/source mapping.
- Transient parser bug → deploy the fix (`bash scripts/deploy_local.sh`).

## 4. Redrive (DLQ → main queue) after the fix

```bash
MAIN=$(awslocal sqs get-queue-url --queue-name deedstream-records --query QueueUrl --output text)
MAIN_ARN=$(awslocal sqs get-queue-attributes --queue-url "$MAIN" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs start-message-move-task --source-arn \
  "$(awslocal sqs get-queue-attributes --queue-url "$DLQ" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)" \
  --destination-arn "$MAIN_ARN"
```

The parser reprocesses redriven messages **idempotently** (conditional
`PutItem`), so any records that already made it to DynamoDB are not duplicated.

## 5. Verify empty + reconciled

```bash
awslocal sqs get-queue-attributes --queue-url "$DLQ" --attribute-names ApproximateNumberOfMessages
python scripts/run_pipeline.py --days 1 --count-per-county 1   # or re-check S3 vs DynamoDB counts
```
