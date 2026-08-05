"""End-to-end smoke test against the SAM stack DEPLOYED to LocalStack.

Proves the real serverless wiring: invoke the deployed Fetcher Lambda -> S3 +
SQS -> the SQS event-source mapping triggers the deployed Parser Lambda ->
DynamoDB; query through the deployed API Gateway; a malformed message redrives
to the DLQ. Writes results/e2e_smoke.json.

Run after scripts/deploy_local.sh. Requires AWS_ENDPOINT_URL=http://localhost:4566.
"""

from __future__ import annotations

import json
import os
import sys
import time

import boto3
import urllib.request

ENDPOINT = "http://localhost:4566"
STACK = "deedstream"
DAY = "2026-08-04"
COUNT = 40


def _c(svc):
    return boto3.client(svc, endpoint_url=ENDPOINT, region_name="us-east-1")


def stack_outputs(cfn):
    outs = cfn.describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]
    return {o["OutputKey"]: o["OutputValue"] for o in outs}


def function_name(lam, needle):
    for fn in lam.list_functions()["Functions"]:
        if needle in fn["FunctionName"]:
            return fn["FunctionName"]
    raise RuntimeError(f"no deployed function matching {needle!r}")


def api_id_from_outputs(outs):
    # ApiBaseUrl: https://<id>.execute-api.amazonaws.com:4566/Prod
    return outs["ApiBaseUrl"].split("://", 1)[1].split(".", 1)[0]


def wait_for_count(ddb, table, target, timeout=90):
    deadline = time.time() + timeout
    count = 0
    while time.time() < deadline:
        count = ddb.scan(TableName=table, Select="COUNT")["Count"]
        if count >= target:
            return count
        time.sleep(2)
    return count


def main():
    cfn = _c("cloudformation")
    lam = _c("lambda")
    ddb = _c("dynamodb")
    sqs = _c("sqs")

    outs = stack_outputs(cfn)
    table = outs["RecordsTableName"]
    queue_url = outs["RecordsQueueUrl"]
    dlq_url = outs["DLQUrl"]
    fetcher = function_name(lam, "FetcherFunction")

    result = {"target": "SAM stack deployed to LocalStack (emulated AWS)"}

    # 1) Invoke the deployed fetcher -> S3 + SQS
    payload = json.dumps({"date": DAY, "count": COUNT, "counties": ["HARRIS"]}).encode()
    inv = lam.invoke(FunctionName=fetcher, Payload=payload)
    body = json.loads(inv["Payload"].read())
    result["fetcher_invoke"] = {"status": inv["StatusCode"], "response": body}

    # 2) SQS ESM triggers the deployed parser -> DynamoDB
    stored = wait_for_count(ddb, table, COUNT)
    result["dynamodb_items_after_pipeline"] = stored
    result["parser_pipeline_ok"] = stored >= COUNT

    # 3) Query through the deployed API Gateway
    api = api_id_from_outputs(outs)
    url = f"{ENDPOINT}/restapis/{api}/Prod/_user_request_/records?county=HARRIS&date={DAY}"
    with urllib.request.urlopen(url, timeout=15) as r:
        api_body = json.loads(r.read().decode())
    result["api_gateway_query"] = {"url_shape": "/records?county=&date=", "count": api_body.get("count")}
    result["api_gateway_ok"] = api_body.get("count", 0) >= COUNT

    # 4) Malformed message -> DLQ (real SQS redrive on the deployed queue)
    sqs.send_message(QueueUrl=queue_url, MessageBody='{"county":"HARRIS"}')  # missing fields
    # Redrive takes ~maxReceiveCount * VisibilityTimeout (3 * 60s) on the deployed queue.
    deadline = time.time() + 240
    dlq_depth = 0
    while time.time() < deadline:
        dlq_depth = int(
            sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"])[
                "Attributes"
            ]["ApproximateNumberOfMessages"]
        )
        if dlq_depth >= 1:
            break
        time.sleep(3)
    result["dlq_depth_after_malformed"] = dlq_depth
    result["dlq_ok"] = dlq_depth >= 1

    result["all_ok"] = all(
        result[k] for k in ("parser_pipeline_ok", "api_gateway_ok", "dlq_ok")
    )

    os.makedirs("results", exist_ok=True)
    with open("results/e2e_smoke.json", "w") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["all_ok"] else 1)


if __name__ == "__main__":
    main()
