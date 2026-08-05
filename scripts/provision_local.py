"""Provision DeedStream data-plane resources on LocalStack via boto3.

Creates the S3 bronze bucket, the records queue + DLQ (with redrive), and the
single-table DynamoDB table with both GSIs. Idempotent: safe to re-run. Used by
the measurement harness and the LocalStack integration test. The SAM template
(template.yaml) provisions the SAME resources plus the Lambda/EventBridge/API
wiring; this script is the reliable data-plane path for measurement.
"""

from __future__ import annotations

import os
import sys

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from deedstream import config  # noqa: E402

VISIBILITY_TIMEOUT = os.environ.get("QUEUE_VISIBILITY_TIMEOUT", "2")
MAX_RECEIVE_COUNT = os.environ.get("QUEUE_MAX_RECEIVE_COUNT", "3")


def _clients():
    kw = {"region_name": config.AWS_REGION, "endpoint_url": config.endpoint_url()}
    return boto3.client("s3", **kw), boto3.client("sqs", **kw), boto3.client("dynamodb", **kw)


def provision():
    s3, sqs, ddb = _clients()

    # S3 bronze bucket
    try:
        s3.create_bucket(Bucket=config.BRONZE_BUCKET)
    except ClientError as err:
        if err.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise

    # DLQ then main queue with redrive policy
    dlq_url = sqs.create_queue(QueueName=config.DLQ_NAME)["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    redrive = (
        '{"deadLetterTargetArn":"%s","maxReceiveCount":"%s"}' % (dlq_arn, MAX_RECEIVE_COUNT)
    )
    queue_url = sqs.create_queue(
        QueueName=config.QUEUE_NAME,
        Attributes={"VisibilityTimeout": VISIBILITY_TIMEOUT, "RedrivePolicy": redrive},
    )["QueueUrl"]

    # DynamoDB single-table with two GSIs
    try:
        ddb.create_table(
            TableName=config.TABLE_NAME,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "grantor_key", "AttributeType": "S"},
                {"AttributeName": "grantee_key", "AttributeType": "S"},
                {"AttributeName": "filing_date", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": config.GSI_GRANTOR,
                    "KeySchema": [
                        {"AttributeName": "grantor_key", "KeyType": "HASH"},
                        {"AttributeName": "filing_date", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": config.GSI_GRANTEE,
                    "KeySchema": [
                        {"AttributeName": "grantee_key", "KeyType": "HASH"},
                        {"AttributeName": "filing_date", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            Tags=[{"Key": "Project", "Value": "deedstream"}],
        )
        ddb.get_waiter("table_exists").wait(TableName=config.TABLE_NAME)
    except ClientError as err:
        if err.response["Error"]["Code"] != "ResourceInUseException":
            raise

    return {"bucket": config.BRONZE_BUCKET, "queue_url": queue_url, "dlq_url": dlq_url,
            "table": config.TABLE_NAME}


if __name__ == "__main__":
    info = provision()
    print("provisioned:", info)
