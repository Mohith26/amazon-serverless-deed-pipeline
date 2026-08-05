"""Shared test helpers for building resources inside a moto mock."""

import boto3

from deedstream import config

REGION = "us-east-1"


def create_records_table():
    ddb = boto3.client("dynamodb", region_name=REGION)
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
    )
    return boto3.resource("dynamodb", region_name=REGION).Table(config.TABLE_NAME)


def create_bucket_and_queue():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=config.BRONZE_BUCKET)
    sqs = boto3.client("sqs", region_name=REGION)
    queue_url = sqs.create_queue(QueueName=config.QUEUE_NAME)["QueueUrl"]
    return s3, sqs, queue_url
