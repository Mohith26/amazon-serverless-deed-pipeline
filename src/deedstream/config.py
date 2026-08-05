"""Shared configuration and boto3 client factory.

Resource names come from environment variables (set by the SAM template on
deployed Lambdas, or by the local harness). ``AWS_ENDPOINT_URL`` selects the
target: LocalStack injects it into deployed Lambdas, and the local harness /
integration tests export ``http://localhost:4566``. When it is unset (unit
tests under moto, or a real-AWS deploy) boto3 uses its default endpoints.
"""

from __future__ import annotations

import os

import boto3

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-east-1"

TABLE_NAME = os.environ.get("TABLE_NAME", "deedstream-records")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "deedstream-bronze")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "deedstream-records")
DLQ_NAME = os.environ.get("DLQ_NAME", "deedstream-records-dlq")

GSI_GRANTOR = "grantor-index"
GSI_GRANTEE = "grantee-index"

# Default number of synthetic records the fetcher stages per county per day.
DEFAULT_RECORDS_PER_DAY = int(os.environ.get("RECORDS_PER_DAY", "500"))


def endpoint_url() -> str | None:
    """Return the AWS endpoint override, or None to use real AWS defaults."""
    return os.environ.get("AWS_ENDPOINT_URL") or None


def client(service: str):
    return boto3.client(service, region_name=AWS_REGION, endpoint_url=endpoint_url())


def resource(service: str):
    return boto3.resource(service, region_name=AWS_REGION, endpoint_url=endpoint_url())


def norm_party(name: str | None) -> str:
    """Normalize a party name for the GSI key (case/whitespace insensitive)."""
    return (name or "").strip().upper()


def bronze_key(county: str, filing_date: str, doc_id: str) -> str:
    """S3 key for a raw (bronze) record payload — one object per record."""
    return f"bronze/county={county}/date={filing_date}/{doc_id}.json"
