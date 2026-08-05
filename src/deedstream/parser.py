"""Parser Lambda: normalize an SQS record and idempotently store it.

Single-table DynamoDB design:
  PK = "<COUNTY>#<filing_date>", SK = doc_id
  GSI grantor-index (grantor_key, filing_date), GSI grantee-index (grantee_key, filing_date)

Writes use a conditional put so replaying a day produces zero duplicates.
Malformed records raise, and the SQS -> DLQ redrive policy quarantines them.
"""

from __future__ import annotations

import json

import botocore.exceptions

from . import config

REQUIRED_FIELDS = ("county", "filing_date", "doc_id", "doc_type", "grantor", "grantee")


class MalformedRecord(ValueError):
    """Raised when a raw record is missing required fields."""


def normalize(raw: dict) -> dict:
    """Validate and normalize a raw record into a DynamoDB item.

    Raises MalformedRecord if any required field is missing or empty.
    """
    if not isinstance(raw, dict):
        raise MalformedRecord("record is not a JSON object")
    missing = [f for f in REQUIRED_FIELDS if not str(raw.get(f, "")).strip()]
    if missing:
        raise MalformedRecord(f"missing/empty required fields: {','.join(missing)}")

    county = str(raw["county"]).strip().upper()
    filing_date = str(raw["filing_date"]).strip()
    doc_id = str(raw["doc_id"]).strip()
    return {
        "PK": f"{county}#{filing_date}",
        "SK": doc_id,
        "county": county,
        "filing_date": filing_date,
        "doc_id": doc_id,
        "doc_type": str(raw["doc_type"]).strip().upper(),
        "grantor": str(raw["grantor"]).strip(),
        "grantee": str(raw["grantee"]).strip(),
        "grantor_key": config.norm_party(raw["grantor"]),
        "grantee_key": config.norm_party(raw["grantee"]),
        "legal_desc": str(raw.get("legal_desc", "")).strip(),
    }


def put_idempotent(table, item: dict) -> bool:
    """Conditional put. Returns True if newly written, False if it already existed."""
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
        )
        return True
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def process_record(table, raw: dict) -> bool:
    """Normalize + idempotently store one record. Returns True if newly written."""
    return put_idempotent(table, normalize(raw))


def lambda_handler(event, context):
    """SQS-triggered entrypoint using partial batch responses.

    Malformed / unparseable messages are reported as failures so SQS redrives
    them to the DLQ after maxReceiveCount; valid messages are stored idempotently.
    """
    table = config.resource("dynamodb").Table(config.TABLE_NAME)
    written = 0
    duplicates = 0
    failures: list[dict] = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            raw = json.loads(record.get("body", "{}"))
            if process_record(table, raw):
                written += 1
            else:
                duplicates += 1
        except (MalformedRecord, json.JSONDecodeError):
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures, "written": written, "duplicates": duplicates}
