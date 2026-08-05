"""Query Lambda: GET /records?county=&date=&party= backed by DynamoDB.

- party given            -> query both GSIs (grantor + grantee), merge, dedupe.
- county+date given      -> query the base table partition directly.
County/date narrow a party query when supplied together.
"""

from __future__ import annotations

import json

from boto3.dynamodb.conditions import Key

from . import config

_LIMIT = 500


def query_records(*, county=None, date=None, party=None, table=None, limit=_LIMIT):
    """Return matching records. Raises ValueError if no usable filter is given."""
    table = table or config.resource("dynamodb").Table(config.TABLE_NAME)

    if party:
        party_key = config.norm_party(party)
        merged: dict[tuple, dict] = {}
        for index, key_attr in (
            (config.GSI_GRANTOR, "grantor_key"),
            (config.GSI_GRANTEE, "grantee_key"),
        ):
            resp = table.query(
                IndexName=index,
                KeyConditionExpression=Key(key_attr).eq(party_key),
                Limit=limit,
            )
            for item in resp.get("Items", []):
                merged[(item["PK"], item["SK"])] = item
        results = list(merged.values())
        if county:
            results = [r for r in results if r["county"] == county.strip().upper()]
        if date:
            results = [r for r in results if r["filing_date"] == str(date)]
        return results

    if county and date:
        pk = f"{county.strip().upper()}#{date}"
        resp = table.query(KeyConditionExpression=Key("PK").eq(pk), Limit=limit)
        return resp.get("Items", [])

    raise ValueError("provide 'party', or both 'county' and 'date'")


def lambda_handler(event, context):
    """API Gateway proxy entrypoint."""
    params = (event or {}).get("queryStringParameters") or {}
    headers = {"Content-Type": "application/json"}
    try:
        items = query_records(
            county=params.get("county"),
            date=params.get("date"),
            party=params.get("party"),
        )
    except ValueError as err:
        return {"statusCode": 400, "headers": headers, "body": json.dumps({"error": str(err)})}

    body = {"count": len(items), "records": items}
    return {"statusCode": 200, "headers": headers, "body": json.dumps(body, default=str)}
