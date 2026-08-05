import json

import pytest
from moto import mock_aws

from deedstream import parser as ds_parser
from deedstream import query as ds_query
from tests.util import create_records_table

REC = {
    "county": "HARRIS",
    "filing_date": "2026-08-01",
    "doc_id": "HAR-20260801-00001",
    "doc_type": "WARRANTY DEED",
    "grantor": "JANE SMITH",
    "grantee": "ACME LLC",
    "legal_desc": "LOT 1",
}


def _seed(table):
    ds_parser.process_record(table, REC)
    ds_parser.process_record(
        table, {**REC, "doc_id": "HAR-20260801-00002", "grantor": "ACME LLC", "grantee": "BOB JONES"}
    )


@mock_aws
def test_query_by_county_and_date():
    table = create_records_table()
    _seed(table)
    results = ds_query.query_records(county="harris", date="2026-08-01", table=table)
    assert len(results) == 2


@mock_aws
def test_query_by_party_matches_grantor_and_grantee():
    table = create_records_table()
    _seed(table)
    # ACME LLC is grantee on doc 1 and grantor on doc 2 -> both, deduped.
    results = ds_query.query_records(party="acme llc", table=table)
    doc_ids = sorted(r["doc_id"] for r in results)
    assert doc_ids == ["HAR-20260801-00001", "HAR-20260801-00002"]


@mock_aws
def test_query_requires_a_filter():
    table = create_records_table()
    with pytest.raises(ValueError):
        ds_query.query_records(table=table)


@mock_aws
def test_lambda_handler_returns_400_without_filters():
    create_records_table()
    resp = ds_query.lambda_handler({"queryStringParameters": {}}, None)
    assert resp["statusCode"] == 400
    assert "error" in json.loads(resp["body"])
