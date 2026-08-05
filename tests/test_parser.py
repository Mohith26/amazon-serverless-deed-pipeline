import pytest
from moto import mock_aws

from deedstream import parser as ds_parser
from tests.util import create_records_table

VALID = {
    "county": "harris",
    "filing_date": "2026-08-01",
    "doc_id": "HAR-20260801-00001",
    "doc_type": "warranty deed",
    "grantor": "  Jane Smith ",
    "grantee": "Acme LLC",
    "legal_desc": "LOT 1 BLK 2",
}


def test_normalize_builds_keys_and_uppercases():
    item = ds_parser.normalize(VALID)
    assert item["PK"] == "HARRIS#2026-08-01"
    assert item["SK"] == "HAR-20260801-00001"
    assert item["county"] == "HARRIS"
    assert item["grantor_key"] == "JANE SMITH"
    assert item["grantee_key"] == "ACME LLC"


@pytest.mark.parametrize("missing", ["county", "doc_id", "grantor", "grantee"])
def test_normalize_rejects_missing_fields(missing):
    bad = {**VALID, missing: "  "}
    with pytest.raises(ds_parser.MalformedRecord):
        ds_parser.normalize(bad)


@mock_aws
def test_conditional_put_is_idempotent():
    table = create_records_table()
    assert ds_parser.process_record(table, VALID) is True
    assert ds_parser.process_record(table, VALID) is False  # duplicate -> no-op
    assert table.scan(Select="COUNT")["Count"] == 1


@mock_aws
def test_lambda_handler_reports_malformed_as_batch_failure():
    create_records_table()
    event = {
        "Records": [
            {"messageId": "1", "body": '{"county":"HARRIS"}'},  # malformed
            {"messageId": "2", "body": __import__("json").dumps(VALID)},
        ]
    }
    result = ds_parser.lambda_handler(event, None)
    assert result["written"] == 1
    assert result["batchItemFailures"] == [{"itemIdentifier": "1"}]
