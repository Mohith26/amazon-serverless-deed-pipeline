from moto import mock_aws

from deedstream import config, fetcher
from tests.util import create_bucket_and_queue


@mock_aws
def test_fetch_and_stage_writes_s3_and_sqs_per_record():
    s3, sqs, queue_url = create_bucket_and_queue()

    staged = fetcher.fetch_and_stage("HARRIS", "2026-08-01", 12, s3=s3, sqs=sqs, queue_url=queue_url)
    assert staged == 12

    objects = s3.list_objects_v2(Bucket=config.BRONZE_BUCKET, Prefix="bronze/")
    assert objects["KeyCount"] == 12

    depth = int(
        sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]["ApproximateNumberOfMessages"]
    )
    assert depth == 12


@mock_aws
def test_lambda_handler_stages_all_counties():
    create_bucket_and_queue()
    result = fetcher.lambda_handler({"date": "2026-08-01", "count": 5}, None)
    assert result["staged"] == 5 * len(result["counties"])
