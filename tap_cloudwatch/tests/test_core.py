"""Tests standard tap features using the built-in SDK tests library."""

from unittest.mock import patch

import boto3
from botocore.stub import Stubber
from freezegun import freeze_time
from singer_sdk.testing import get_standard_tap_tests

from tap_cloudwatch.cloudwatch_api import CloudwatchAPI
from tap_cloudwatch.tap import TapCloudWatch

SAMPLE_CONFIG = {
    "log_group_name": "my_log_group_name",
    "query": "fields @timestamp, @message",
    "aws_region_name": "us-east-1",
    "start_date": "2022-12-29",
    "batch_increment_s": 86400
}

client = boto3.client("logs", region_name="us-east-1")
stubber = Stubber(client)


# Run standard built-in tap tests from the SDK:
@freeze_time("2022-12-30")
@patch.object(CloudwatchAPI, "_create_client", return_value=client)
def test_standard_tap_tests(patch_client):
    """Run standard tap tests from the SDK."""
    stubber.add_response(
        "start_query",
        {"queryId": "123"},
        {
            "endTime": 1672358400,
            "limit": 10000,
            "logGroupName": "my_log_group_name",
            "queryString": "fields @timestamp, @message | sort @timestamp asc",
            "startTime": 1672272000,
        },
    )
    stubber.add_response(
        "get_query_results",
        {
            "status": "abc",
            "results": [
                [
                    {"field": "@timestamp", "value": "2022-01-01"},
                    {"field": "@message", "value": "abc"},
                ]
            ],
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "statistics": {"recordsMatched": 0}
        },
        {"queryId": "123"},
    )
    stubber.activate()

    tests = get_standard_tap_tests(TapCloudWatch, config=SAMPLE_CONFIG)
    for test in tests:
        test()
