"""Tests cloudwatch api module."""

import logging
from contextlib import nullcontext as does_not_raise

import boto3
import pytest
from botocore.stub import Stubber
from freezegun import freeze_time

from tap_cloudwatch.cloudwatch_api import CloudwatchAPI
from tap_cloudwatch.exception import InvalidQueryException


@pytest.mark.parametrize(
    "start,end,batch,expected",
    [
        [1672272000, 1672275600, 3600, [(1672272000, 1672275600)]],
        [
            1672272000,
            1672275601,
            3600,
            [(1672272000, 1672275600), (1672275601, 1672279200)],
        ],
    ],
)
def test_split_batch_into_windows(start, end, batch, expected):
    """Run standard tap tests from the SDK."""
    api = CloudwatchAPI(None)
    batches = api._split_batch_into_windows(start, end, batch)
    assert batches == expected


@pytest.mark.parametrize(
    "query,expectation",
    [
        [
            "fields @timestamp, @message | sort @timestamp desc",
            pytest.raises(InvalidQueryException),
        ],
        ["fields @timestamp, @message | limit 5", pytest.raises(InvalidQueryException)],
        ["stats count(*) by duration as time", pytest.raises(InvalidQueryException)],
        ["fields @message", pytest.raises(InvalidQueryException)],
        ["fields @timestamp, @message", does_not_raise()],
    ],
)
def test_validate_query(query, expectation):
    """Run standard tap tests from the SDK."""
    api = CloudwatchAPI(None)
    with expectation:
        api._validate_query(query)


@freeze_time("2022-12-30")
def test_handle_batch_window():
    """Run standard tap tests from the SDK."""
    client = boto3.client("logs", region_name="us-east-1")
    stubber = Stubber(client)

    api = CloudwatchAPI(logging.getLogger())
    api._client = client
    query_start = 1672272000
    query_end = 1672275600
    log_group = "my_log_group_name"
    in_query = "fields @timestamp, @message"

    response = {
        "status": "abc",
        "results": [
            [
                {"field": "@timestamp", "value": "2022-01-01"},
                {"field": "@message", "value": "abc"},
            ]
        ],
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "statistics": {"recordsMatched": 10000},
    }
    stubber.add_response(
        "start_query",
        {"queryId": "123"},
        {
            "endTime": query_end,
            "limit": 10000,
            "logGroupName": log_group,
            "queryString": in_query + " | sort @timestamp asc",
            "startTime": query_start,
        },
    )
    stubber.add_response(
        "get_query_results",
        response,
        {"queryId": "123"},
    )
    stubber.activate()

    query_id = api._start_query(query_start, query_end, log_group, in_query)
    output = api._get_results(log_group, query_start, query_end, in_query, query_id)

    assert response["results"] == output
