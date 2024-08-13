"""Tests subquery module."""

from contextlib import nullcontext as does_not_raise
from unittest.mock import patch

import boto3
import pytest
from botocore.stub import Stubber
from freezegun import freeze_time

from tap_cloudwatch.subquery import Subquery


@freeze_time("2022-12-30")
def test_subquery():
    """Run subquery test."""
    client = boto3.client("logs", region_name="us-east-1")
    stubber = Stubber(client)
    query_start = 1672272000
    query_end = 1672275600
    log_group = "my_log_group_name"
    in_query = "fields @timestamp, @message"

    response = {
        "status": "Complete",
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
        {"status": "Running"},
        {"queryId": "123"},
    )
    stubber.add_response(
        "get_query_results",
        response,
        {"queryId": "123"},
    )
    stubber.activate()

    query_obj = Subquery(client, query_start, query_end, log_group, in_query)
    query_obj.execute()
    output = query_obj.get_results()

    assert response["results"] == output


@pytest.mark.parametrize(
    "first_status,second_status,expectation",
    [
        ["Failed", "Complete", does_not_raise()],
        ["Cancelled", "Complete", does_not_raise()],
        ["Timeout", "Complete", does_not_raise()],
        ["Failed", "Failed", pytest.raises(Exception)],
        ["Cancelled", "Cancelled", pytest.raises(Exception)],
        ["Timeout", "Timeout", pytest.raises(Exception)],
    ],
)
@freeze_time("2022-12-30")
def test_subquery_retry(first_status, second_status, expectation):
    """Run subquery test."""
    client = boto3.client("logs", region_name="us-east-1")
    stubber = Stubber(client)
    query_start = 1672272000
    query_end = 1672275600
    log_group = "my_log_group_name"
    in_query = "fields @timestamp, @message"

    response = {
        "status": second_status,
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
        "get_query_results",
        {
            "status": first_status,
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "statistics": {"recordsMatched": 0.0},
            "results": [],
        },
        {"queryId": "123"},
    )
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

    query_obj = Subquery(client, query_start, query_end, log_group, in_query)
    query_obj.query_id = "123"
    with expectation:
        output = query_obj.get_results()
        assert response["results"] == output


@patch.object(Subquery, "_handle_limit_exceeded", return_value=["foo"])
def test_subquery_limit_exceeded(patch_limit):
    """Run subquery test."""
    client = boto3.client("logs", region_name="us-east-1")
    stubber = Stubber(client)
    query_start = 1672272000
    query_end = 1672275600
    log_group = "my_log_group_name"
    in_query = "fields @timestamp, @message"

    response = {
        "status": "Complete",
        "results": [
            [
                {"field": "@timestamp", "value": "2022-01-01"},
                {"field": "@message", "value": "abc"},
            ]
        ],
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "statistics": {"recordsMatched": 10001},
    }
    stubber.add_response(
        "get_query_results",
        response,
        {"queryId": "123"},
    )
    stubber.activate()

    query_obj = Subquery(client, query_start, query_end, log_group, in_query)
    query_obj.query_id = "123"
    output = query_obj.get_results()

    patch_limit.assert_called_with(response)
    results = response["results"]
    results += ["foo"]
    assert results == output


@patch.object(Subquery, "execute")
@patch.object(Subquery, "get_results")
def test_handle_limit_exceeded(patch_result, execute):
    """Run subquery test."""
    response = {
        "status": "Complete",
        "results": [
            [
                {"field": "@timestamp", "value": "2022-01-01"},
                {"field": "@message", "value": "abc"},
            ],
            [
                {"field": "@timestamp", "value": "2023-01-01"},
                {"field": "@message", "value": "def"},
            ],
        ],
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "statistics": {"recordsMatched": 10001},
    }

    query_obj = Subquery("", "", "", "", "")
    query_obj.query_id = "123"
    query_obj._handle_limit_exceeded(response)

    patch_result.assert_called()
    execute.assert_called()

    assert query_obj.start_ts == 1672531200
