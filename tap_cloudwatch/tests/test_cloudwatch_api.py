"""Tests cloudwatch api module."""

from contextlib import nullcontext as does_not_raise

import pytest

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
