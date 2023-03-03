"""Tests cloudwatch api module."""

from contextlib import nullcontext as does_not_raise

import pytest
from freezegun import freeze_time

from tap_cloudwatch.cloudwatch_api import CloudwatchAPI
from tap_cloudwatch.exception import InvalidQueryException
from tap_cloudwatch.tests.utils import datetime_from_str


@pytest.mark.parametrize(
    "start,end,batch,expected",
    [
        [
            datetime_from_str("2022-12-29 00:00:00"),
            datetime_from_str("2022-12-29 01:00:00"),
            3600,
            [
                (
                    datetime_from_str("2022-12-29 00:00:00").timestamp(),
                    datetime_from_str("2022-12-29 01:00:00").timestamp(),
                )
            ],
        ],
        [
            datetime_from_str("2022-12-29 00:00:00"),
            datetime_from_str("2022-12-29 01:00:01"),
            3600,
            [
                (
                    datetime_from_str("2022-12-29 00:00:00").timestamp(),
                    datetime_from_str("2022-12-29 01:00:00").timestamp(),
                ),
                (
                    datetime_from_str("2022-12-29 01:00:01").timestamp(),
                    datetime_from_str("2022-12-29 01:00:01").timestamp(),
                ),
            ],
        ],
        [
            datetime_from_str("2022-12-29 00:00:00"),
            datetime_from_str("2022-12-29 03:00:00"),
            3600,
            [
                (
                    datetime_from_str("2022-12-29 00:00:00").timestamp(),
                    datetime_from_str("2022-12-29 01:00:00").timestamp(),
                ),
                (
                    datetime_from_str("2022-12-29 01:00:01").timestamp(),
                    datetime_from_str("2022-12-29 02:00:00").timestamp(),
                ),
                (
                    datetime_from_str("2022-12-29 02:00:01").timestamp(),
                    datetime_from_str("2022-12-29 03:00:00").timestamp(),
                ),
            ],
        ],
    ],
)
def test_split_batch_into_windows(start, end, batch, expected):
    """Test _split_batch_into_windows."""
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


@pytest.mark.parametrize(
    "input_end_ts,expectation",
    [
        [None, datetime_from_str("2022-12-29 23:55:00")],
        [
            datetime_from_str("2022-12-29 00:00:00"),
            datetime_from_str("2022-12-29 00:00:00"),
        ],
        [
            datetime_from_str("2022-12-29 23:59:00"),
            datetime_from_str("2022-12-29 23:55:00"),
        ],
    ],
)
@freeze_time("2022-12-30")
def test_alter_end_ts(input_end_ts, expectation):
    api = CloudwatchAPI(None)
    assert api._alter_end_ts(input_end_ts) == expectation
