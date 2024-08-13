"""Class for interacting with Cloudwatch API."""

from __future__ import annotations

import os
from collections import deque
from datetime import datetime, timedelta, timezone
from math import ceil

import boto3

from tap_cloudwatch.exception import InvalidQueryException
from tap_cloudwatch.subquery import Subquery


class CloudwatchAPI:
    """Cloudwatch class for interacting with the API."""

    def __init__(self, logger):
        """Initialize CloudwatchAPI."""
        self._client = None
        self.logger = logger
        self.max_concurrent_queries = 20

    @property
    def client(self):
        """Property to access client object."""
        if not self._client:
            raise Exception("Client not yet initialized")
        return self._client

    def authenticate(self, config):
        """Authenticate the AWS client."""
        self._client = self._create_client(config)

    def _create_client(self, config):
        aws_access_key_id = config.get("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        aws_secret_access_key = config.get("aws_secret_access_key") or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_session_token = config.get("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )
        aws_profile = config.get("aws_profile") or os.environ.get("AWS_PROFILE")
        aws_endpoint_url = config.get("aws_endpoint_url")
        aws_region_name = config.get("aws_region_name")

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region_name,
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)
        if aws_endpoint_url:
            logs = aws_session.client("logs", endpoint_url=aws_endpoint_url)
        else:
            logs = aws_session.client("logs")
        return logs

    def _split_batch_into_windows(self, start_time, end_time, batch_increment_s):
        start_time_epoch = start_time.timestamp()
        end_time_epoch = end_time.timestamp()
        diff_s = end_time_epoch - start_time_epoch
        total_batches = ceil(diff_s / batch_increment_s)
        batch_windows = []
        for batch_num in range(total_batches):
            if batch_num != 0:
                # Inclusive start and end date, so on second iteration
                # we can skip one second.
                query_start = int(
                    start_time_epoch + (batch_increment_s * batch_num) + 1
                )
            else:
                query_start = int(start_time_epoch + (batch_increment_s * batch_num))
            # Never exceed the end_time
            query_end = min(
                int(start_time_epoch + (batch_increment_s * (batch_num + 1))),
                int(end_time_epoch),
            )
            batch_windows.append((query_start, query_end))
        return batch_windows

    def _validate_query(self, query):
        if "|sort" in query.replace(" ", ""):
            raise InvalidQueryException("sort not allowed")
        if "|limit" in query.replace(" ", ""):
            raise InvalidQueryException("limit not allowed")
        if "stats" in query:
            raise InvalidQueryException("stats not allowed")
        if "@timestamp" not in query.split("|")[0]:
            raise InvalidQueryException(
                "@timestamp field is used as the replication key so it must be selected"
            )

    def _queue_is_full(self, queue):
        return len(queue) >= self.max_concurrent_queries

    @staticmethod
    def _get_completed_query(queue):
        return queue.popleft()

    def _iterate_batches(self, batch_windows, log_group, query):
        queue: deque[Subquery] = deque()

        for start_ts, end_ts in batch_windows:
            if self._queue_is_full(queue):
                query_obj = self._get_completed_query(queue)
                queue.append(
                    Subquery(self.client, start_ts, end_ts, log_group, query).execute()
                )
                yield query_obj.get_results()
            else:
                queue.append(
                    Subquery(self.client, start_ts, end_ts, log_group, query).execute()
                )

        # Clear queue to complete
        while len(queue) > 0:
            query_obj = self._get_completed_query(queue)
            yield query_obj.get_results()

    def _alter_end_ts(self, end_time):
        default_end_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        if end_time:
            return min([end_time, default_end_time])
        else:
            return default_end_time

    def get_records_iterator(
        self, bookmark, log_group, query, batch_increment_s, end_time
    ):
        """Retrieve records from Cloudwatch."""
        self._validate_query(query)
        batch_windows = self._split_batch_into_windows(
            bookmark, self._alter_end_ts(end_time), batch_increment_s
        )

        yield from self._iterate_batches(batch_windows, log_group, query)
