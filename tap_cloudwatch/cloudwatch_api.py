"""Class for interacting with Cloudwatch API."""

import os
import time
from datetime import datetime, timezone
import pytz

from tap_cloudwatch.exception import InvalidQueryException

import boto3
from math import ceil

class CloudwatchAPI:
    """Cloudwatch class for interacting with the API."""

    def __init__(self, logger):
        """Initialize CloudwatchAPI."""
        self._client = None
        self.logger = logger
        self.limit = 10000

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

    @staticmethod
    def _request_more_records():
        return True

    def split_batch_into_windows(self, start_time, end_time, batch_increment_s):
        diff_s = end_time - start_time
        total_batches = ceil(diff_s / batch_increment_s)
        batch_windows = []
        for batch_num in range(total_batches):
            if batch_num != 0:
                # Inclusive start and end date, so on second iteration
                # we can skip one second.
                query_start = int(start_time + (batch_increment_s * batch_num) + 1)
            else:
                query_start = int(start_time + (batch_increment_s * batch_num))
            query_end = int(start_time + (batch_increment_s * (batch_num + 1)))
            batch_windows.append((query_start, query_end))
        return batch_windows

    def validate_query(self, query):
        if "|sort" in query.replace(" ", ""):
            raise InvalidQueryException("sort not allowed")
        if "|limit" in query.replace(" ", ""):
            raise InvalidQueryException("limit not allowed")
        if "stats" in query:
            raise InvalidQueryException("stats not allowed")
        if "@timestamp" not in query.split("|")[0]:
            raise InvalidQueryException("@timestamp field is used as the replication key so it must be selected")

    def get_records_iterator(self, bookmark, log_group, query, batch_increment_s):
        """Retrieve records from Cloudwatch."""
        end_time = datetime.now(timezone.utc).timestamp()
        start_time = bookmark.timestamp()
        self.validate_query(query)
        batch_windows = self.split_batch_into_windows(start_time, end_time, batch_increment_s)

        for window in batch_windows:
            # TODO: start 20, keep track of order and query_ids, get query results one at a time
            query_id = self.start_query(window[0], window[1], log_group, query)
            results = self.get_results(log_group, window[0], window[1], query, query_id)
            yield results

    def handle_limit_exceeded(self, response, log_group, query_start, query_end, query):
        results = response.get("results")
        last_record = results[-1]

        latest_ts_str = [i["value"] for i in last_record if i["field"] == "@timestamp"][0]
        # Include latest ts in query, this could cause duplicates but
        # without it we might miss ties
        new_query_start = int(datetime.fromisoformat(latest_ts_str).replace(tzinfo=pytz.UTC).timestamp())
        new_query_id = self.start_query(new_query_start, query_end, log_group, query)
        return self.get_results(log_group, new_query_start, query_end, query, new_query_id)

    def alter_query(self, query):
        query += " | sort @timestamp asc"
        return query

    def start_query(self, query_start, query_end, log_group, query, prev_start=None):
        self.logger.info(
            (
                "Retrieving batch from:"
                f" `{datetime.utcfromtimestamp(query_start).isoformat()} UTC` -"
                f" `{datetime.utcfromtimestamp(query_end).isoformat()} UTC`"
            )
        )
        query = self.alter_query(query)
        start_query_response = self.client.start_query(
            logGroupName=log_group,
            startTime=query_start,
            endTime=query_end,
            queryString=query,
            limit=self.limit,
        )
        return start_query_response["queryId"]

    def get_results(self, log_group, query_start, query_end, query, query_id, prev_start=None):
        response = None
        while response is None or response["status"] == "Running":
            time.sleep(1)
            response = self.client.get_query_results(queryId=query_id)
        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
            raise Exception(f"Failed: {response}")
        result_size = response.get("statistics", {}).get("recordsMatched")
        results = response['results']
        if result_size > self.limit:
            if prev_start == query_start:
                raise Exception("Stuck in a loop, smaller batch still exceeds limit. Reduce batch window.")
            self.logger.info(
                f"Result set size '{int(result_size)}' exceeded limit '{self.limit}'. Re-running sub-batch..."
            )
            results += self.handle_limit_exceeded(response, log_group, query_start, query_end, query)
        return results
