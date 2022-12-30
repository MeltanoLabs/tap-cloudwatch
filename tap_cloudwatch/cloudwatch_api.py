"""Class for interacting with Cloudwatch API."""

import os
import time
from datetime import datetime

import boto3


class CloudwatchAPI:
    """Cloudwatch class for interacting with the API."""

    def __init__(self, logger):
        """Initialize CloudwatchAPI."""
        self._client = None
        self.logger = logger

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

    def get_records_iterator(self, bookmark, log_group, query, increment_mins):
        """Retrieve records from Cloudwatch."""
        limit = 10000
        end_time = datetime.utcnow().timestamp()
        start_time = bookmark.timestamp()
        diff_s = end_time - start_time
        diff_mins = diff_s / 60.0
        batches = diff_mins / increment_mins
        count = 0
        while count < batches:
            if count != 0:
                # inclusive start and end date, so on second iteration we can skip the first second
                query_start = int(start_time + (increment_mins * 60 * count) + 1)
            else:
                query_start = int(start_time + (increment_mins * 60 * count))
            query_end = int(start_time + (increment_mins * 60 * (count + 1)))
            self.logger.info(
                f"Retrieving batch from: `{datetime.fromtimestamp(query_start).isoformat()}` - `{datetime.fromtimestamp(query_end).isoformat()}`"
            )
            start_query_response = self.client.start_query(
                logGroupName=log_group,
                startTime=query_start,
                endTime=query_end,
                queryString=query,
                limit=limit,
            )

            query_id = start_query_response["queryId"]
            response = None
            while response is None or response["status"] == "Running":
                time.sleep(1)
                response = self.client.get_query_results(queryId=query_id)
            if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                raise Exception(f"Failed: {response}")
            result_size = response.get("statistics", {}).get("recordsMatched")
            if result_size == limit:
                raise Exception(
                    f"The result size is the same as limit ({limit}), theres a risk of missing data. \
                    Try reducing the increment config and re-run."
                )
            yield response
            count += 1
