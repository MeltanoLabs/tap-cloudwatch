"""Class for managing a Subquery."""

from __future__ import annotations

import logging
import time
from datetime import datetime

import pytz


class Subquery:
    """Subquery managing a Subquery."""

    def __init__(self, client, start_ts, end_ts, log_group, query):
        """Initialize Subquery."""
        self.logger = logging.getLogger(__name__)
        self.client = client
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.log_group = log_group
        self.query = self._alter_query(query)
        self.query_id = None
        self.limit = 10000

    def execute(self):
        """Run the query."""
        self.logger.info(
            "Submitting query for batch from:"
            f" `{datetime.utcfromtimestamp(self.start_ts).isoformat()} UTC` -"
            f" `{datetime.utcfromtimestamp(self.end_ts).isoformat()} UTC`"
        )
        start_query_response = self.client.start_query(
            logGroupName=self.log_group,
            startTime=self.start_ts,
            endTime=self.end_ts,
            queryString=self.query,
            limit=self.limit,
        )
        self.query_id = start_query_response["queryId"]
        return self

    def get_results(self, prev_start=None):
        """Get results from query and recurse if needed."""
        self.logger.info(
            "Retrieving results for batch from:"
            f" `{datetime.utcfromtimestamp(self.start_ts).isoformat()} UTC` -"
            f" `{datetime.utcfromtimestamp(self.end_ts).isoformat()} UTC`"
        )
        response = None
        retry = True
        first = True
        while response is None or response["status"] != "Complete":
            if not first:
                time.sleep(0.5)
            first = False
            response = self.client.get_query_results(queryId=self.query_id)
            status = response["status"]
            if status in ("Failed", "Cancelled", "Timeout"):
                # Retry the query
                if retry:
                    self.logger.info(f"Status: {status}. Retrying...")
                    self.execute()
                    retry = False
                else:
                    break
            if status in ("Scheduled", "Unknown"):
                self.logger.info(f"Status: {status}, continuing to poll.")

        if (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200
            or response["status"] != "Complete"
        ):
            raise Exception(f"Failed: {response}")
        result_size = response.get("statistics", {}).get("recordsMatched")
        results = response["results"]
        self.logger.info(f"Result set size '{int(result_size)}' received.")
        if result_size > self.limit:
            if prev_start == self.start_ts:
                raise Exception(
                    "Stuck in a loop, smaller batch still exceeds limit."
                    "Reduce batch window."
                )
            self.logger.info(
                f"Result set size '{int(result_size)}' exceeded limit "
                f"'{self.limit}'. Re-running sub-batch..."
            )
            results += self._handle_limit_exceeded(response)
        return results

    def _handle_limit_exceeded(self, response):
        results = response.get("results")
        last_record = results[-1]

        latest_ts_str = [i["value"] for i in last_record if i["field"] == "@timestamp"][
            0
        ]
        # Include latest ts in query, this could cause duplicates but
        # without it we might miss ties
        self.start_ts = int(
            datetime.fromisoformat(latest_ts_str).replace(tzinfo=pytz.UTC).timestamp()
        )
        self.execute()
        return self.get_results()

    def _alter_query(self, query):
        query += " | sort @timestamp asc"
        return query
