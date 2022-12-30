"""Custom client handling, including CloudWatchStream base class."""

from typing import Optional, Iterable
from singer_sdk import typing as th

from singer_sdk.streams import Stream
from tap_cloudwatch.cloudwatch_api import CloudwatchAPI

class CloudWatchStream(Stream):
    """Stream class for CloudWatch streams."""

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: move to iterate batches
        # TODO: log stats metrics returned by cloudwatch
        # self.metrics_logger.info('test')
        client = CloudwatchAPI()
        client.authenticate(self.config)
        query_resp = client.get_records(
            self.get_starting_timestamp(context),
            self.config.get('log_group_name'),
            self.config.get('query'),
            self.config.get('start_date'),
        )
        for record in query_resp.get('results'):
            yield {i['field'][1:]: i['value'] for i in record}

