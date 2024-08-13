"""Custom client handling, including CloudWatchStream base class."""

from __future__ import annotations

import typing as t

from singer_sdk.streams import Stream

from tap_cloudwatch.cloudwatch_api import CloudwatchAPI

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class CloudWatchStream(Stream):
    """Stream class for CloudWatch streams."""

    @property
    def is_sorted(self) -> bool:
        """Expect stream to be sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        Returns
        -------
            `True` if stream is sorted. Defaults to `False`.

        """
        return True

    @property
    def check_sorted(self) -> bool:
        """Check if stream is sorted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.

        Returns
        -------
            `True` if sorting is checked. Defaults to `True`.

        """
        # The stream is sorted but when the limit is exceeded we recursively
        # request sub-batches to get ever smaller batches until all data has been
        # replicated. As part of that we use >= logic so some duplicates are
        # created on the edges of the date range window. The requests are at seconds
        # grain but the log timestamps are at the millisecond grain, which causes this
        # to throw an exception if the max is like `2023-02-20 06:01:57.792` because the
        # sub-batch filter is `2023-02-20 06:01:57` and we get some records that are
        # matching the start date but are smaller than the previous max like
        # `2023-02-20 06:01:57.009`. For that reason it is disabled.
        return False

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: move to iterate batches
        # TODO: log stats metrics returned by cloudwatch
        # self.metrics_logger.info('test')
        client = CloudwatchAPI(self.logger)
        client.authenticate(self.config)
        cloudwatch_iter = client.get_records_iterator(
            self.get_starting_timestamp(context),
            self.config.get("log_group_name"),
            self.config.get("query"),
            self.config.get("batch_increment_s"),
            self.config.get("end_date"),
        )
        for batch in cloudwatch_iter:
            for record in batch:
                yield {i["field"][1:]: i["value"] for i in record}
