"""CloudWatch tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_cloudwatch.streams import LogStream

STREAM_TYPES = [
    LogStream,
]


class TapCloudWatch(Tap):
    """CloudWatch tap for extracting log data from AWS Cloudwatch Logs Insights API."""

    name = "tap-cloudwatch"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "aws_access_key_id",
            th.StringType,
            secret=True,
            description="The access key for your AWS account.",
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            secret=True,
            description="The secret key for your AWS account.",
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            secret=True,
            description=(
                "The session key for your AWS account. This is only needed when"
                " you are using temporary credentials."
            ),
        ),
        th.Property(
            "aws_profile",
            th.StringType,
            description=(
                "The AWS credentials profile name to use. The profile must be "
                "configured and accessible."
            ),
        ),
        th.Property(
            "aws_endpoint_url",
            th.StringType,
            description="The complete URL to use for the constructed client.",
        ),
        th.Property(
            "aws_region_name",
            th.StringType,
            description="The AWS region name (e.g. us-east-1) ",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "log_group_name",
            th.StringType,
            required=True,
            description="The log group on which to perform the query.",
        ),
        th.Property(
            "query",
            th.StringType,
            required=True,
            description=(
                "The query string to use. For more information, see [CloudWatch"
                " Logs Insights Query Syntax](https://docs.aws.amazon.com/Amazon"
                "CloudWatch/latest/logs/CWL_QuerySyntax.html)."
            ),
        ),
        th.Property(
            "batch_increment_s",
            th.IntegerType,
            default=3600,  # type: ignore
            description=(
                "The size of the time window to query by, default 3,600 seconds"
                " (i.e. 1 hour). If the result set for a batch is greater than "
                "the max limit of 10,000 records then the tap will query the "
                "same window again where >= the most recent record received. This "
                "means that the same data is potentially being scanned >1 times"
                " but < 2 times, depending on the amount the results set went over"
                " the 10k max. For example a batch window with 15k records would "
                "scan the 15k once, receiving 10k results, then scan ~5k again to "
                "get the rest. The net result is the same data was scanned ~1.5 times"
                " for that batch. To avoid this you should set the batch window to "
                "avoid exceeding the 10k limit."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapCloudWatch.cli()
