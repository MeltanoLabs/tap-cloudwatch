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
            description="he secret key for your AWS account.",
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            secret=True,
            description="The session key for your AWS account. This is only needed when \
                you are using temporary credentials.",
        ),
        th.Property(
            "aws_profile",
            th.StringType,
            description="The AWS credentials profile name to use. The profile must be \
                configured and accessible.",
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
            description="The query string to use. For more information, see [CloudWatch \
                Logs Insights Query Syntax](https://docs.aws.amazon.com/Amazon\
                    CloudWatch/latest/logs/CWL_QuerySyntax.html).",
        ),
        # auto optimize? requests huge limit to start, if timeout then shrink,
        # expand if size=limit
        # batch window size (default to all but if you hit limit = result size
        # then we could be missing records)
        # limit (default to something big )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapCloudWatch.cli()
