"""Stream type classes for tap-cloudwatch."""

from typing import List

from singer_sdk import typing as th

from tap_cloudwatch.client import CloudWatchStream


class LogStream(CloudWatchStream):
    """Log stream."""

    name = "log"
    primary_keys: List[str] = []
    replication_key = "timestamp"

    @property
    def schema(self):
        """Dynamically detect the json schema for the stream."""
        properties: List[th.Property] = []

        # TODO: handle parse and unmask syntax
        # | parse @message "[*] *" as loggingType, loggingMessage
        for prop in self.config.get('query').split("|")[0].split(","):
            prop = prop.strip()
            if prop.startswith("fields "):
                prop = prop[7:].strip()
            if prop.startswith('@'):
                prop = prop[1:]
            if prop == 'timestamp':
                properties.append(th.Property(prop, th.DateTimeType()))
            else:
                # Assume string type for all fields
                properties.append(th.Property(prop, th.StringType()))
        return th.PropertiesList(*properties).to_dict()
