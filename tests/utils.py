"""Utility functions for tests."""

from datetime import datetime, timezone


def datetime_from_str(date_str):
    """Convert string to datetime."""
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
