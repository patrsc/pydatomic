"""Utility functions"""
from datetime import datetime, timezone


def now():
    return datetime_to_int(datetime.now(timezone.utc))


def int_to_datetime(n: int):
    return datetime.fromtimestamp(n/1000.0, tz=timezone.utc)


def datetime_to_int(t: datetime):
    return int(t.timestamp()*1000)
