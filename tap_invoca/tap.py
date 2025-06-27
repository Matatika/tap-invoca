"""Invoca tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from typing_extensions import override

from tap_invoca import streams


class TapInvoca(Tap):
    """Invoca tap class."""

    name = "tap-invoca"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "network_name",
            th.StringType(nullable=False),
            required=True,
            title="Network Name",
            description="Network name associated with the given network ID",
        ),
        th.Property(
            "network_id",
            th.IntegerType(nullable=False),
            required=True,
            title="Network ID",
            description="Network ID associated with the given network name",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @override
    def discover_streams(self):
        return [
            streams.NetworkTransactionStream(self),
        ]


if __name__ == "__main__":
    TapInvoca.cli()
