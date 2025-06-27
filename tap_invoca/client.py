"""REST client handling, including InvocaStream base class."""

from __future__ import annotations

from functools import cached_property

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream
from typing_extensions import override


class InvocaStream(RESTStream):
    """Invoca stream class."""

    @override
    @cached_property
    def url_base(self):
        return f'https://{self.config["network_name"]}.invoca.net/api/2023-10-09'

    @override
    @cached_property
    def authenticator(self):
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=self.config.get("auth_token", ""),
            location="header",
        )
