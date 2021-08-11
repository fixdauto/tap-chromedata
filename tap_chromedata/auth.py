"""chromedata Authentication."""


from singer_sdk.authenticators import SimpleAuthenticator


class chromedataAuthenticator(SimpleAuthenticator):
    """Authenticator class for chromedata."""

    @classmethod
    def create_for_stream(cls, stream) -> "chromedataAuthenticator":
        return cls(
            stream=stream,
            auth_headers={
                "Private-Token": stream.config.get("auth_token")
            }
        )
