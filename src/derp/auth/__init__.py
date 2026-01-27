"""OAuth2 authentication utilities for Derp ORM.

Example usage:

    from derp.auth import OAuth2Client, TokenStore

    client = OAuth2Client(
        client_id="your-client-id",
        client_secret="your-client-secret",
        authorization_url="https://example.com/oauth/authorize",
        token_url="https://example.com/oauth/token",
        redirect_uri="https://yourapp.com/callback",
    )

    # Generate authorization URL
    auth_url, state = client.get_authorization_url()

    # Exchange authorization code for tokens
    tokens = await client.exchange_code("authorization-code", state)

    # Refresh tokens
    new_tokens = await client.refresh_token(tokens.refresh_token)
"""

from derp.auth.client import OAuth2Client
from derp.auth.tokens import MemoryTokenStore, TokenResponse, TokenStore

__all__ = ["OAuth2Client", "TokenResponse", "TokenStore", "MemoryTokenStore"]
