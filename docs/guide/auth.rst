Auth
====

Config
------

.. code-block:: toml

   # derp.toml
   [auth.native.jwt]
   secret = "$JWT_SECRET"
   algorithm = "HS256"
   access_token_expire_minutes = 15
   refresh_token_expire_days = 7

   [auth.native.password]
   min_length = 8

   [auth.native]
   enable_signup = true
   enable_confirmation = true
   enable_magic_link = false

Native auth requires ``[email]`` to be configured for sending
confirmation and magic link emails.

Sign Up
-------

.. code-block:: python

   user, tokens = await derp.auth.sign_up(
       email="alice@example.com",
       password="s3cur3passw0rd",
       confirmation_url="https://app.example.com/confirm",
   )
   # tokens.access_token, tokens.refresh_token

Sign In
-------

.. code-block:: python

   user, tokens = await derp.auth.sign_in_with_password(
       "alice@example.com", "s3cur3passw0rd"
   )

Refresh Tokens
--------------

.. code-block:: python

   new_tokens = await derp.auth.refresh_token(tokens.refresh_token)

Authenticate Request
--------------------

.. code-block:: python

   session = await derp.auth.authenticate(request)
   # Returns SessionInfo(user_id, session_id, role, expires_at, org_id, ...)
   # Returns None if no valid token is present

OAuth (Google)
--------------

.. code-block:: python

   # 1. Generate authorization URL
   url = derp.auth.get_oauth_authorization_url("google", state=csrf_token)

   # 2. Handle callback
   user, tokens = await derp.auth.sign_in_with_oauth("google", code=auth_code)

Requires ``[auth.native.google_oauth]`` in ``derp.toml``:

.. code-block:: toml

   [auth.native.google_oauth]
   client_id = "$GOOGLE_CLIENT_ID"
   client_secret = "$GOOGLE_CLIENT_SECRET"
   redirect_uri = "https://app.example.com/auth/callback/google"

Magic Link
----------

.. code-block:: python

   # 1. Send magic link email
   await derp.auth.sign_in_with_magic_link(
       email="alice@example.com",
       magic_link_url="https://app.example.com/auth/magic",
   )

   # 2. Verify token from the link
   user, tokens = await derp.auth.verify_magic_link(token)

Requires ``enable_magic_link = true`` in ``[auth.native]``.

Organizations
-------------

.. code-block:: python

   org = await derp.auth.create_org(
       name="Acme Inc", slug="acme", creator_id=user.id
   )

   await derp.auth.add_org_member(
       org_id=org.id, user_id=other_user_id, role="member"
   )

   new_tokens = await derp.auth.set_active_org(
       session_id=session.session_id, org_id=org.id
   )

   orgs = await derp.auth.list_orgs(user_id=user.id)

Protecting Routes
-----------------

.. code-block:: python

   from fastapi import Request, Depends, HTTPException

   from derp import DerpClient
   from derp.auth import SessionInfo

   def get_derp(request: Request) -> DerpClient:
       return request.app.state.derp_client

   async def get_current_user(
       request: Request,
       derp: DerpClient = Depends(get_derp),
   ) -> SessionInfo:
       session = await derp.auth.authenticate(request)
       if session is None:
           raise HTTPException(status_code=401)
       return session

   @app.get("/orders")
   async def list_orders(session: SessionInfo = Depends(get_current_user)):
       ...

Clerk Backend
-------------

Same interface, different config:

.. code-block:: toml

   [auth.clerk]
   secret_key = "$CLERK_SECRET_KEY"

Only one backend (``[auth.native]`` or ``[auth.clerk]``) can be active
at a time. The ``derp.auth`` property returns the configured backend
transparently.
