Slack Clone Example
===================

A full messaging app built with Derp: workspaces, channels, messages, auth, file uploads.

Source: ``examples/messaging/``

Setup
-----

.. code-block:: bash

   $ cd examples/messaging
   $ cp .env.example .env  # set DATABASE_URL, JWT_SECRET, etc.
   $ derp migrate
   $ uvicorn app.main:app --reload

Models
------

Extend ``AuthUser`` to add profile fields. Define channels and messages with foreign keys:

.. code-block:: python

   from derp.auth.models import AuthUser
   from derp.orm import (
       Table, Field, Nullable, UUID, Varchar, Text, Boolean, TimestampTZ,
   )

   class User(AuthUser, table="users"):
       username: Nullable[Varchar[100]] = Field()
       display_name: Nullable[Varchar[255]] = Field()
       avatar_url: Nullable[Varchar[512]] = Field()

   class Channel(Table, table="channels"):
       id: UUID = Field(primary=True, default="gen_random_uuid()")
       workspace_id: UUID = Field()
       name: Varchar[80] = Field()
       is_private: Boolean = Field(default="false")
       is_dm: Boolean = Field(default="false")
       created_by: UUID = Field(foreign_key=User.id, on_delete="cascade")

   class Message(Table, table="messages"):
       id: UUID = Field(primary=True, default="gen_random_uuid()")
       channel_id: UUID = Field(foreign_key=Channel.id, on_delete="cascade")
       sender_id: UUID = Field(foreign_key=User.id, on_delete="cascade")
       content: Text = Field()
       created_at: TimestampTZ = Field(default="now()")

Workspaces are ``AuthOrganization`` — no extra table needed.

App Lifecycle
-------------

.. code-block:: python

   @asynccontextmanager
   async def lifespan(app: FastAPI) -> AsyncIterator[None]:
       config = DerpConfig.load(Path(__file__).parent.parent / "derp.toml")
       client = DerpClient(config)
       await client.connect()
       app.state.derp_client = client
       yield
       await client.disconnect()

   app = FastAPI(title="Slack Clone API", lifespan=lifespan)

Dependencies
------------

.. code-block:: python

   def get_derp(request: Request) -> DerpClient:
       return request.app.state.derp_client

   async def get_current_user(
       request: Request, derp: DerpClient = Depends(get_derp)
   ) -> UserInfo:
       session = await derp.auth.authenticate(request)
       if session is None:
           raise HTTPException(401, "Invalid or expired token")
       user = await derp.auth.get_user(session.user_id)
       if not user:
           raise HTTPException(401, "User not found")
       return user

Key Endpoints
-------------

**Create workspace** (creates org + #general channel):

.. code-block:: python

   @router.post("/workspaces")
   async def create_workspace(
       body: CreateWorkspaceRequest,
       user: UserInfo = Depends(get_current_user),
       derp: DerpClient = Depends(get_derp),
   ):
       org = await derp.auth.create_org(
           name=body.name, slug=body.slug, creator_id=user.id,
       )
       # Auto-create #general channel
       await derp.db.insert(Channel).values(
           workspace_id=org.id, name="general", created_by=user.id,
       ).execute()
       return org

**Send message** (insert + update channel's last_message_at):

.. code-block:: python

   @router.post("/channels/{channel_id}/messages")
   async def send_message(
       channel_id: uuid.UUID,
       body: SendMessageRequest,
       user: UserInfo = Depends(get_current_user),
       derp: DerpClient = Depends(get_derp),
   ):
       msg = await (
           derp.db.insert(Message)
           .values(channel_id=channel_id, sender_id=user.id, content=body.content)
           .returning(Message)
           .execute()
       )
       await (
           derp.db.update(Channel)
           .set(last_message_at=msg.created_at)
           .where(Channel.id == channel_id)
           .execute()
       )
       return msg

Config
------

.. code-block:: toml

   # derp.toml
   [database]
   db_url = "$DATABASE_URL"
   schema_path = "app/*"

   [auth.native]
   enable_confirmation = false
   enable_magic_link = true

   [auth.native.jwt]
   secret = "$JWT_SECRET"

   [kv.valkey]
   host = "localhost"
   port = 6379

   [storage]
   endpoint_url = "$STORAGE_ENDPOINT_URL"
   access_key_id = "$STORAGE_ACCESS_KEY_ID"
   secret_access_key = "$STORAGE_SECRET_ACCESS_KEY"
