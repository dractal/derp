"""Database models for the Slack clone."""

from __future__ import annotations

from derp.auth.models import AuthUser
from derp.orm import (
    UUID,
    Boolean,
    Field,
    Index,
    Nullable,
    Table,
    Text,
    TimestampTZ,
    Varchar,
)


class User(AuthUser, table="users"):
    """User model with profile fields."""

    username: Nullable[Varchar[100]] = Field()
    display_name: Nullable[Varchar[255]] = Field()
    avatar_url: Nullable[Varchar[512]] = Field()


class Channel(Table, table="channels"):
    """A channel within a workspace.

    Channels can be public (#general) or private. DMs are private
    channels with is_dm=True.
    """

    id: UUID = Field(primary=True, default="gen_random_uuid()")
    workspace_id: UUID = Field()
    name: Varchar[80] = Field()
    topic: Nullable[Text] = Field()
    is_private: Boolean = Field(default="false")
    is_dm: Boolean = Field(default="false")
    created_by: UUID = Field(foreign_key=User.id, on_delete="cascade")
    last_message_at: Nullable[TimestampTZ] = Field()
    created_at: TimestampTZ = Field(default="now()")

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.workspace_id)]


class ChannelMember(Table, table="channel_members"):
    """Membership in a channel."""

    id: UUID = Field(primary=True, default="gen_random_uuid()")
    channel_id: UUID = Field(foreign_key=Channel.id, on_delete="cascade")
    user_id: UUID = Field(foreign_key=User.id, on_delete="cascade")
    joined_at: TimestampTZ = Field(default="now()")

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.channel_id), Index(cls.user_id)]


class Message(Table, table="messages"):
    """Message within a channel."""

    id: UUID = Field(primary=True, default="gen_random_uuid()")
    channel_id: UUID = Field(foreign_key=Channel.id, on_delete="cascade")
    sender_id: UUID = Field(foreign_key=User.id, on_delete="cascade")
    content: Text = Field()
    created_at: TimestampTZ = Field(default="now()")
    edited_at: Nullable[TimestampTZ] = Field()

    @classmethod
    def indexes(cls) -> list[Index]:
        return [Index(cls.channel_id)]
