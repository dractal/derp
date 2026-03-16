"""Database models for the Slack clone."""

from __future__ import annotations

import uuid
from datetime import datetime

from derp.auth.models import AuthUser
from derp.orm import Field, Table
from derp.orm.fields import (
    UUID,
    Boolean,
    ForeignKey,
    ForeignKeyAction,
    Text,
    Timestamp,
    Varchar,
)


class User(AuthUser, table="users"):
    """User model with profile fields."""

    username: str | None = Field(Varchar(100), nullable=True)
    display_name: str | None = Field(Varchar(255), nullable=True)
    avatar_url: str | None = Field(Varchar(512), nullable=True)


class Channel(Table, table="channels"):
    """A channel within a workspace.

    Channels can be public (#general) or private. DMs are private
    channels with is_dm=True.
    """

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    workspace_id: uuid.UUID = Field(UUID(), index=True)
    name: str = Field(Varchar(80))
    topic: str | None = Field(Text(), nullable=True)
    is_private: bool = Field(Boolean(), default="false")
    is_dm: bool = Field(Boolean(), default="false")
    created_by: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
    )
    last_message_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class ChannelMember(Table, table="channel_members"):
    """Membership in a channel."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    channel_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(Channel, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    user_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    joined_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class Message(Table, table="messages"):
    """Message within a channel."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    channel_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(Channel, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    sender_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
    )
    content: str = Field(Text())
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
    edited_at: datetime | None = Field(Timestamp(with_timezone=True), nullable=True)
