"""Database models for the messaging application."""

from __future__ import annotations

import uuid
from datetime import datetime

from derp.auth.models import AuthSession, BaseUser
from derp.orm import Field, Table
from derp.orm.fields import (
    UUID,
    ForeignKey,
    ForeignKeyAction,
    Text,
    Timestamp,
    Varchar,
)


class User(BaseUser, table="users"):
    """User model with messaging profile fields."""

    username: str | None = Field(Varchar(100), nullable=True)
    avatar_url: str | None = Field(Varchar(512), nullable=True)
    bio: str | None = Field(Text(), nullable=True)


class AuthSession(AuthSession, table="auth_sessions"):
    """Authentication session table."""


class Conversation(Table, table="conversations"):
    """Conversation between two users.

    user1_id < user2_id is enforced to ensure uniqueness without needing
    a composite unique constraint on both orderings.
    """

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    user1_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    user2_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    last_message_at: datetime | None = Field(
        Timestamp(with_timezone=True), nullable=True
    )
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")


class Message(Table, table="messages"):
    """Message within a conversation."""

    id: uuid.UUID = Field(UUID(), primary_key=True, default="gen_random_uuid()")
    conversation_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(Conversation, on_delete=ForeignKeyAction.CASCADE),
        index=True,
    )
    sender_id: uuid.UUID = Field(
        UUID(),
        foreign_key=ForeignKey(User, on_delete=ForeignKeyAction.CASCADE),
    )
    content: str = Field(Text())
    read_at: datetime | None = Field(Timestamp(with_timezone=True), nullable=True)
    created_at: datetime = Field(Timestamp(with_timezone=True), default="now()")
