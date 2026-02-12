"""Conversation and messaging endpoints."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.dependencies import get_current_user, get_derp
from app.models import Conversation, Message, User
from app.schemas import (
    ChatMessageResponse,
    ConversationDetailResponse,
    ConversationResponse,
    MarkReadResponse,
    SendMessageRequest,
    StartConversationRequest,
    UserPublicResponse,
)
from derp import DerpClient

router = APIRouter(prefix="/conversations", tags=["conversations"])


async def _get_other_user(
    derp: DerpClient[User], conversation: Conversation, current_user_id: uuid.UUID
) -> User | None:
    """Get the other user in a conversation."""
    other_user_id = (
        conversation.user2_id
        if conversation.user1_id == current_user_id
        else conversation.user1_id
    )
    other_user = await derp.auth.get_user(other_user_id)
    return other_user


@router.get("", response_model=list[ConversationResponse])
async def list_conversations(
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
) -> list[ConversationResponse]:
    """List all conversations for the current user."""
    conversations = await (
        derp.db.select(Conversation)
        .where(
            (Conversation.c.user1_id == str(user.id))
            | (Conversation.c.user2_id == str(user.id))
        )
        .order_by(Conversation.c.last_message_at, asc=False)
        .execute()
    )

    result = []
    for conv in conversations:
        other_user = await _get_other_user(derp, conv, user.id)
        if not other_user:
            continue

        # Count unread messages
        unread_count = await (
            derp.db.select(Message)
            .where(
                (Message.c.conversation_id == str(conv.id))
                & (Message.c.sender_id != str(user.id))
                & (Message.c.read_at.is_null())
            )
            .count()
        )

        result.append(
            ConversationResponse(
                id=conv.id,
                other_user=UserPublicResponse.model_validate(other_user),
                last_message_at=conv.last_message_at,
                created_at=conv.created_at,
                unread_count=unread_count,
            )
        )

    return result


@router.post(
    "", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED
)
async def start_conversation(
    data: StartConversationRequest,
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
) -> ConversationResponse:
    """Start a new conversation with a user or get existing one."""
    if data.user_id == user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot start conversation with yourself",
        )

    other_user = await derp.auth.get_user(data.user_id)
    if not other_user or not other_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    # Ensure user1_id < user2_id for uniqueness
    user1_id, user2_id = sorted([user.id, data.user_id])

    # Check if conversation exists
    existing = await (
        derp.db.select(Conversation)
        .where(
            (Conversation.c.user1_id == str(user1_id))
            & (Conversation.c.user2_id == str(user2_id))
        )
        .first_or_none()
    )

    if existing:
        return ConversationResponse(
            id=existing.id,
            other_user=UserPublicResponse.model_validate(other_user),
            last_message_at=existing.last_message_at,
            created_at=existing.created_at,
            unread_count=0,
        )

    # Create new conversation
    conversation = await (
        derp.db.insert(Conversation)
        .values(user1_id=user1_id, user2_id=user2_id)
        .returning(Conversation)
        .execute()
    )

    return ConversationResponse(
        id=conversation.id,
        other_user=UserPublicResponse.model_validate(other_user),
        last_message_at=None,
        created_at=conversation.created_at,
        unread_count=0,
    )


@router.get("/{conversation_id}", response_model=ConversationDetailResponse)
async def get_conversation(
    conversation_id: uuid.UUID,
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
    limit: int = Query(ge=1, le=100, default=50),
    before: uuid.UUID | None = Query(default=None),
) -> ConversationDetailResponse:
    """Get a conversation with its messages."""
    conversation = await (
        derp.db.select(Conversation)
        .where(Conversation.c.id == str(conversation_id))
        .first_or_none()
    )

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found",
        )

    # Check user is part of conversation
    if user.id not in (conversation.user1_id, conversation.user2_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not a participant in this conversation",
        )

    other_user = await _get_other_user(derp, conversation, user.id)

    # Get messages
    query = (
        derp.db.select(Message)
        .where(Message.c.conversation_id == str(conversation_id))
        .order_by(Message.c.created_at, asc=False)
        .limit(limit)
    )

    if before:
        cursor_msg = await (
            derp.db.select(Message).where(Message.c.id == str(before)).first_or_none()
        )
        if cursor_msg:
            query = query.where(Message.c.created_at < cursor_msg.created_at)

    messages = await query.execute()

    return ConversationDetailResponse(
        id=conversation.id,
        other_user=UserPublicResponse.model_validate(other_user),
        messages=[
            ChatMessageResponse(
                id=msg.id,
                sender_id=msg.sender_id,
                content=msg.content,
                read_at=msg.read_at,
                created_at=msg.created_at,
                is_mine=msg.sender_id == user.id,
            )
            for msg in messages
        ],
        created_at=conversation.created_at,
    )


@router.post(
    "/{conversation_id}/messages",
    response_model=ChatMessageResponse,
    status_code=status.HTTP_201_CREATED,
)
async def send_message(
    conversation_id: uuid.UUID,
    data: SendMessageRequest,
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
) -> ChatMessageResponse:
    """Send a message in a conversation."""
    conversation = await (
        derp.db.select(Conversation)
        .where(Conversation.c.id == str(conversation_id))
        .first_or_none()
    )

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found",
        )

    if user.id not in (conversation.user1_id, conversation.user2_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not a participant in this conversation",
        )

    now = datetime.now(UTC)

    # Create message
    message = await (
        derp.db.insert(Message)
        .values(
            conversation_id=conversation_id,
            sender_id=user.id,
            content=data.content,
        )
        .returning(Message)
        .execute()
    )

    # Update conversation last_message_at
    await (
        derp.db.update(Conversation)
        .set(last_message_at=now)
        .where(Conversation.c.id == str(conversation_id))
        .execute()
    )

    return ChatMessageResponse(
        id=message.id,
        sender_id=message.sender_id,
        content=message.content,
        read_at=message.read_at,
        created_at=message.created_at,
        is_mine=True,
    )


@router.patch("/{conversation_id}/read", response_model=MarkReadResponse)
async def mark_messages_read(
    conversation_id: uuid.UUID,
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
) -> MarkReadResponse:
    """Mark all messages from the other user as read."""
    conversation = await (
        derp.db.select(Conversation)
        .where(Conversation.c.id == str(conversation_id))
        .first_or_none()
    )

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found",
        )

    if user.id not in (conversation.user1_id, conversation.user2_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not a participant in this conversation",
        )

    now = datetime.now(UTC)

    result = (
        await derp.db.update(Message)
        .set(read_at=now)
        .where(
            (Conversation.c.id == str(conversation_id))
            & (Message.c.sender_id != str(user.id))
            & (Message.c.read_at.is_null())
        )
        .returning(Message.c.id)
        .execute()
    )

    return MarkReadResponse(marked_count=len(result))
