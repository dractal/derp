"""Channel and messaging endpoints."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.dependencies import get_current_user, get_derp, get_workspace_member
from app.models import Channel, ChannelMember, Message, User
from app.schemas import (
    ChannelMemberResponse,
    ChannelResponse,
    ChatMessageResponse,
    CreateChannelRequest,
    EditMessageRequest,
    MessageResponse,
    SendMessageRequest,
    StartDMRequest,
    UpdateChannelRequest,
)
from derp import DerpClient
from derp.auth.models import OrgMemberInfo, UserInfo

router = APIRouter(tags=["channels"])


# ---------------------------------------------------------------------------
# Channel CRUD
# ---------------------------------------------------------------------------


@router.post(
    "/workspaces/{workspace_id}/channels",
    response_model=ChannelResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_channel(
    workspace_id: uuid.UUID,
    data: CreateChannelRequest,
    user: UserInfo = Depends(get_current_user),
    _member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> ChannelResponse:
    """Create a new channel in a workspace."""
    # Check name uniqueness within workspace
    existing = await (
        derp.db.select(Channel)
        .where(
            (Channel.c.workspace_id == str(workspace_id))
            & (Channel.c.name == data.name)
            & (Channel.c.is_dm == False)  # noqa: E712
        )
        .first_or_none()
    )
    if existing:
        raise HTTPException(status_code=409, detail="Channel name already taken")

    channel = await (
        derp.db.insert(Channel)
        .values(
            workspace_id=workspace_id,
            name=data.name,
            topic=data.topic,
            is_private=data.is_private,
            created_by=user.id,
        )
        .returning(Channel)
        .execute()
    )

    # Creator auto-joins
    await (
        derp.db.insert(ChannelMember)
        .values(channel_id=channel.id, user_id=user.id)
        .execute()
    )

    return ChannelResponse(
        id=channel.id,
        workspace_id=channel.workspace_id,
        name=channel.name,
        topic=channel.topic,
        is_private=channel.is_private,
        is_dm=channel.is_dm,
        member_count=1,
        last_message_at=None,
        created_at=channel.created_at,
    )


@router.post(
    "/workspaces/{workspace_id}/dm",
    response_model=ChannelResponse,
    status_code=status.HTTP_201_CREATED,
)
async def start_dm(
    workspace_id: uuid.UUID,
    data: StartDMRequest,
    user: UserInfo = Depends(get_current_user),
    _member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> ChannelResponse:
    """Start or get a DM channel with another workspace member."""
    if data.user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot DM yourself")

    # Verify target is a workspace member
    target_member = await derp.auth.get_org_member(
        org_id=workspace_id, user_id=data.user_id
    )
    if not target_member:
        raise HTTPException(status_code=404, detail="User is not in this workspace")

    target = await derp.auth.get_user(data.user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    # Check if DM already exists between these two users in this workspace
    my_channels = await (
        derp.db.select(ChannelMember)
        .where(ChannelMember.c.user_id == str(user.id))
        .execute()
    )
    my_channel_ids = [str(m.channel_id) for m in my_channels]

    if my_channel_ids:
        dm_channels = await (
            derp.db.select(Channel)
            .where(
                (Channel.c.workspace_id == str(workspace_id))
                & (Channel.c.is_dm == True)  # noqa: E712
                & (Channel.c.id.in_(my_channel_ids))
            )
            .execute()
        )

        for dm in dm_channels:
            other_member = await (
                derp.db.select(ChannelMember)
                .where(
                    (ChannelMember.c.channel_id == str(dm.id))
                    & (ChannelMember.c.user_id == str(data.user_id))
                )
                .first_or_none()
            )
            if other_member:
                count = await (
                    derp.db.select(ChannelMember)
                    .where(ChannelMember.c.channel_id == str(dm.id))
                    .count()
                )
                return ChannelResponse(
                    id=dm.id,
                    workspace_id=dm.workspace_id,
                    name=dm.name,
                    topic=None,
                    is_private=True,
                    is_dm=True,
                    member_count=count,
                    last_message_at=dm.last_message_at,
                    created_at=dm.created_at,
                )

    # Create new DM channel
    target_name = target.username or target.email.split("@")[0]
    my_name = user.username or user.email.split("@")[0]
    dm_name = f"dm-{min(my_name, target_name)}-{max(my_name, target_name)}"

    channel = await (
        derp.db.insert(Channel)
        .values(
            workspace_id=workspace_id,
            name=dm_name,
            is_private=True,
            is_dm=True,
            created_by=user.id,
        )
        .returning(Channel)
        .execute()
    )

    # Add both users
    await (
        derp.db.insert(ChannelMember)
        .values(channel_id=channel.id, user_id=user.id)
        .execute()
    )
    await (
        derp.db.insert(ChannelMember)
        .values(channel_id=channel.id, user_id=data.user_id)
        .execute()
    )

    return ChannelResponse(
        id=channel.id,
        workspace_id=channel.workspace_id,
        name=channel.name,
        topic=None,
        is_private=True,
        is_dm=True,
        member_count=2,
        last_message_at=None,
        created_at=channel.created_at,
    )


@router.get("/channels/{channel_id}", response_model=ChannelResponse)
async def get_channel(
    channel_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> ChannelResponse:
    """Get channel details."""
    channel = await (
        derp.db.select(Channel).where(Channel.c.id == str(channel_id)).first_or_none()
    )
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    # Verify membership for private channels
    if channel.is_private:
        membership = await (
            derp.db.select(ChannelMember)
            .where(
                (ChannelMember.c.channel_id == str(channel_id))
                & (ChannelMember.c.user_id == str(user.id))
            )
            .first_or_none()
        )
        if not membership:
            raise HTTPException(status_code=403, detail="Not a member of this channel")

    count = await (
        derp.db.select(ChannelMember)
        .where(ChannelMember.c.channel_id == str(channel_id))
        .count()
    )

    return ChannelResponse(
        id=channel.id,
        workspace_id=channel.workspace_id,
        name=channel.name,
        topic=channel.topic,
        is_private=channel.is_private,
        is_dm=channel.is_dm,
        member_count=count,
        last_message_at=channel.last_message_at,
        created_at=channel.created_at,
    )


@router.patch("/channels/{channel_id}", response_model=ChannelResponse)
async def update_channel(
    channel_id: uuid.UUID,
    data: UpdateChannelRequest,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> ChannelResponse:
    """Update channel name or topic."""
    channel = await (
        derp.db.select(Channel).where(Channel.c.id == str(channel_id)).first_or_none()
    )
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    membership = await (
        derp.db.select(ChannelMember)
        .where(
            (ChannelMember.c.channel_id == str(channel_id))
            & (ChannelMember.c.user_id == str(user.id))
        )
        .first_or_none()
    )
    if not membership:
        raise HTTPException(status_code=403, detail="Not a member of this channel")

    updates = data.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    await (
        derp.db.update(Channel)
        .set(**updates)
        .where(Channel.c.id == str(channel_id))
        .execute()
    )

    return await get_channel(channel_id, user, derp)


@router.delete("/channels/{channel_id}", response_model=MessageResponse)
async def delete_channel(
    channel_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> MessageResponse:
    """Delete a channel (creator or workspace owner only)."""
    channel = await (
        derp.db.select(Channel).where(Channel.c.id == str(channel_id)).first_or_none()
    )
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    if channel.name == "general" and not channel.is_dm:
        raise HTTPException(status_code=400, detail="Cannot delete #general")

    # Check if user is channel creator or workspace owner
    ws_member = await derp.auth.get_org_member(
        org_id=channel.workspace_id, user_id=user.id
    )
    if not ws_member:
        raise HTTPException(status_code=403, detail="Not a workspace member")

    if channel.created_by != user.id and ws_member.role != "owner":
        raise HTTPException(status_code=403, detail="Only creator or owner can delete")

    await derp.db.delete(Channel).where(Channel.c.id == str(channel_id)).execute()

    return MessageResponse(message="Channel deleted")


# ---------------------------------------------------------------------------
# Channel membership
# ---------------------------------------------------------------------------


@router.post("/channels/{channel_id}/join", response_model=MessageResponse)
async def join_channel(
    channel_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> MessageResponse:
    """Join a public channel."""
    channel = await (
        derp.db.select(Channel).where(Channel.c.id == str(channel_id)).first_or_none()
    )
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    if channel.is_private:
        raise HTTPException(status_code=403, detail="Cannot join a private channel")

    # Verify workspace membership
    ws_member = await derp.auth.get_org_member(
        org_id=channel.workspace_id, user_id=user.id
    )
    if not ws_member:
        raise HTTPException(status_code=403, detail="Not a workspace member")

    existing = await (
        derp.db.select(ChannelMember)
        .where(
            (ChannelMember.c.channel_id == str(channel_id))
            & (ChannelMember.c.user_id == str(user.id))
        )
        .first_or_none()
    )
    if existing:
        return MessageResponse(message="Already a member")

    await (
        derp.db.insert(ChannelMember)
        .values(channel_id=channel_id, user_id=user.id)
        .execute()
    )

    return MessageResponse(message="Joined channel")


@router.post("/channels/{channel_id}/leave", response_model=MessageResponse)
async def leave_channel(
    channel_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> MessageResponse:
    """Leave a channel."""
    channel = await (
        derp.db.select(Channel).where(Channel.c.id == str(channel_id)).first_or_none()
    )
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")

    await (
        derp.db.delete(ChannelMember)
        .where(
            (ChannelMember.c.channel_id == str(channel_id))
            & (ChannelMember.c.user_id == str(user.id))
        )
        .execute()
    )

    return MessageResponse(message="Left channel")


@router.get(
    "/channels/{channel_id}/members", response_model=list[ChannelMemberResponse]
)
async def list_channel_members(
    channel_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> list[ChannelMemberResponse]:
    """List members of a channel."""
    membership = await (
        derp.db.select(ChannelMember)
        .where(
            (ChannelMember.c.channel_id == str(channel_id))
            & (ChannelMember.c.user_id == str(user.id))
        )
        .first_or_none()
    )
    if not membership:
        raise HTTPException(status_code=403, detail="Not a member of this channel")

    members = await (
        derp.db.select(ChannelMember)
        .where(ChannelMember.c.channel_id == str(channel_id))
        .execute()
    )

    result = []
    for m in members:
        u = await (
            derp.db.select(User).where(User.c.id == str(m.user_id)).first_or_none()
        )
        result.append(
            ChannelMemberResponse(
                user_id=m.user_id,
                username=u.username if u else None,
                display_name=u.display_name if u else None,
                avatar_url=u.avatar_url if u else None,
                joined_at=m.joined_at,
            )
        )

    return result


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


async def _enrich_message(msg: Message, derp: DerpClient) -> ChatMessageResponse:
    """Add sender info to a message."""
    sender = await (
        derp.db.select(User).where(User.c.id == str(msg.sender_id)).first_or_none()
    )
    return ChatMessageResponse(
        id=msg.id,
        channel_id=msg.channel_id,
        sender_id=msg.sender_id,
        sender_name=(
            sender.display_name or sender.username or sender.email
            if sender
            else "Unknown"
        ),
        sender_avatar=sender.avatar_url if sender else None,
        content=msg.content,
        created_at=msg.created_at,
        edited_at=msg.edited_at,
    )


@router.get("/channels/{channel_id}/messages", response_model=list[ChatMessageResponse])
async def list_messages(
    channel_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
    limit: int = Query(ge=1, le=100, default=50),
    before: uuid.UUID | None = Query(default=None),
) -> list[ChatMessageResponse]:
    """Get messages in a channel (newest first)."""
    membership = await (
        derp.db.select(ChannelMember)
        .where(
            (ChannelMember.c.channel_id == str(channel_id))
            & (ChannelMember.c.user_id == str(user.id))
        )
        .first_or_none()
    )
    if not membership:
        raise HTTPException(status_code=403, detail="Not a member of this channel")

    query = (
        derp.db.select(Message)
        .where(Message.c.channel_id == str(channel_id))
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

    return [await _enrich_message(m, derp) for m in messages]


@router.post(
    "/channels/{channel_id}/messages",
    response_model=ChatMessageResponse,
    status_code=status.HTTP_201_CREATED,
)
async def send_message(
    channel_id: uuid.UUID,
    data: SendMessageRequest,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> ChatMessageResponse:
    """Send a message to a channel."""
    membership = await (
        derp.db.select(ChannelMember)
        .where(
            (ChannelMember.c.channel_id == str(channel_id))
            & (ChannelMember.c.user_id == str(user.id))
        )
        .first_or_none()
    )
    if not membership:
        raise HTTPException(status_code=403, detail="Not a member of this channel")

    message = await (
        derp.db.insert(Message)
        .values(
            channel_id=channel_id,
            sender_id=user.id,
            content=data.content,
        )
        .returning(Message)
        .execute()
    )

    # Update channel last_message_at
    await (
        derp.db.update(Channel)
        .set(last_message_at=datetime.now(UTC))
        .where(Channel.c.id == str(channel_id))
        .execute()
    )

    return await _enrich_message(message, derp)


@router.patch("/messages/{message_id}", response_model=ChatMessageResponse)
async def edit_message(
    message_id: uuid.UUID,
    data: EditMessageRequest,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> ChatMessageResponse:
    """Edit a message (sender only)."""
    message = await (
        derp.db.select(Message).where(Message.c.id == str(message_id)).first_or_none()
    )
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    if message.sender_id != user.id:
        raise HTTPException(status_code=403, detail="Can only edit your own messages")

    [updated] = await (
        derp.db.update(Message)
        .set(content=data.content, edited_at=datetime.now(UTC))
        .where(Message.c.id == str(message_id))
        .returning(Message)
        .execute()
    )

    return await _enrich_message(updated, derp)


@router.delete("/messages/{message_id}", response_model=MessageResponse)
async def delete_message(
    message_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> MessageResponse:
    """Delete a message (sender only)."""
    message = await (
        derp.db.select(Message).where(Message.c.id == str(message_id)).first_or_none()
    )
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    if message.sender_id != user.id:
        raise HTTPException(status_code=403, detail="Can only delete your own messages")

    await derp.db.delete(Message).where(Message.c.id == str(message_id)).execute()

    return MessageResponse(message="Message deleted")
