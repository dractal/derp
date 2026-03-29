"""Workspace (organization) endpoints."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_current_user, get_derp, get_workspace_member
from app.models import Channel, ChannelMember
from app.schemas import (
    ChannelResponse,
    CreateWorkspaceRequest,
    InviteMemberRequest,
    MessageResponse,
    UserPublicResponse,
    WorkspaceMemberResponse,
    WorkspaceResponse,
)
from derp import DerpClient
from derp.auth.exceptions import OrgAlreadyExistsError, OrgMemberExistsError
from derp.auth.models import OrgMemberInfo, UserInfo

router = APIRouter(prefix="/workspaces", tags=["workspaces"])


@router.post("", response_model=WorkspaceResponse, status_code=status.HTTP_201_CREATED)
async def create_workspace(
    data: CreateWorkspaceRequest,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> WorkspaceResponse:
    """Create a new workspace. The creator becomes the owner."""
    try:
        org = await derp.auth.create_org(
            name=data.name, slug=data.slug, creator_id=user.id
        )
    except OrgAlreadyExistsError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A workspace with this slug already exists",
        ) from None

    # Create default #general channel
    channel = await (
        derp.db.insert(Channel)
        .values(
            workspace_id=org.id,
            name="general",
            topic="Company-wide announcements and work-based matters",
            is_private=False,
            created_by=user.id,
        )
        .returning(Channel)
        .execute()
    )
    # Add creator to #general
    await (
        derp.db.insert(ChannelMember)
        .values(channel_id=channel.id, user_id=user.id)
        .execute()
    )

    return WorkspaceResponse(
        id=org.id,
        name=org.name,
        slug=org.slug,
        created_at=org.created_at,
    )


@router.get("", response_model=list[WorkspaceResponse])
async def list_workspaces(
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> list[WorkspaceResponse]:
    """List workspaces the current user belongs to."""
    orgs = await derp.auth.list_orgs(user_id=user.id)
    return [
        WorkspaceResponse(id=o.id, name=o.name, slug=o.slug, created_at=o.created_at)
        for o in orgs
    ]


@router.get("/{workspace_id}", response_model=WorkspaceResponse)
async def get_workspace(
    workspace_id: uuid.UUID,
    _member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> WorkspaceResponse:
    """Get workspace details."""
    org = await derp.auth.get_org(workspace_id)
    if org is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return WorkspaceResponse(
        id=org.id, name=org.name, slug=org.slug, created_at=org.created_at
    )


@router.get("/{workspace_id}/members", response_model=list[WorkspaceMemberResponse])
async def list_workspace_members(
    workspace_id: uuid.UUID,
    _member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> list[WorkspaceMemberResponse]:
    """List members of a workspace."""
    members = await derp.auth.list_org_members(workspace_id)
    result = []
    for m in members:
        u = await derp.auth.get_user(m.user_id)
        result.append(
            WorkspaceMemberResponse(
                user_id=m.user_id,
                role=m.role,
                user=UserPublicResponse.model_validate(u) if u else None,
            )
        )
    return result


@router.post(
    "/{workspace_id}/members",
    response_model=WorkspaceMemberResponse,
    status_code=status.HTTP_201_CREATED,
)
async def invite_member(
    workspace_id: uuid.UUID,
    data: InviteMemberRequest,
    member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> WorkspaceMemberResponse:
    """Invite a user to the workspace (owner/admin only)."""
    if member.role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="Only owners and admins can invite")

    target = await derp.auth.get_user(data.user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        new_member = await derp.auth.add_org_member(
            org_id=workspace_id, user_id=data.user_id, role=data.role
        )
    except OrgMemberExistsError:
        raise HTTPException(
            status_code=409, detail="User is already a member"
        ) from None

    # Auto-join them to #general
    general = await (
        derp.db.select(Channel)
        .where(
            (Channel.workspace_id == workspace_id)
            & (Channel.name == "general")
            & (Channel.is_dm == False)  # noqa: E712
        )
        .first_or_none()
    )
    if general:
        existing = await (
            derp.db.select(ChannelMember)
            .where(
                (ChannelMember.channel_id == general.id)
                & (ChannelMember.user_id == data.user_id)
            )
            .first_or_none()
        )
        if not existing:
            await (
                derp.db.insert(ChannelMember)
                .values(channel_id=general.id, user_id=data.user_id)
                .execute()
            )

    return WorkspaceMemberResponse(
        user_id=new_member.user_id,
        role=new_member.role,
        user=UserPublicResponse.model_validate(target),
    )


@router.delete("/{workspace_id}/members/{user_id}", response_model=MessageResponse)
async def remove_member(
    workspace_id: uuid.UUID,
    user_id: uuid.UUID,
    member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> MessageResponse:
    """Remove a member from the workspace (owner only)."""
    if member.role != "owner":
        raise HTTPException(status_code=403, detail="Only owners can remove members")

    removed = await derp.auth.remove_org_member(org_id=workspace_id, user_id=user_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Member not found")

    # Remove from all channels in this workspace
    channels = await (
        derp.db.select(Channel).where(Channel.workspace_id == workspace_id).execute()
    )
    for ch in channels:
        await (
            derp.db.delete(ChannelMember)
            .where(
                (ChannelMember.channel_id == ch.id) & (ChannelMember.user_id == user_id)
            )
            .execute()
        )

    return MessageResponse(message="Member removed")


@router.get("/{workspace_id}/channels", response_model=list[ChannelResponse])
async def list_workspace_channels(
    workspace_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    _member: OrgMemberInfo = Depends(get_workspace_member),
    derp: DerpClient = Depends(get_derp),
) -> list[ChannelResponse]:
    """List channels the user can see in a workspace."""
    # Get all channels the user is a member of
    memberships = await (
        derp.db.select(ChannelMember).where(ChannelMember.user_id == user.id).execute()
    )
    member_channel_ids = {m.channel_id for m in memberships}

    # Get all channels in workspace
    channels = await (
        derp.db.select(Channel)
        .where(Channel.workspace_id == workspace_id)
        .order_by(Channel.name)
        .execute()
    )

    result = []
    for ch in channels:
        # Show public channels + channels user is a member of
        if ch.is_private and ch.id not in member_channel_ids:
            continue

        count = await (
            derp.db.select(ChannelMember)
            .where(ChannelMember.channel_id == ch.id)
            .count()
        )

        result.append(
            ChannelResponse(
                id=ch.id,
                workspace_id=ch.workspace_id,
                name=ch.name,
                topic=ch.topic,
                is_private=ch.is_private,
                is_dm=ch.is_dm,
                member_count=count,
                last_message_at=ch.last_message_at,
                created_at=ch.created_at,
            )
        )

    return result
