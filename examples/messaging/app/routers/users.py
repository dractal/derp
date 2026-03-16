"""User profile endpoints."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status

from app.dependencies import get_current_user, get_derp, get_workspace_member
from app.models import User
from app.schemas import (
    AvatarUploadResponse,
    UserProfileUpdateRequest,
    UserPublicResponse,
)
from derp import DerpClient
from derp.auth.models import OrgMemberInfo, UserInfo

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserPublicResponse)
async def get_current_user_profile(
    user: UserInfo = Depends(get_current_user),
) -> UserPublicResponse:
    """Get current user's public profile."""
    return UserPublicResponse.model_validate(user)


@router.patch("/me", response_model=UserPublicResponse)
async def update_current_user_profile(
    data: UserProfileUpdateRequest,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> UserPublicResponse:
    """Update current user's profile."""
    updates = data.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update",
        )

    updated_user = await derp.auth.update_user(user_id=user.id, **updates)
    return UserPublicResponse.model_validate(updated_user)


@router.post("/me/avatar", response_model=AvatarUploadResponse)
async def upload_avatar(
    file: UploadFile,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> AvatarUploadResponse:
    """Upload or update user avatar."""
    allowed_types = {"image/jpeg", "image/png", "image/webp", "image/gif"}
    if file.content_type not in allowed_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type. Allowed: {', '.join(allowed_types)}",
        )

    max_size = 5 * 1024 * 1024  # 5MB
    content = await file.read()
    if len(content) > max_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File too large. Maximum size is 5MB",
        )

    extension = file.content_type.split("/")[-1]
    key = f"avatars/{user.id}/{uuid.uuid4().hex}.{extension}"

    # Delete old avatar if exists
    full_user = await derp.db.select(User).where(User.c.id == user.id).first_or_none()
    if full_user and full_user.avatar_url:
        old_key = full_user.avatar_url.split("avatars/")[-1]
        try:
            await derp.storage.delete_file(bucket="avatars", key=old_key)
        except Exception:
            pass

    await derp.storage.upload_file(
        bucket="avatars",
        key=key,
        data=content,
        content_type=file.content_type,
        metadata={"user_id": str(user.id)},
    )

    avatar_url = derp.storage.get_url(bucket="avatars", key=key)
    await derp.auth.update_user(user_id=user.id, avatar_url=avatar_url)

    return AvatarUploadResponse(avatar_url=avatar_url)


@router.get(
    "/workspaces/{workspace_id}/users/search",
    response_model=list[UserPublicResponse],
)
async def search_workspace_users(
    workspace_id: uuid.UUID,
    _member: OrgMemberInfo = Depends(get_workspace_member),
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
    q: str | None = Query(default=None, min_length=1, max_length=100),
    limit: int = Query(ge=1, le=50, default=20),
) -> list[UserPublicResponse]:
    """Search users within a workspace."""
    members = await derp.auth.list_org_members(workspace_id)
    member_ids = [str(m.user_id) for m in members]

    if not member_ids:
        return []

    query = (
        derp.db.select(User)
        .where(
            (User.c.id.in_(member_ids))
            & (User.c.id != str(user.id))
            & (User.c.is_active == True)  # noqa: E712
        )
        .limit(limit)
    )

    if q:
        query = query.where(
            (User.c.email.ilike(f"%{q}%"))
            | (User.c.username.ilike(f"%{q}%"))
            | (User.c.display_name.ilike(f"%{q}%"))
        )

    users = await query.execute()
    return [UserPublicResponse.model_validate(u) for u in users]
