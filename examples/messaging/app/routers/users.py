"""User profile endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, status

from app.config import settings
from app.dependencies import get_current_user, get_derp
from app.models import User
from app.schemas import (
    AvatarUploadResponse,
    UserProfileUpdateRequest,
    UserPublicResponse,
)
from derp import DerpClient

router = APIRouter(prefix="/users", tags=["users"])


@router.get("", response_model=list[UserPublicResponse])
async def list_users(
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
    search: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[UserPublicResponse]:
    """List users, optionally filtered by search query."""
    query = (
        derp.db.select(User)
        .where(User.c.is_active == True)  # noqa: E712
        .where(User.c.id != str(user.id))
        .order_by(User.c.created_at, asc=False)
        .limit(limit)
        .offset(offset)
    )

    if search:
        query = query.where(
            (User.c.email.ilike(f"%{search}%"))
            | (User.c.username.ilike(f"%{search}%"))
        )

    users = await query.execute()

    return [UserPublicResponse.model_validate(u) for u in users]


@router.get("/me", response_model=UserPublicResponse)
async def get_current_user_profile(
    user: User = Depends(get_current_user),
) -> UserPublicResponse:
    """Get current user's public profile."""
    return UserPublicResponse.model_validate(user)


@router.patch("/me", response_model=UserPublicResponse)
async def update_current_user_profile(
    data: UserProfileUpdateRequest,
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
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
    user: User = Depends(get_current_user),
    derp: DerpClient[User] = Depends(get_derp),
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
    key = f"avatars/{user.id}/{uuid.uuid4()}.{extension}"

    # Delete old avatar if exists
    if user.avatar_url:
        old_key = user.avatar_url.split(f"{settings.storage_bucket}/")[-1]
        try:
            await derp.storage.delete_file(bucket=settings.storage_bucket, key=old_key)
        except Exception:
            pass

    await derp.storage.upload_file(
        bucket=settings.storage_bucket,
        key=key,
        data=content,
        content_type=file.content_type,
        metadata={"user_id": str(user.id)},
    )

    avatar_url = f"{settings.storage_endpoint_url}/{settings.storage_bucket}/{key}"
    await derp.auth.update_user(user_id=user.id, avatar_url=avatar_url)

    return AvatarUploadResponse(avatar_url=avatar_url)


@router.get("/{user_id}", response_model=UserPublicResponse)
async def get_user_profile(
    user_id: uuid.UUID,
    derp: DerpClient[User] = Depends(get_derp),
) -> UserPublicResponse:
    """Get a user's public profile."""
    user = await derp.auth.get_user(user_id)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return UserPublicResponse.model_validate(user)
