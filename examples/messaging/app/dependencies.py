"""FastAPI dependencies."""

from __future__ import annotations

import uuid

from fastapi import Depends, HTTPException, Request, status

from derp import DerpClient
from derp.auth.models import OrgMemberInfo, UserInfo


def get_derp(request: Request) -> DerpClient:
    """Get DerpClient from app state."""
    return request.app.state.derp_client


async def get_current_user(
    request: Request, derp: DerpClient = Depends(get_derp)
) -> UserInfo:
    """Get the current authenticated user from the request."""
    session = await derp.auth.authenticate(request)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await derp.auth.get_user(session.user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def get_workspace_member(
    workspace_id: uuid.UUID,
    user: UserInfo = Depends(get_current_user),
    derp: DerpClient = Depends(get_derp),
) -> OrgMemberInfo:
    """Verify the current user is a member of the workspace."""
    member = await derp.auth.get_org_member(org_id=workspace_id, user_id=user.id)
    if member is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not a member of this workspace",
        )
    return member
