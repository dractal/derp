"""Tests for organization support in the auth module."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from derp.auth.exceptions import OrgAlreadyExistsError, OrgMemberExistsError
from derp.auth.jwt import decode_token
from derp.derp_client import DerpClient
from tests.conftest import bearer_request


async def _create_user(derp: DerpClient, email: str, mock_smtp: AsyncMock) -> str:
    """Helper to create a user and return their ID."""
    result = await derp.auth.sign_up(
        email=email,
        password="password123",
        confirmation_url="http://localhost:3000/auth/confirm",
    )
    assert result is not None
    return result.user.id


class TestCreateOrg:
    """Tests for organization creation."""

    async def test_create_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        org = await derp.auth.create_org(
            name="Acme Corp",
            slug="acme-corp",
            creator_id=user_id,
        )

        assert org.name == "Acme Corp"
        assert org.slug == "acme-corp"
        assert org.id is not None
        assert org.created_at is not None
        assert org.updated_at is not None

    async def test_create_org_adds_creator_as_owner(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        org = await derp.auth.create_org(
            name="Acme Corp",
            slug="acme-corp",
            creator_id=user_id,
        )

        member = await derp.auth.get_org_member(org_id=org.id, user_id=user_id)
        assert member is not None
        assert member.role == "owner"

    async def test_create_org_duplicate_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        await derp.auth.create_org(
            name="Acme Corp",
            slug="acme",
            creator_id=user_id,
        )

        with pytest.raises(OrgAlreadyExistsError):
            await derp.auth.create_org(
                name="Different Name",
                slug="acme",
                creator_id=user_id,
            )


class TestGetOrg:
    """Tests for getting organizations."""

    async def test_get_org_by_id(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )

        fetched = await derp.auth.get_org(org.id)
        assert fetched is not None
        assert fetched.id == org.id
        assert fetched.name == "Acme Corp"
        assert fetched.slug == "acme"

    async def test_get_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.get_org("00000000-0000-0000-0000-000000000000")
        assert result is None

    async def test_get_org_by_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )

        fetched = await derp.auth.get_org_by_slug("acme")
        assert fetched is not None
        assert fetched.id == org.id

    async def test_get_org_by_slug_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.get_org_by_slug("nonexistent")
        assert result is None


class TestUpdateOrg:
    """Tests for updating organizations."""

    async def test_update_org_name(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )

        updated = await derp.auth.update_org(org_id=org.id, name="New Name")
        assert updated is not None
        assert updated.name == "New Name"
        assert updated.slug == "acme"

    async def test_update_org_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )

        updated = await derp.auth.update_org(org_id=org.id, slug="new-slug")
        assert updated is not None
        assert updated.slug == "new-slug"
        assert updated.name == "Acme Corp"

    async def test_update_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.update_org(
            org_id="00000000-0000-0000-0000-000000000000",
            name="New Name",
        )
        assert result is None


class TestDeleteOrg:
    """Tests for deleting organizations."""

    async def test_delete_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )

        await derp.auth.delete_org(org.id)

        result = await derp.auth.get_org(org.id)
        assert result is None

    async def test_delete_org_cascades_members(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )

        await derp.auth.delete_org(org.id)

        members = await derp.auth.list_org_members(org.id)
        assert members == []

    async def test_delete_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.delete_org("00000000-0000-0000-0000-000000000000")
        assert result is False


class TestListOrgs:
    """Tests for listing organizations."""

    async def test_list_all_orgs(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        await derp.auth.create_org(name="Org A", slug="org-a", creator_id=user_id)
        await derp.auth.create_org(name="Org B", slug="org-b", creator_id=user_id)

        orgs = await derp.auth.list_orgs()
        assert len(orgs) >= 2

    async def test_list_user_orgs(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_a = await _create_user(derp, "a@example.com", mock_smtp)
        user_b = await _create_user(derp, "b@example.com", mock_smtp)

        await derp.auth.create_org(name="Org A", slug="org-a", creator_id=user_a)
        await derp.auth.create_org(name="Org B", slug="org-b", creator_id=user_b)

        orgs_a = await derp.auth.list_orgs(user_id=user_a)
        assert len(orgs_a) == 1
        assert orgs_a[0].name == "Org A"

    async def test_list_orgs_with_pagination(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        await derp.auth.create_org(name="Org A", slug="org-a", creator_id=user_id)
        await derp.auth.create_org(name="Org B", slug="org-b", creator_id=user_id)

        orgs = await derp.auth.list_orgs(limit=1)
        assert len(orgs) == 1


class TestOrgMembers:
    """Tests for organization membership."""

    async def test_add_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        member = await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        assert member.role == "member"
        assert member.user_id == user_id
        assert member.org_id == org.id

    async def test_add_member_with_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        member = await derp.auth.add_org_member(
            org_id=org.id, user_id=user_id, role="admin"
        )
        assert member.role == "admin"

    async def test_add_member_already_exists(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        with pytest.raises(OrgMemberExistsError):
            await derp.auth.add_org_member(org_id=org.id, user_id=user_id)

    async def test_update_member_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        updated = await derp.auth.update_org_member(
            org_id=org.id, user_id=user_id, role="admin"
        )
        assert updated is not None
        assert updated.role == "admin"

    async def test_update_member_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        result = await derp.auth.update_org_member(
            org_id=org.id,
            user_id="00000000-0000-0000-0000-000000000000",
            role="admin",
        )
        assert result is None

    async def test_remove_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        await derp.auth.remove_org_member(org_id=org.id, user_id=user_id)

        member = await derp.auth.get_org_member(org_id=org.id, user_id=user_id)
        assert member is None

    async def test_remove_last_owner(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        result = await derp.auth.remove_org_member(org_id=org.id, user_id=owner_id)
        assert result is False

    async def test_list_members(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)

        members = await derp.auth.list_org_members(org.id)
        assert len(members) == 2

    async def test_get_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        member = await derp.auth.get_org_member(org_id=org.id, user_id=owner_id)
        assert member is not None
        assert member.role == "owner"

    async def test_get_member_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)

        member = await derp.auth.get_org_member(
            org_id=org.id,
            user_id="00000000-0000-0000-0000-000000000000",
        )
        assert member is None


class TestOrgSessionContext:
    """Tests for organization session context."""

    async def test_set_active_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None

        new_tokens = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )

        # Verify new token carries org context
        assert new_tokens is not None
        assert derp.config.auth is not None
        assert derp.config.auth.native is not None
        payload = decode_token(
            derp.config.auth.native.jwt,
            new_tokens.access_token,
        )
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra["org_id"] == org.id
        assert payload.extra["org_role"] == "owner"

    async def test_set_active_org_not_member(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        sign_up_result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert sign_up_result is not None
        # Create org as a different user
        other_id = await _create_user(derp, "other@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=other_id)

        session = await derp.auth.authenticate(
            bearer_request(sign_up_result.tokens.access_token)
        )
        assert session is not None

        set_org_result = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )
        assert set_org_result is None

    async def test_clear_active_org(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None

        # Set org then clear it
        await derp.auth.set_active_org(session_id=session.session_id, org_id=org.id)
        new_tokens = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=None
        )

        assert derp.config.auth is not None
        assert derp.config.auth.native is not None
        assert new_tokens is not None
        payload = decode_token(
            derp.config.auth.native.jwt,
            new_tokens.access_token,
        )
        assert payload is not None
        assert payload.extra is not None
        assert payload.extra.get("org_id") is None
        assert payload.extra.get("org_role") is None

    async def test_authenticate_with_org_context(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None

        new_tokens = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )

        # Authenticate with the new token
        assert new_tokens is not None
        org_session = await derp.auth.authenticate(
            bearer_request(new_tokens.access_token)
        )
        assert org_session is not None
        assert org_session.org_id == org.id
        assert org_session.org_role == "owner"

    async def test_authenticate_without_org_context(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None
        assert session.org_id is None
        assert session.org_role is None


class TestOrgAuthorization:
    """Tests for organization-level authorization."""

    async def test_is_org_authorized_correct_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None

        new_tokens = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )
        assert new_tokens is not None
        org_session = await derp.auth.authenticate(
            bearer_request(new_tokens.access_token)
        )
        assert org_session is not None

        assert derp.auth.is_org_authorized(org_session, org.id, "owner", "admin")

    async def test_is_org_authorized_wrong_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None

        new_tokens = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )
        assert new_tokens is not None
        org_session = await derp.auth.authenticate(
            bearer_request(new_tokens.access_token)
        )
        assert org_session is not None

        # User is owner, not admin-only
        assert not derp.auth.is_org_authorized(org_session, org.id, "admin")

    async def test_is_org_authorized_wrong_org(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        result = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )

        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None

        new_tokens = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )
        assert new_tokens is not None
        org_session = await derp.auth.authenticate(
            bearer_request(new_tokens.access_token)
        )
        assert org_session is not None

        assert not derp.auth.is_org_authorized(
            org_session,
            "00000000-0000-0000-0000-000000000000",
            "owner",
        )
