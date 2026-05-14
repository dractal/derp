"""Tests for organization support in the auth module."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from derp.auth.exceptions import (
    LastOwnerError,
    MemberAlreadyExistsError,
    OrgMemberNotFoundError,
    OrgNotFoundError,
    OrgSlugConflictError,
)
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
        assert org is not None

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
        assert org is not None

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

        with pytest.raises(OrgSlugConflictError) as exc:
            await derp.auth.create_org(
                name="Different Name",
                slug="acme",
                creator_id=user_id,
            )
        assert exc.value.slug == "acme"


class TestGetOrg:
    """Tests for getting organizations."""

    async def test_get_org_by_id(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )
        assert org is not None

        fetched = await derp.auth.get_org(org_id=org.id)
        assert fetched is not None
        assert fetched.id == org.id
        assert fetched.name == "Acme Corp"
        assert fetched.slug == "acme"

    async def test_get_org_not_found(self, derp: DerpClient) -> None:
        with pytest.raises(OrgNotFoundError):
            await derp.auth.get_org(org_id="00000000-0000-0000-0000-000000000000")


class TestUpdateOrg:
    """Tests for updating organizations."""

    async def test_update_org_name(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )
        assert org is not None

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
        assert org is not None

        updated = await derp.auth.update_org(org_id=org.id, slug="new-slug")
        assert updated is not None
        assert updated.slug == "new-slug"
        assert updated.name == "Acme Corp"

    async def test_update_org_not_found(self, derp: DerpClient) -> None:
        with pytest.raises(OrgNotFoundError):
            await derp.auth.update_org(
                org_id="00000000-0000-0000-0000-000000000000",
                name="New Name",
            )


class TestDeleteOrg:
    """Tests for deleting organizations."""

    async def test_delete_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )
        assert org is not None

        await derp.auth.delete_org(org_id=org.id)

        with pytest.raises(OrgNotFoundError):
            await derp.auth.get_org(org_id=org.id)

    async def test_delete_org_cascades_members(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(
            name="Acme Corp", slug="acme", creator_id=user_id
        )
        assert org is not None

        await derp.auth.delete_org(org_id=org.id)

        # ``org_id=`` is passthrough — we don't re-fetch the org just to
        # check it exists. Membership rows are gone via FK cascade.
        members = await derp.auth.list_org_members(org_id=org.id)
        assert members == []

    async def test_delete_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.delete_org(
            org_id="00000000-0000-0000-0000-000000000000"
        )
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
        assert org is not None

        member = await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        assert member is not None
        assert member.role == "member"
        assert member.user_id == user_id
        assert member.org_id == org.id

    async def test_add_member_with_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        member = await derp.auth.add_org_member(
            org_id=org.id, user_id=user_id, role="admin"
        )
        assert member is not None
        assert member.role == "admin"

    async def test_add_member_already_exists(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        with pytest.raises(MemberAlreadyExistsError):
            await derp.auth.add_org_member(org_id=org.id, user_id=user_id)

    async def test_update_member_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

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
        assert org is not None

        with pytest.raises(OrgMemberNotFoundError):
            await derp.auth.update_org_member(
                org_id=org.id,
                user_id="00000000-0000-0000-0000-000000000000",
                role="admin",
            )

    async def test_remove_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)
        await derp.auth.remove_org_member(org_id=org.id, user_id=user_id)

        with pytest.raises(OrgMemberNotFoundError):
            await derp.auth.get_org_member(org_id=org.id, user_id=user_id)

    async def test_remove_last_owner(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        with pytest.raises(LastOwnerError):
            await derp.auth.remove_org_member(org_id=org.id, user_id=owner_id)

    async def test_list_members(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        await derp.auth.add_org_member(org_id=org.id, user_id=user_id)

        members = await derp.auth.list_org_members(org_id=org.id)
        assert len(members) == 2

    async def test_get_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        member = await derp.auth.get_org_member(org_id=org.id, user_id=owner_id)
        assert member is not None
        assert member.role == "owner"

    async def test_get_member_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        with pytest.raises(OrgMemberNotFoundError):
            await derp.auth.get_org_member(
                org_id=org.id,
                user_id="00000000-0000-0000-0000-000000000000",
            )


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
        assert org is not None

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
        assert org is not None

        session = await derp.auth.authenticate(
            bearer_request(sign_up_result.tokens.access_token)
        )
        assert session is not None

        with pytest.raises(OrgMemberNotFoundError):
            await derp.auth.set_active_org(session_id=session.session_id, org_id=org.id)

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
        assert org is not None

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
        assert org is not None

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
        assert org is not None

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
        assert org is not None

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
        assert org is not None

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


class TestAssertSameOrg:
    """Tenant-scoping helper — the recommended one-liner for endpoint guards."""

    async def _make_org_session(self, derp: DerpClient, mock_smtp: AsyncMock):
        """Sign up, create an org, set it active — return the resulting session."""
        result = await derp.auth.sign_up(
            email="owner@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        org = await derp.auth.create_org(
            name="Acme", slug="acme", creator_id=result.user.id
        )
        assert org is not None
        first_session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert first_session is not None
        new_tokens = await derp.auth.set_active_org(
            session_id=first_session.session_id, org_id=org.id
        )
        assert new_tokens is not None
        session = await derp.auth.authenticate(bearer_request(new_tokens.access_token))
        assert session is not None
        return session, org

    async def test_is_same_org_true_when_match(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        session, org = await self._make_org_session(derp, mock_smtp)
        # Same id type on both sides — apples-to-apples comparison.
        assert derp.auth.is_same_org(session, org.id) is True
        derp.auth.assert_same_org(session, org.id)  # does not raise

    async def test_is_same_org_false_when_mismatch(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        from derp.auth import OrgMismatchError

        session, _org = await self._make_org_session(derp, mock_smtp)
        other_org_id = "00000000-0000-0000-0000-000000000000"
        assert derp.auth.is_same_org(session, other_org_id) is False
        with pytest.raises(OrgMismatchError):
            derp.auth.assert_same_org(session, other_org_id)

    async def test_is_same_org_false_when_no_active_org(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        from derp.auth import OrgMismatchError

        result = await derp.auth.sign_up(
            email="owner@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert result is not None
        session = await derp.auth.authenticate(
            bearer_request(result.tokens.access_token)
        )
        assert session is not None
        assert session.org_id is None  # no active org set

        assert derp.auth.is_same_org(session, "any-id") is False
        with pytest.raises(OrgMismatchError):
            derp.auth.assert_same_org(session, "any-id")


# =============================================================================
# Slug-based qualification (org_id is optional — pass slug= instead)
# =============================================================================


class TestOrgMethodsBySlug:
    """Every org-touching method accepts ``slug=`` as an alternative to ``org_id=``."""

    async def test_get_org_by_slug_kwarg(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=user_id)
        assert org is not None

        result = await derp.auth.get_org(slug="acme")
        assert result is not None
        assert result.id == org.id

    async def test_get_org_by_slug_unknown(self, derp: DerpClient) -> None:
        with pytest.raises(OrgNotFoundError):
            await derp.auth.get_org(slug="never-existed")

    async def test_get_org_requires_exactly_one(self, derp: DerpClient) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            await derp.auth.get_org()
        with pytest.raises(ValueError, match="exactly one"):
            await derp.auth.get_org(org_id="x", slug="y")

    async def test_update_org_by_org_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """``update_org`` uses ``org_slug=`` for lookup (``slug=`` is the new value)."""
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=user_id)
        assert org is not None

        updated = await derp.auth.update_org(
            org_slug="acme", name="Acme Inc", slug="acme-inc"
        )
        assert updated is not None
        assert updated.name == "Acme Inc"
        assert updated.slug == "acme-inc"

    async def test_delete_org_by_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=user_id)
        assert org is not None

        assert await derp.auth.delete_org(slug="acme") is True
        with pytest.raises(OrgNotFoundError):
            await derp.auth.get_org(org_id=org.id)

    async def test_delete_org_by_unknown_slug(self, derp: DerpClient) -> None:
        assert await derp.auth.delete_org(slug="nope") is False

    async def test_member_methods_by_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Add / get / list / update / remove member, all keyed by slug."""
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        assert org is not None

        added = await derp.auth.add_org_member(slug="acme", user_id=user_id)
        assert added is not None
        assert added.user_id == user_id

        member = await derp.auth.get_org_member(slug="acme", user_id=user_id)
        assert member is not None
        assert member.role == "member"

        members = await derp.auth.list_org_members(slug="acme")
        assert len(members) == 2  # owner + new member

        promoted = await derp.auth.update_org_member(
            slug="acme", user_id=user_id, role="admin"
        )
        assert promoted is not None
        assert promoted.role == "admin"

        assert await derp.auth.remove_org_member(slug="acme", user_id=user_id) is True

    async def test_member_methods_unknown_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """Unknown slug → OrgNotFoundError for everything except remove (False)."""
        user_id = await _create_user(derp, "user@example.com", mock_smtp)

        with pytest.raises(OrgNotFoundError):
            await derp.auth.add_org_member(slug="nope", user_id=user_id)
        with pytest.raises(OrgNotFoundError):
            await derp.auth.get_org_member(slug="nope", user_id=user_id)
        with pytest.raises(OrgNotFoundError):
            await derp.auth.list_org_members(slug="nope")
        with pytest.raises(OrgNotFoundError):
            await derp.auth.update_org_member(
                slug="nope", user_id=user_id, role="admin"
            )
        # remove_org_member is the bool-returning sibling — quietly False.
        assert (
            await derp.auth.remove_org_member(slug="nope", user_id=user_id)
        ) is False

    async def test_add_member_requires_exactly_one(self, derp: DerpClient) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            await derp.auth.add_org_member(user_id="x")
        with pytest.raises(ValueError, match="exactly one"):
            await derp.auth.add_org_member(org_id="a", slug="b", user_id="x")
