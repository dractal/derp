"""Tests for organization support in the auth module."""

from __future__ import annotations

from unittest.mock import AsyncMock

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
    outcome = await derp.auth.sign_up(
        email=email,
        password="password123",
        confirmation_url="http://localhost:3000/auth/confirm",
    )
    assert outcome.identity is not None
    return outcome.identity.id


class TestCreateOrg:
    """Tests for organization creation."""

    async def test_create_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        org = (
            await derp.auth.create_org(
                name="Acme Corp",
                slug="acme-corp",
                creator_id=user_id,
            )
        ).raise_for_status()

        assert org.name == "Acme Corp"
        assert org.slug == "acme-corp"
        assert org.tenant_id is None
        assert org.id is not None
        assert org.created_at is not None
        assert org.updated_at is not None

    async def test_create_org_adds_creator_as_owner(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        org = (
            await derp.auth.create_org(
                name="Acme Corp",
                slug="acme-corp",
                creator_id=user_id,
            )
        ).raise_for_status()

        members = await derp.auth.list_members(org_id=org.id)
        creator = next(m for m in members.items if m.user_id == user_id)
        assert creator.role == "owner"

    async def test_create_org_duplicate_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        (
            await derp.auth.create_org(
                name="Acme Corp",
                slug="acme",
                creator_id=user_id,
            )
        ).raise_for_status()

        result = await derp.auth.create_org(
            name="Different Name",
            slug="acme",
            creator_id=user_id,
        )
        assert isinstance(result.error, OrgSlugConflictError)
        assert result.error.slug == "acme"


class TestGetOrg:
    """Tests for getting organizations."""

    async def test_get_org_by_id(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(
                name="Acme Corp", slug="acme", creator_id=user_id
            )
        ).raise_for_status()

        fetched = (await derp.auth.get_org(org_id=org.id)).raise_for_status()
        assert fetched.id == org.id
        assert fetched.name == "Acme Corp"
        assert fetched.slug == "acme"

    async def test_get_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.get_org(org_id="00000000-0000-0000-0000-000000000000")
        assert isinstance(result.error, OrgNotFoundError)


class TestUpdateOrg:
    """Tests for updating organizations."""

    async def test_update_org_name(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(
                name="Acme Corp", slug="acme", creator_id=user_id
            )
        ).raise_for_status()

        updated = (
            await derp.auth.update_org(org_id=org.id, name="New Name")
        ).raise_for_status()
        assert updated.name == "New Name"
        assert updated.slug == "acme"

    async def test_update_org_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(
                name="Acme Corp", slug="acme", creator_id=user_id
            )
        ).raise_for_status()

        updated = (
            await derp.auth.update_org(org_id=org.id, new_slug="new-slug")
        ).raise_for_status()
        assert updated.slug == "new-slug"
        assert updated.name == "Acme Corp"

    async def test_update_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.update_org(
            org_id="00000000-0000-0000-0000-000000000000",
            name="New Name",
        )
        assert isinstance(result.error, OrgNotFoundError)


class TestDeleteOrg:
    """Tests for deleting organizations."""

    async def test_delete_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(
                name="Acme Corp", slug="acme", creator_id=user_id
            )
        ).raise_for_status()

        await derp.auth.delete_org(org_id=org.id)

        result = await derp.auth.get_org(org_id=org.id)
        assert isinstance(result.error, OrgNotFoundError)

    async def test_delete_org_cascades_members(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(
                name="Acme Corp", slug="acme", creator_id=user_id
            )
        ).raise_for_status()

        await derp.auth.delete_org(org_id=org.id)

        # The org (and its memberships via FK cascade) is gone, so listing its
        # members yields an empty page.
        members = await derp.auth.list_members(org_id=org.id)
        assert members.items == []

    async def test_delete_org_not_found(self, derp: DerpClient) -> None:
        result = await derp.auth.delete_org(
            org_id="00000000-0000-0000-0000-000000000000"
        )
        assert result is False


class TestListOrgs:
    """Tests for listing organizations."""

    async def test_list_all_orgs(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        (
            await derp.auth.create_org(name="Org A", slug="org-a", creator_id=user_id)
        ).raise_for_status()
        (
            await derp.auth.create_org(name="Org B", slug="org-b", creator_id=user_id)
        ).raise_for_status()

        orgs = await derp.auth.list_orgs()
        assert len(orgs.items) >= 2

    async def test_list_user_orgs(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        user_a = await _create_user(derp, "a@example.com", mock_smtp)
        user_b = await _create_user(derp, "b@example.com", mock_smtp)

        (
            await derp.auth.create_org(name="Org A", slug="org-a", creator_id=user_a)
        ).raise_for_status()
        (
            await derp.auth.create_org(name="Org B", slug="org-b", creator_id=user_b)
        ).raise_for_status()

        orgs_a = await derp.auth.list_orgs(user_id=user_a)
        assert len(orgs_a.items) == 1
        assert orgs_a.items[0].name == "Org A"

    async def test_list_orgs_with_pagination(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)

        (
            await derp.auth.create_org(name="Org A", slug="org-a", creator_id=user_id)
        ).raise_for_status()
        (
            await derp.auth.create_org(name="Org B", slug="org-b", creator_id=user_id)
        ).raise_for_status()

        page = await derp.auth.list_orgs(limit=1)
        assert len(page.items) == 1
        assert page.has_more is True
        assert page.next_cursor is not None

        # Walk to the next page via the cursor.
        page2 = await derp.auth.list_orgs(limit=1, cursor=page.next_cursor)
        assert len(page2.items) == 1
        assert page2.items[0].id != page.items[0].id


class TestOrgMembers:
    """Tests for organization membership."""

    async def test_add_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        member = (
            await derp.auth.add_member(org_id=org.id, user_id=user_id)
        ).raise_for_status()
        assert member.role == "member"
        assert member.user_id == user_id
        assert member.org_id == org.id

    async def test_add_member_with_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        member = (
            await derp.auth.add_member(org_id=org.id, user_id=user_id, role="admin")
        ).raise_for_status()
        assert member.role == "admin"

    async def test_add_member_already_exists(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        (await derp.auth.add_member(org_id=org.id, user_id=user_id)).raise_for_status()
        result = await derp.auth.add_member(org_id=org.id, user_id=user_id)
        assert isinstance(result.error, MemberAlreadyExistsError)

    async def test_update_member_role(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        (await derp.auth.add_member(org_id=org.id, user_id=user_id)).raise_for_status()
        updated = (
            await derp.auth.update_member(org_id=org.id, user_id=user_id, role="admin")
        ).raise_for_status()
        assert updated.role == "admin"

    async def test_update_member_not_found(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        result = await derp.auth.update_member(
            org_id=org.id,
            user_id="00000000-0000-0000-0000-000000000000",
            role="admin",
        )
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_remove_member(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        (await derp.auth.add_member(org_id=org.id, user_id=user_id)).raise_for_status()
        removed = (
            await derp.auth.remove_member(org_id=org.id, user_id=user_id)
        ).raise_for_status()
        assert removed is True

        members = await derp.auth.list_members(org_id=org.id)
        assert all(m.user_id != user_id for m in members.items)

    async def test_remove_last_owner(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        result = await derp.auth.remove_member(org_id=org.id, user_id=owner_id)
        assert isinstance(result.error, LastOwnerError)

    async def test_list_members(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        (await derp.auth.add_member(org_id=org.id, user_id=user_id)).raise_for_status()

        members = await derp.auth.list_members(org_id=org.id)
        assert len(members.items) == 2


class TestOrgSessionContext:
    """Tests for organization session context."""

    async def test_set_active_org(self, derp: DerpClient, mock_smtp: AsyncMock) -> None:
        outcome = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert outcome.identity is not None
        assert outcome.tokens is not None
        org = (
            await derp.auth.create_org(
                name="Acme", slug="acme", creator_id=outcome.identity.id
            )
        ).raise_for_status()

        session = await derp.auth.authenticate(
            bearer_request(outcome.tokens.access_token)
        )
        assert session is not None

        new_tokens = (
            await derp.auth.set_active_org(session_id=session.session_id, org_id=org.id)
        ).raise_for_status()

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
        sign_up = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert sign_up.tokens is not None
        # Create org as a different user
        other_id = await _create_user(derp, "other@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=other_id)
        ).raise_for_status()

        session = await derp.auth.authenticate(
            bearer_request(sign_up.tokens.access_token)
        )
        assert session is not None

        result = await derp.auth.set_active_org(
            session_id=session.session_id, org_id=org.id
        )
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_clear_active_org(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        outcome = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert outcome.identity is not None
        assert outcome.tokens is not None
        org = (
            await derp.auth.create_org(
                name="Acme", slug="acme", creator_id=outcome.identity.id
            )
        ).raise_for_status()

        session = await derp.auth.authenticate(
            bearer_request(outcome.tokens.access_token)
        )
        assert session is not None

        # Set org then clear it
        (
            await derp.auth.set_active_org(session_id=session.session_id, org_id=org.id)
        ).raise_for_status()
        new_tokens = (
            await derp.auth.set_active_org(session_id=session.session_id)
        ).raise_for_status()

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
        outcome = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert outcome.identity is not None
        assert outcome.tokens is not None
        org = (
            await derp.auth.create_org(
                name="Acme", slug="acme", creator_id=outcome.identity.id
            )
        ).raise_for_status()

        session = await derp.auth.authenticate(
            bearer_request(outcome.tokens.access_token)
        )
        assert session is not None

        new_tokens = (
            await derp.auth.set_active_org(session_id=session.session_id, org_id=org.id)
        ).raise_for_status()

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
        outcome = await derp.auth.sign_up(
            email="user@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert outcome.tokens is not None

        session = await derp.auth.authenticate(
            bearer_request(outcome.tokens.access_token)
        )
        assert session is not None
        assert session.org_id is None
        assert session.org_role is None


class TestOrgScopedSession:
    """Org context surfaced on the verified ``Session``."""

    async def _make_org_session(self, derp: DerpClient, mock_smtp: AsyncMock):
        """Sign up, create an org, set it active — return the resulting session."""
        outcome = await derp.auth.sign_up(
            email="owner@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert outcome.identity is not None
        assert outcome.tokens is not None
        org = (
            await derp.auth.create_org(
                name="Acme", slug="acme", creator_id=outcome.identity.id
            )
        ).raise_for_status()
        first_session = await derp.auth.authenticate(
            bearer_request(outcome.tokens.access_token)
        )
        assert first_session is not None
        new_tokens = (
            await derp.auth.set_active_org(
                session_id=first_session.session_id, org_id=org.id
            )
        ).raise_for_status()
        assert new_tokens is not None
        session = await derp.auth.authenticate(bearer_request(new_tokens.access_token))
        assert session is not None
        return session, org

    async def test_session_carries_active_org(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        session, org = await self._make_org_session(derp, mock_smtp)
        assert session.org_id == org.id
        assert session.org_role == "owner"

    async def test_session_org_role_authorization(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        session, org = await self._make_org_session(derp, mock_smtp)
        # Owner is in the allowed set, but not an admin-only set.
        assert session.org_role in {"owner", "admin"}
        assert session.org_role != "admin"

    async def test_session_without_active_org(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        outcome = await derp.auth.sign_up(
            email="owner@example.com",
            password="password123",
            confirmation_url="http://localhost:3000/auth/confirm",
        )
        assert outcome.tokens is not None
        session = await derp.auth.authenticate(
            bearer_request(outcome.tokens.access_token)
        )
        assert session is not None
        assert session.org_id is None
        assert session.org_role is None


# =============================================================================
# Slug addressing — an op addressed by ``slug=`` matches the same org as ``org_id=``.
# =============================================================================


class TestOrgMethodsBySlug:
    """Org admin methods address an org by ``org_id=`` **or** ``slug=`` (one)."""

    async def test_get_org_by_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=user_id)
        ).raise_for_status()

        result = (await derp.auth.get_org(slug="acme")).raise_for_status()
        assert result.id == org.id

    async def test_get_org_by_slug_unknown(self, derp: DerpClient) -> None:
        result = await derp.auth.get_org(slug="never-existed")
        assert isinstance(result.error, OrgNotFoundError)

    async def test_add_member_by_slug_or_id(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """add_member resolves the org by slug just as it does by id."""
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        by_slug = await _create_user(derp, "by-slug@example.com", mock_smtp)
        by_id = await _create_user(derp, "by-id@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()

        # Address the org by its slug...
        member_via_slug = (
            await derp.auth.add_member(slug="acme", user_id=by_slug)
        ).raise_for_status()
        assert member_via_slug.org_id == org.id
        assert member_via_slug.user_id == by_slug

        # ...and by its id — both resolve to the same canonical org.
        member_via_id = (
            await derp.auth.add_member(org_id=org.id, user_id=by_id)
        ).raise_for_status()
        assert member_via_id.org_id == org.id

    async def test_list_members_by_slug_matches_by_id(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        """list_members addressed by slug returns the same rows as by id."""
        owner_id = await _create_user(derp, "owner@example.com", mock_smtp)
        user_id = await _create_user(derp, "user@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=owner_id)
        ).raise_for_status()
        (await derp.auth.add_member(org_id=org.id, user_id=user_id)).raise_for_status()

        by_slug = await derp.auth.list_members(slug="acme")
        by_id = await derp.auth.list_members(org_id=org.id)
        assert {m.user_id for m in by_slug.items} == {m.user_id for m in by_id.items}
        assert len(by_slug.items) == 2

    async def test_update_org_by_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=user_id)
        ).raise_for_status()

        updated = (
            await derp.auth.update_org(slug="acme", name="Renamed")
        ).raise_for_status()
        assert updated.id == org.id
        assert updated.name == "Renamed"
        assert updated.slug == "acme"

    async def test_delete_org_by_slug(
        self, derp: DerpClient, mock_smtp: AsyncMock
    ) -> None:
        user_id = await _create_user(derp, "creator@example.com", mock_smtp)
        org = (
            await derp.auth.create_org(name="Acme", slug="acme", creator_id=user_id)
        ).raise_for_status()

        assert await derp.auth.delete_org(slug="acme") is True
        result = await derp.auth.get_org(org_id=org.id)
        assert isinstance(result.error, OrgNotFoundError)
