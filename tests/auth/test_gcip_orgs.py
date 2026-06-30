"""DB-backed tests for GCIP's derp-layered organizations.

GCIP has no native orgs, so derp owns the org graph in Postgres. Org mutations
are plain Postgres writes with no token side-effects; a session's active org is
carried by a derp-signed org-context credential minted by ``set_active_org``.
These tests run the CRUD against a real database with the GCIP admin HTTP mocked.
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from derp.auth.exceptions import (
    LastOwnerError,
    MemberAlreadyExistsError,
    OrgMemberNotFoundError,
    OrgMismatchError,
    OrgNotFoundError,
    OrgSlugConflictError,
    UserNotFoundError,
)
from derp.auth.gcip_client import GCIPAuthClient, _Connection
from derp.auth.models import AuthStatus
from derp.config import GCIPConfig
from derp.orm import DatabaseEngine

INVITEE_UID = "invitee-uid-1"
INVITEE_EMAIL = "invitee@example.com"
PROJECT_ID = "test-project"


def _resp(status_code: int = 200, body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.json = MagicMock(return_value=body or {})
    resp.text = json.dumps(body or {})
    return resp


@pytest.fixture(scope="module")
def rsa_keypair():
    pk = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return pk, pk.public_key()


def _request(rsa_keypair, *, sub: str, org_context: str | None = None) -> MagicMock:
    """Build a request carrying a signed GCIP ID token (and optional pointer)."""
    private_key, _ = rsa_keypair
    now = int(time.time())
    token = pyjwt.encode(
        {
            "iss": f"https://securetoken.google.com/{PROJECT_ID}",
            "aud": PROJECT_ID,
            "sub": sub,
            "user_id": sub,
            "iat": now,
            "exp": now + 3600,
            "email": f"{sub}@example.com",
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "k1"},
    )
    req = MagicMock()
    req.headers = {"Authorization": f"Bearer {token}"}
    if org_context is not None:
        req.headers["X-Org-Context"] = org_context
    return req


async def _invite_token(db: DatabaseEngine, invitation_id: str) -> str:
    """Read the invite's secret token from the DB (the API never returns it)."""
    [row] = await db.execute(
        "SELECT token FROM org_invitations WHERE id = $1", [invitation_id]
    )
    return row["token"]


async def _create_gcip_org_tables(db: DatabaseEngine) -> None:
    await db.execute("""
        CREATE TABLE IF NOT EXISTS organizations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(255) NOT NULL,
            slug VARCHAR(255) UNIQUE NOT NULL,
            metadata TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)
    # GCIP's org_members keys members by the IdP uid (a string), no users FK.
    await db.execute("""
        CREATE TABLE IF NOT EXISTS org_members (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'member',
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            UNIQUE (org_id, user_id)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS org_invitations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            email VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'member',
            state VARCHAR(20) NOT NULL DEFAULT 'pending',
            token VARCHAR(255) UNIQUE NOT NULL,
            expires_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
        )
    """)


@pytest.fixture
async def gcip_db(clean_database: str) -> AsyncGenerator[DatabaseEngine, None]:
    engine = DatabaseEngine(clean_database)
    await engine.connect()
    await _create_gcip_org_tables(engine)
    yield engine
    await engine.disconnect()


@pytest.fixture
def client(gcip_db: DatabaseEngine, rsa_keypair) -> GCIPAuthClient:
    """A GCIP client wired to the DB, with admin HTTP and JWKS mocked.

    The HTTP mock answers admin calls (``accounts:lookup`` / ``find_user``) with
    the invitee user record. The JWKS returns the test public key so
    ``authenticate`` can verify the Bearer ID tokens built by ``_request``.
    """
    _, public_key = rsa_keypair
    cfg = GCIPConfig(
        project_id=PROJECT_ID,
        public_api_key="key",
        service_account_json="{}",
        org_context_secret="test-org-context-secret-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    )
    c = GCIPAuthClient(cfg)
    c.set_db(gcip_db)
    http = AsyncMock()
    http.request = AsyncMock(
        return_value=_resp(
            200,
            {"users": [{"localId": INVITEE_UID, "email": INVITEE_EMAIL}]},
        )
    )
    jwks = MagicMock()
    jwks.get_signing_key = AsyncMock(return_value=public_key)
    c._conn = _Connection(
        http=http,
        jwks=jwks,
        client_email="sa@test.iam.gserviceaccount.com",
        private_key="unused",
        private_key_id="pk-1",
        token_uri="https://oauth2.googleapis.com/token",
        access_token="admin-token",
        token_expires_at=time.time() + 3600,
    )
    return c


class TestCreateOrg:
    async def test_creates_org_and_owner(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="uid-1")
        ).raise_for_status()
        assert org.slug == "acme"
        assert org.name == "Acme"

        members = await client.list_members(org_id=org.id)
        assert len(members.items) == 1
        assert members.items[0].user_id == "uid-1"
        assert members.items[0].role == "owner"

    async def test_duplicate_slug_conflict(self, client: GCIPAuthClient) -> None:
        (
            await client.create_org(name="Acme", slug="acme", creator_id="uid-1")
        ).raise_for_status()
        with pytest.raises(OrgSlugConflictError):
            (
                await client.create_org(
                    name="Acme Two", slug="acme", creator_id="uid-2"
                )
            ).raise_for_status()


class TestGetOrg:
    async def test_by_slug_and_id(self, client: GCIPAuthClient) -> None:
        created = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        by_slug = (await client.get_org(slug="acme")).raise_for_status()
        by_id = (await client.get_org(org_id=created.id)).raise_for_status()
        assert by_slug.id == by_id.id == created.id

    async def test_missing_raises(self, client: GCIPAuthClient) -> None:
        with pytest.raises(OrgNotFoundError):
            (await client.get_org(slug="nope")).raise_for_status()


class TestMembership:
    async def test_add_list_update_remove(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()

        member = (
            await client.add_member(org_id=org.id, user_id="u2", role="member")
        ).raise_for_status()
        assert member.role == "member"

        updated = (
            await client.update_member(org_id=org.id, user_id="u2", role="admin")
        ).raise_for_status()
        assert updated.role == "admin"

        members = await client.list_members(org_id=org.id)
        assert {m.user_id for m in members.items} == {"owner-1", "u2"}

        assert (
            await client.remove_member(org_id=org.id, user_id="u2")
        ).raise_for_status() is True
        members = await client.list_members(org_id=org.id)
        assert {m.user_id for m in members.items} == {"owner-1"}

    async def test_delete_user_cleans_up_memberships(
        self, client: GCIPAuthClient, gcip_db: DatabaseEngine
    ) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        (
            await client.add_member(org_id=org.id, user_id="u2", role="member")
        ).raise_for_status()

        # The admin HTTP is mocked to 200, so the provider delete "succeeds";
        # the membership row (no FK to cascade) must be removed by derp.
        assert await client.delete_user("u2") is True

        gone = await gcip_db.execute(
            "SELECT 1 FROM org_members WHERE user_id = $1", ["u2"]
        )
        assert gone == []
        # Other members are untouched.
        kept = await client.list_members(org_id=org.id)
        assert {m.user_id for m in kept.items} == {"owner-1"}

    async def test_addressing_by_slug_matches_by_id(
        self, client: GCIPAuthClient
    ) -> None:
        """Admin methods resolve slug or id to the same org."""
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        # Add by slug, observe by id — and vice versa.
        (
            await client.add_member(slug="acme", user_id="u2", role="member")
        ).raise_for_status()
        by_id = await client.list_members(org_id=org.id)
        by_slug = await client.list_members(slug="acme")
        assert {m.user_id for m in by_id.items} == {m.user_id for m in by_slug.items}
        assert "u2" in {m.user_id for m in by_slug.items}

    async def test_duplicate_member(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        (await client.add_member(org_id=org.id, user_id="u2")).raise_for_status()
        with pytest.raises(MemberAlreadyExistsError):
            (await client.add_member(org_id=org.id, user_id="u2")).raise_for_status()

    async def test_update_non_member(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        with pytest.raises(OrgMemberNotFoundError):
            (
                await client.update_member(org_id=org.id, user_id="ghost", role="admin")
            ).raise_for_status()

    async def test_cannot_remove_last_owner(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        result = await client.remove_member(org_id=org.id, user_id="owner-1")
        assert isinstance(result.error, LastOwnerError)

    async def test_remove_missing_returns_false(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        assert (
            await client.remove_member(org_id=org.id, user_id="ghost")
        ).value is False


class TestGetMemberRole:
    async def test_resolves_authoritatively(self, client: GCIPAuthClient) -> None:
        """The DB lookup resolves any membership (the separate-API path)."""
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        (
            await client.add_member(org_id=org.id, user_id="u2", role="admin")
        ).raise_for_status()

        assert await client.get_member_role(user_id="u1", org="acme") == "owner"
        assert await client.get_member_role(user_id="u2", org="acme") == "admin"
        assert await client.get_member_role(user_id="ghost", org="acme") is None
        assert await client.get_member_role(user_id="u1", org="nope") is None


class TestSetActiveOrg:
    async def test_by_id_and_slug_roundtrip(self, client, rsa_keypair) -> None:
        """set_active_org mints a pointer authenticate() can bind, by id or slug."""
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()

        by_id = (
            await client.set_active_org(session_id="u1", org_id=org.id)
        ).raise_for_status()
        by_slug = (
            await client.set_active_org(session_id="u1", slug="acme")
        ).raise_for_status()
        assert by_id.access_token
        # Same canonical org → identical signed pointer.
        assert by_id.access_token == by_slug.access_token
        # The pointer round-trips through authenticate, binding id/slug/role.
        session = await client.authenticate(
            _request(rsa_keypair, sub="u1", org_context=by_id.access_token)
        )
        assert (session.org_id, session.org_slug, session.org_role) == (
            org.id,
            "acme",
            "owner",
        )

    async def test_clear(self, client: GCIPAuthClient) -> None:
        tokens = (await client.set_active_org(session_id="u1")).raise_for_status()
        assert tokens.access_token == ""

    async def test_not_a_member(self, client: GCIPAuthClient) -> None:
        (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        result = await client.set_active_org(session_id="stranger", slug="acme")
        assert isinstance(result.error, OrgMemberNotFoundError)

    async def test_missing_org(self, client: GCIPAuthClient) -> None:
        result = await client.set_active_org(session_id="u1", slug="ghost")
        assert isinstance(result.error, OrgMemberNotFoundError)


class TestAuthenticateActiveOrg:
    """End-to-end: mint a pointer via set_active_org, then authenticate against
    the real DB. The role is resolved from Postgres on each request, so there is
    no staleness window and nothing to refresh."""

    async def test_binds_org_and_role_from_db(self, client, rsa_keypair) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        cred = (
            await client.set_active_org(session_id="u1", slug="acme")
        ).raise_for_status()
        session = await client.authenticate(
            _request(rsa_keypair, sub="u1", org_context=cred.access_token)
        )
        assert (session.org_id, session.org_slug, session.org_role) == (
            org.id,
            "acme",
            "owner",
        )
        assert client.require_org(session, "acme") is session
        with pytest.raises(OrgMismatchError):
            client.require_org(session, "other")

    async def test_role_is_fresh_after_demotion(self, client, rsa_keypair) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        (
            await client.add_member(org_id=org.id, user_id="u2", role="admin")
        ).raise_for_status()
        cred = (
            await client.set_active_org(session_id="u2", slug="acme")
        ).raise_for_status()
        # Demote in the DB *after* the pointer was minted — no re-mint.
        (
            await client.update_member(org_id=org.id, user_id="u2", role="viewer")
        ).raise_for_status()
        session = await client.authenticate(
            _request(rsa_keypair, sub="u2", org_context=cred.access_token)
        )
        assert session.org_role == "viewer"  # resolved fresh from Postgres

    async def test_removed_member_binds_no_org(self, client, rsa_keypair) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        (
            await client.add_member(org_id=org.id, user_id="u2", role="member")
        ).raise_for_status()
        cred = (
            await client.set_active_org(session_id="u2", slug="acme")
        ).raise_for_status()
        (await client.remove_member(org_id=org.id, user_id="u2")).raise_for_status()
        session = await client.authenticate(
            _request(rsa_keypair, sub="u2", org_context=cred.access_token)
        )
        assert session.org_id is None  # zero staleness — no expiry needed

    async def test_pointer_is_bound_to_user(self, client, rsa_keypair) -> None:
        """u1's pointer is ignored when presented by u2 — even though u2 is a
        member — because the HMAC binds the pointer to its user."""
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        (await client.add_member(org_id=org.id, user_id="u2")).raise_for_status()
        u1_cred = (
            await client.set_active_org(session_id="u1", slug="acme")
        ).raise_for_status()
        session = await client.authenticate(
            _request(rsa_keypair, sub="u2", org_context=u1_cred.access_token)
        )
        assert session.org_id is None

    async def test_cookie_pointer_ignored(self, client, rsa_keypair) -> None:
        (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        cred = (
            await client.set_active_org(session_id="u1", slug="acme")
        ).raise_for_status()
        req = _request(rsa_keypair, sub="u1")  # no X-Org-Context header
        req.cookies = {"derp_org_context": cred.access_token}
        session = await client.authenticate(req)
        assert session.org_id is None  # header-only


class TestListOrgs:
    async def test_scoped_to_member(self, client: GCIPAuthClient) -> None:
        a = (
            await client.create_org(name="A", slug="a", creator_id="u1")
        ).raise_for_status()
        (
            await client.create_org(name="B", slug="b", creator_id="u2")
        ).raise_for_status()
        (await client.add_member(org_id=a.id, user_id="u2")).raise_for_status()

        u2_orgs = await client.list_orgs(user_id="u2")
        assert {o.slug for o in u2_orgs.items} == {"a", "b"}
        u1_orgs = await client.list_orgs(user_id="u1")
        assert {o.slug for o in u1_orgs.items} == {"a"}


class TestUpdateOrg:
    async def test_slug_change_is_a_plain_write(self, client, rsa_keypair) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        updated = (
            await client.update_org(org_id=org.id, new_slug="acme-corp")
        ).raise_for_status()
        assert updated.slug == "acme-corp"
        # No token side-effects: the rename is a single Postgres write. A member
        # who switches now gets a pointer carrying the new slug.
        cred = (
            await client.set_active_org(session_id="u1", slug="acme-corp")
        ).raise_for_status()
        session = await client.authenticate(
            _request(rsa_keypair, sub="u1", org_context=cred.access_token)
        )
        assert (session.org_id, session.org_slug) == (org.id, "acme-corp")


class TestDeleteOrg:
    async def test_delete(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="u1")
        ).raise_for_status()
        assert await client.delete_org(org_id=org.id) is True
        with pytest.raises(OrgNotFoundError):
            (await client.get_org(org_id=org.id)).raise_for_status()

    async def test_delete_missing(self, client: GCIPAuthClient) -> None:
        import uuid

        assert await client.delete_org(org_id=str(uuid.uuid4())) is False


class TestInvitations:
    async def test_invite_list_revoke(self, client: GCIPAuthClient) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        inv = (
            await client.invite_to_org(
                org_id=org.id, email="new@example.com", role="member"
            )
        ).raise_for_status()
        assert inv.state == "pending"
        assert inv.expires_at is not None  # invites carry a TTL

        listed = await client.list_invitations(org_id=org.id)
        assert [i.id for i in listed.items] == [inv.id]

        assert await client.revoke_invitation(invitation_id=inv.id) is True
        listed = await client.list_invitations(org_id=org.id)
        assert listed.items[0].state == "revoked"

    async def test_accept_adds_member(
        self, client: GCIPAuthClient, gcip_db: DatabaseEngine
    ) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        inv = (
            await client.invite_to_org(org_id=org.id, email=INVITEE_EMAIL)
        ).raise_for_status()
        token = await _invite_token(gcip_db, inv.id)

        outcome = await client.accept_invitation(invitation_token=token)
        assert outcome.status is AuthStatus.COMPLETE
        assert outcome.identity is not None
        assert outcome.identity.id == INVITEE_UID
        # GCIP doesn't mint tokens — the invitee keeps their existing session.
        assert outcome.tokens is None

        members = await client.list_members(org_id=org.id)
        assert INVITEE_UID in {m.user_id for m in members.items}

        accepted = await client.list_invitations(org_id=org.id)
        assert accepted.items[0].state == "accepted"

    async def test_accept_invalid_token(self, client: GCIPAuthClient) -> None:
        outcome = await client.accept_invitation(invitation_token="bogus")
        assert outcome.status is AuthStatus.INVALID_TOKEN

    async def test_accept_expired_invitation(
        self, client: GCIPAuthClient, gcip_db: DatabaseEngine
    ) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        inv = (
            await client.invite_to_org(org_id=org.id, email=INVITEE_EMAIL)
        ).raise_for_status()
        token = await _invite_token(gcip_db, inv.id)
        await gcip_db.execute(
            "UPDATE org_invitations SET expires_at = now() - interval '1 hour' "
            "WHERE id = $1",
            [inv.id],
        )

        outcome = await client.accept_invitation(invitation_token=token)
        assert outcome.status is AuthStatus.INVALID_TOKEN
        # The expired invite is marked so, and no membership was added.
        listed = await client.list_invitations(org_id=org.id)
        assert listed.items[0].state == "expired"
        members = await client.list_members(org_id=org.id)
        assert INVITEE_UID not in {m.user_id for m in members.items}

    async def test_accept_unknown_invitee_raises(
        self, client: GCIPAuthClient, gcip_db: DatabaseEngine
    ) -> None:
        org = (
            await client.create_org(name="Acme", slug="acme", creator_id="owner-1")
        ).raise_for_status()
        inv = (
            await client.invite_to_org(org_id=org.id, email="ghost@example.com")
        ).raise_for_status()
        token = await _invite_token(gcip_db, inv.id)
        # Admin lookup returns no matching user for this email.
        client._conn.http.request = AsyncMock(  # type: ignore[union-attr]
            return_value=_resp(200, {"users": []})
        )
        with pytest.raises(UserNotFoundError):
            await client.accept_invitation(invitation_token=token)
