"""Tests for the zero-IO authorization layer on ``BaseAuthClient``.

``active_role`` / ``require_org`` / ``require_role`` / ``has`` /
``require_permission`` read the single active org a verified token already
carries (``Session.org_id`` / ``org_slug`` / ``org_role`` / ``permissions``),
so they need no backend and no IO — exercised here against a stub client and
hand-built sessions.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from derp.auth.base import BaseAuthClient, MFAStatus, Session
from derp.auth.exceptions import ForbiddenError, OrgMismatchError


class _StubClient(BaseAuthClient):
    """Minimal concrete client — only the abstract method, no IO."""

    async def verify_token(self, token: str) -> Session | None:
        return None


def _session(
    *,
    org_id: str | None = None,
    org_slug: str | None = None,
    org_role: str | None = None,
    permissions: Sequence[str] = (),
) -> Session:
    now = datetime.now(UTC)
    return Session(
        user_id="u1",
        session_id="s1",
        tenant_id=None,
        org_id=org_id,
        org_role=org_role,
        roles=(),
        scopes=(),
        is_anonymous=False,
        mfa=MFAStatus(enrolled=False, satisfied=False, factor_types=()),
        issued_at=now,
        expires_at=now,
        claims={},
        org_slug=org_slug,
        permissions=permissions,
    )


@pytest.fixture
def client() -> _StubClient:
    return _StubClient()


@pytest.fixture
def session() -> Session:
    """A session bound to an active org (``acme``) with two permissions."""
    return _session(
        org_id="org-1",
        org_slug="acme",
        org_role="admin",
        permissions=("billing:manage", "members:read"),
    )


class TestActiveRole:
    def test_returns_active_org_role(
        self, client: _StubClient, session: Session
    ) -> None:
        assert client.active_role(session) == "admin"

    def test_none_when_unbound(self, client: _StubClient) -> None:
        assert client.active_role(_session()) is None


class TestRequireOrg:
    def test_returns_session_when_active_org_matches(
        self, client: _StubClient, session: Session
    ) -> None:
        bound = client.require_org(session, "acme")
        assert bound is session  # already bound — returned unchanged
        assert bound.org_id == "org-1"  # the stable id, for persistence/FKs
        assert bound.org_role == "admin"

    def test_raises_when_active_org_differs(
        self, client: _StubClient, session: Session
    ) -> None:
        with pytest.raises(OrgMismatchError):
            client.require_org(session, "beta")

    def test_raises_when_unbound(self, client: _StubClient) -> None:
        with pytest.raises(OrgMismatchError):
            client.require_org(_session(), "acme")


class TestRequireRole:
    def test_passes_for_matching_role(
        self, client: _StubClient, session: Session
    ) -> None:
        client.require_role(session, "admin")
        client.require_role(session, "owner", "admin")  # any-of

    def test_raises_for_insufficient_role(
        self, client: _StubClient, session: Session
    ) -> None:
        with pytest.raises(ForbiddenError):
            client.require_role(session, "owner")


class TestPermissions:
    def test_has_true_for_granted(self, client: _StubClient, session: Session) -> None:
        assert client.has(session, "billing:manage") is True

    def test_has_false_for_missing(self, client: _StubClient, session: Session) -> None:
        assert client.has(session, "billing:delete") is False

    def test_require_permission_passes(
        self, client: _StubClient, session: Session
    ) -> None:
        client.require_permission(session, "members:read")

    def test_require_permission_raises(
        self, client: _StubClient, session: Session
    ) -> None:
        with pytest.raises(ForbiddenError):
            client.require_permission(session, "members:write")


class TestUnboundSession:
    """A session with no active org behaves as forbidden everywhere — no crash."""

    def test_no_active_org(self, client: _StubClient) -> None:
        s = _session()
        assert s.org_id is None
        assert s.org_slug is None
        assert client.active_role(s) is None
        assert client.has(s, "anything") is False
        with pytest.raises(OrgMismatchError):
            client.require_org(s, "acme")
