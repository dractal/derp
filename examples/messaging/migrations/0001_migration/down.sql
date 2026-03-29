-- Rollback: migration
-- Version: 0001
-- Generated at: 2026-03-29T03:30:36.896181+00:00

CREATE TYPE "auth_provider" AS ENUM ('email', 'magic_link', 'google', 'github');

DROP INDEX IF EXISTS "idx_auth_sessions_session_id_revoked";

DROP INDEX IF EXISTS "uniq_org_members_org_id_user_id";