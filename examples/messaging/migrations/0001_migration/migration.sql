-- Migration: migration
-- Version: 0001
-- Generated at: 2026-03-29T03:30:36.896181+00:00
-- Previous: 0000

DROP TYPE IF EXISTS "auth_provider" CASCADE;

CREATE INDEX "idx_auth_sessions_session_id_revoked" ON "auth_sessions" ("session_id", "revoked");

CREATE UNIQUE INDEX "uniq_org_members_org_id_user_id" ON "org_members" ("org_id", "user_id");