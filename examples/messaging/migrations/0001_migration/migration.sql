-- Migration: migration
-- Version: 0001
-- Generated at: 2026-02-21T21:29:11.413762+00:00
-- Previous: 0000

DROP TABLE IF EXISTS "auth_refresh_tokens" CASCADE;

DROP TABLE IF EXISTS "auth_magic_links" CASCADE;

ALTER TABLE "auth_sessions" ADD COLUMN "revoked" BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE "auth_sessions" ADD COLUMN "session_id" UUID NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE "auth_sessions" ADD COLUMN "token" VARCHAR(255) NOT NULL UNIQUE;

ALTER TABLE "auth_sessions" ADD CONSTRAINT "auth_sessions_token_unique" UNIQUE ("token");

CREATE INDEX "idx_auth_sessions_session_id" ON "auth_sessions" ("session_id");

CREATE INDEX "idx_auth_sessions_token" ON "auth_sessions" ("token");

ALTER TABLE "users" DROP COLUMN "confirmation_sent_at";

ALTER TABLE "users" DROP COLUMN "confirmation_token";

ALTER TABLE "users" DROP COLUMN "recovery_sent_at";

ALTER TABLE "users" DROP COLUMN "recovery_token";

ALTER TABLE "users" ADD COLUMN "provider_id" VARCHAR(255);