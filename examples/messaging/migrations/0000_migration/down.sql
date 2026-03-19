-- Rollback: migration
-- Version: 0000
-- Generated at: 2026-03-19T07:05:37.289074+00:00

DROP TYPE IF EXISTS "auth_provider" CASCADE;

DROP TABLE IF EXISTS "auth_sessions" CASCADE;

DROP TABLE IF EXISTS "messages" CASCADE;

DROP TABLE IF EXISTS "channels" CASCADE;

DROP TABLE IF EXISTS "organizations" CASCADE;

DROP TABLE IF EXISTS "channel_members" CASCADE;

DROP TABLE IF EXISTS "users" CASCADE;

DROP TABLE IF EXISTS "org_members" CASCADE;