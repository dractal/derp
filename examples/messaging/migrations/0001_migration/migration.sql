-- Migration: migration
-- Version: 0001
-- Generated at: 2026-02-02T01:18:21.611262+00:00
-- Previous: 0000

ALTER TABLE "users" ADD COLUMN "username" VARCHAR(100);

ALTER TABLE "users" DROP COLUMN "display_name";