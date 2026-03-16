-- Migration: migration
-- Version: 0002
-- Generated at: 2026-03-14T18:26:12.344331+00:00
-- Previous: 0001

-- Drop old messaging tables (incompatible with new schema)
DROP TABLE IF EXISTS "messages" CASCADE;
DROP TABLE IF EXISTS "conversations" CASCADE;

-- Add missing columns to users
ALTER TABLE "users" DROP COLUMN IF EXISTS "bio";
ALTER TABLE "users" ADD COLUMN "display_name" VARCHAR(255);
ALTER TABLE "users" ADD COLUMN "role" VARCHAR(50) NOT NULL DEFAULT 'default';

-- Add org support to sessions
ALTER TABLE "auth_sessions" ADD COLUMN "org_id" UUID;
ALTER TABLE "auth_sessions" ADD COLUMN "role" VARCHAR(50) NOT NULL DEFAULT 'default';

-- Organizations (for workspaces)
CREATE TABLE "organizations" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" VARCHAR(255) NOT NULL,
    "slug" VARCHAR(255) NOT NULL UNIQUE,
    "metadata" TEXT,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "organizations_slug_unique" UNIQUE ("slug")
);

CREATE INDEX "idx_organizations_slug" ON "organizations" ("slug");

CREATE TABLE "org_members" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "org_id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "role" VARCHAR(50) NOT NULL DEFAULT 'member',
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "org_members_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
    CONSTRAINT "org_members_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE,
    CONSTRAINT "org_members_org_id_user_id_unique" UNIQUE ("org_id", "user_id")
);

CREATE INDEX "idx_org_members_org_id" ON "org_members" ("org_id");
CREATE INDEX "idx_org_members_user_id" ON "org_members" ("user_id");

-- Channels
CREATE TABLE "channels" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "workspace_id" UUID NOT NULL,
    "name" VARCHAR(80) NOT NULL,
    "topic" TEXT,
    "is_private" BOOLEAN NOT NULL DEFAULT false,
    "is_dm" BOOLEAN NOT NULL DEFAULT false,
    "created_by" UUID NOT NULL,
    "last_message_at" TIMESTAMP WITH TIME ZONE,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "channels_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_channels_workspace_id" ON "channels" ("workspace_id");

CREATE TABLE "channel_members" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "channel_id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "joined_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "channel_members_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE,
    CONSTRAINT "channel_members_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_channel_members_channel_id" ON "channel_members" ("channel_id");
CREATE INDEX "idx_channel_members_user_id" ON "channel_members" ("user_id");

-- Messages (new schema: belongs to channels)
CREATE TABLE "messages" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "channel_id" UUID NOT NULL,
    "sender_id" UUID NOT NULL,
    "content" TEXT NOT NULL,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "edited_at" TIMESTAMP WITH TIME ZONE,
    CONSTRAINT "messages_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE,
    CONSTRAINT "messages_sender_id_fkey" FOREIGN KEY ("sender_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_messages_channel_id" ON "messages" ("channel_id");
