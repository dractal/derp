-- Migration: migration
-- Version: 0000
-- Generated at: 2026-03-19T07:05:37.289074+00:00
-- Previous: none

CREATE TYPE "auth_provider" AS ENUM ('email', 'magic_link', 'google', 'github');

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

CREATE TABLE "users" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "email" VARCHAR(255) NOT NULL UNIQUE,
    "email_confirmed_at" TIMESTAMP WITH TIME ZONE,
    "encrypted_password" TEXT,
    "first_name" VARCHAR(255),
    "last_name" VARCHAR(255),
    "username" VARCHAR(100),
    "image_url" TEXT,
    "provider" AUTH_PROVIDER NOT NULL,
    "provider_id" VARCHAR(255),
    "is_active" BOOLEAN NOT NULL DEFAULT TRUE,
    "is_superuser" BOOLEAN NOT NULL DEFAULT FALSE,
    "role" VARCHAR(50) NOT NULL DEFAULT 'default',
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "last_sign_in_at" TIMESTAMP WITH TIME ZONE,
    "display_name" VARCHAR(255),
    "avatar_url" VARCHAR(512),
    CONSTRAINT "users_email_unique" UNIQUE ("email")
);

CREATE INDEX "idx_users_email" ON "users" ("email");

CREATE TABLE "auth_sessions" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "user_id" UUID NOT NULL,
    "session_id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "token" VARCHAR(255) NOT NULL UNIQUE,
    "role" VARCHAR(50) NOT NULL DEFAULT 'default',
    "revoked" BOOLEAN NOT NULL DEFAULT FALSE,
    "user_agent" TEXT,
    "ip_address" VARCHAR(45),
    "org_id" UUID,
    "not_after" TIMESTAMP WITH TIME ZONE NOT NULL,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "auth_sessions_token_unique" UNIQUE ("token"),
    CONSTRAINT "auth_sessions_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_auth_sessions_user_id" ON "auth_sessions" ("user_id");

CREATE INDEX "idx_auth_sessions_session_id" ON "auth_sessions" ("session_id");

CREATE INDEX "idx_auth_sessions_token" ON "auth_sessions" ("token");

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

CREATE TABLE "org_members" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "org_id" UUID NOT NULL,
    "user_id" UUID NOT NULL,
    "role" VARCHAR(50) NOT NULL DEFAULT 'member',
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "org_members_org_id_fkey" FOREIGN KEY ("org_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
    CONSTRAINT "org_members_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_org_members_org_id" ON "org_members" ("org_id");

CREATE INDEX "idx_org_members_user_id" ON "org_members" ("user_id");