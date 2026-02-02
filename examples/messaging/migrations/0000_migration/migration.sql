-- Migration: migration
-- Version: 0000
-- Generated at: 2026-02-02T01:03:41.607628+00:00
-- Previous: none

CREATE TYPE "auth_provider" AS ENUM ('email', 'magic_link', 'google', 'github');

CREATE TABLE "auth_magic_links" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "email" VARCHAR(255) NOT NULL,
    "token" VARCHAR(255) NOT NULL UNIQUE,
    "used" BOOLEAN NOT NULL DEFAULT FALSE,
    "expires_at" TIMESTAMP WITH TIME ZONE NOT NULL,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "auth_magic_links_token_unique" UNIQUE ("token")
);

CREATE INDEX "idx_auth_magic_links_email" ON "auth_magic_links" ("email");

CREATE INDEX "idx_auth_magic_links_token" ON "auth_magic_links" ("token");

CREATE TABLE "users" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "email" VARCHAR(255) NOT NULL UNIQUE,
    "email_confirmed_at" TIMESTAMP WITH TIME ZONE,
    "encrypted_password" TEXT,
    "provider" AUTH_PROVIDER NOT NULL,
    "is_active" BOOLEAN NOT NULL DEFAULT TRUE,
    "is_superuser" BOOLEAN NOT NULL DEFAULT FALSE,
    "recovery_token" VARCHAR(255),
    "recovery_sent_at" TIMESTAMP WITH TIME ZONE,
    "confirmation_token" VARCHAR(255),
    "confirmation_sent_at" TIMESTAMP WITH TIME ZONE,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "last_sign_in_at" TIMESTAMP WITH TIME ZONE,
    "display_name" VARCHAR(100),
    "avatar_url" VARCHAR(512),
    "bio" TEXT,
    CONSTRAINT "users_email_unique" UNIQUE ("email")
);

CREATE INDEX "idx_users_email" ON "users" ("email");

CREATE TABLE "auth_sessions" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "user_id" UUID NOT NULL,
    "user_agent" TEXT,
    "ip_address" VARCHAR(45),
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    "not_after" TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT "auth_sessions_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_auth_sessions_user_id" ON "auth_sessions" ("user_id");

CREATE TABLE "auth_refresh_tokens" (
    "id" SERIAL PRIMARY KEY,
    "session_id" UUID NOT NULL,
    "token" VARCHAR(255) NOT NULL UNIQUE,
    "revoked" BOOLEAN NOT NULL DEFAULT FALSE,
    "parent" VARCHAR(255),
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "auth_refresh_tokens_token_unique" UNIQUE ("token"),
    CONSTRAINT "auth_refresh_tokens_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "auth_sessions"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_auth_refresh_tokens_session_id" ON "auth_refresh_tokens" ("session_id");

CREATE INDEX "idx_auth_refresh_tokens_token" ON "auth_refresh_tokens" ("token");

CREATE TABLE "conversations" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "user1_id" UUID NOT NULL,
    "user2_id" UUID NOT NULL,
    "last_message_at" TIMESTAMP WITH TIME ZONE,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "conversations_user1_id_fkey" FOREIGN KEY ("user1_id") REFERENCES "users"("id") ON DELETE CASCADE,
    CONSTRAINT "conversations_user2_id_fkey" FOREIGN KEY ("user2_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_conversations_user1_id" ON "conversations" ("user1_id");

CREATE INDEX "idx_conversations_user2_id" ON "conversations" ("user2_id");

CREATE TABLE "messages" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "conversation_id" UUID NOT NULL,
    "sender_id" UUID NOT NULL,
    "content" TEXT NOT NULL,
    "read_at" TIMESTAMP WITH TIME ZONE,
    "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT "messages_conversation_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE CASCADE,
    CONSTRAINT "messages_sender_id_fkey" FOREIGN KEY ("sender_id") REFERENCES "users"("id") ON DELETE CASCADE
);

CREATE INDEX "idx_messages_conversation_id" ON "messages" ("conversation_id");