# Messaging Example

A complete messaging application built with FastAPI, Derp ORM, Derp Auth, and Derp Storage.

## Features

- User authentication (signup, signin, JWT tokens)
- User profiles with avatars (stored in S3-compatible storage)
- Direct messaging between users
- Conversation management with unread counts
- Simple web-based frontend

## Prerequisites

- Python 3.12+
- PostgreSQL 14+
- MinIO (or any S3-compatible storage) for avatars
- [Mailpit](https://mailpit.axllent.org/) (or any local SMTP server) for email confirmation (optional)

## Quick Start

### 1. Setup PostgreSQL Database

```bash
createdb messaging
```

### 2. Start MinIO

```bash
minio server ~/minio-data --console-address ":9001"
```

Then create the `avatars` bucket via http://localhost:9001 (login with minioadmin/minioadmin).

### 3. Start Mailpit (optional)

If email confirmation is enabled (`enable_confirmation = true` in `derp.toml`), you need a local SMTP server. Mailpit listens on port 1025 for SMTP and provides a web UI to view emails:

```bash
brew install mailpit
mailpit
```

- SMTP: localhost:1025
- Web UI: http://localhost:8025

To skip this, set `enable_confirmation = false` under `[auth]` in `derp.toml`.

### 4. Install Dependencies

```bash
cd examples/messaging
uv sync
```

### 5. Configure Environment

```bash
cp .env.example .env
# Edit .env if needed (defaults work for local setup)
```

### 6. Run Database Migrations

```bash
uv run derp generate
uv run derp migrate
```

### 7. Start the Server

```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 8. Open the App

- **Frontend**: http://localhost:8000
- API docs: http://localhost:8000/docs
- MinIO Console: http://localhost:9001

## API Endpoints

### Auth

| Method | Endpoint        | Description       |
| ------ | --------------- | ----------------- |
| POST   | `/auth/signup`  | Register new user |
| POST   | `/auth/signin`  | Sign in           |
| POST   | `/auth/signout` | Sign out          |
| POST   | `/auth/refresh` | Refresh token     |
| GET    | `/auth/user`    | Get current user  |

### Users

| Method | Endpoint           | Description          |
| ------ | ------------------ | -------------------- |
| GET    | `/users`           | List users           |
| GET    | `/users/me`        | Current user profile |
| PATCH  | `/users/me`        | Update profile       |
| POST   | `/users/me/avatar` | Upload avatar        |
| GET    | `/users/{id}`      | Get user profile     |

### Conversations

| Method | Endpoint                       | Description        |
| ------ | ------------------------------ | ------------------ |
| GET    | `/conversations`               | List conversations |
| POST   | `/conversations`               | Start conversation |
| GET    | `/conversations/{id}`          | Get messages       |
| POST   | `/conversations/{id}/messages` | Send message       |
| PATCH  | `/conversations/{id}/read`     | Mark as read       |

## Example Usage

### Register a User

```bash
curl -X POST http://localhost:8000/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email": "alice@example.com", "password": "password123"}'
```

### Sign In

```bash
curl -X POST http://localhost:8000/auth/signin \
  -H "Content-Type: application/json" \
  -d '{"email": "alice@example.com", "password": "password123"}'
```

### Start a Conversation

```bash
curl -X POST http://localhost:8000/conversations \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "<other_user_id>"}'
```

### Send a Message

```bash
curl -X POST http://localhost:8000/conversations/<conversation_id>/messages \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello!"}'
```

### Upload Avatar

```bash
curl -X POST http://localhost:8000/users/me/avatar \
  -H "Authorization: Bearer <access_token>" \
  -F "file=@avatar.jpg"
```

## Project Structure

```
examples/messaging/
├── app/
│   ├── main.py          # FastAPI app with lifespan
│   ├── config.py        # Environment configuration
│   ├── models.py        # ORM table definitions
│   ├── schemas.py       # Pydantic schemas
│   ├── dependencies.py  # FastAPI dependencies
│   └── routers/
│       ├── auth.py          # Auth endpoints
│       ├── users.py         # User endpoints
│       └── conversations.py # Messaging endpoints
├── static/
│   ├── index.html       # Frontend HTML
│   ├── styles.css       # Frontend styles
│   └── app.js           # Frontend JavaScript
├── schema/
│   └── tables.py        # Schema for migrations
├── migrations/          # Generated migrations
├── .env.example
├── derp.toml
└── pyproject.toml
```

## Cleanup

To remove the example data:

```bash
# Drop the database
dropdb messaging

# Remove MinIO data (stop server first)
rm -rf ~/minio-data/avatars
```
