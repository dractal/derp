"""Tests for the AI client and models."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from derp.ai import AIClient, ChatChunk, ChatResponse, Usage
from derp.ai.models import SSEDone, SSEEvent
from derp.config import AIConfig


@pytest.fixture
def ai_config() -> AIConfig:
    return AIConfig(api_key="sk-test-123")


@pytest.fixture
def ai_client(ai_config: AIConfig) -> AIClient:
    client = AIClient(ai_config)
    client._openai_client = MagicMock()
    return client


def _mock_completion(
    content: str = "hello",
    model: str = "gpt-4o-mini",
    role: str = "assistant",
    finish_reason: str = "stop",
    prompt_tokens: int = 10,
    completion_tokens: int = 5,
    total_tokens: int = 15,
) -> MagicMock:
    """Build a mock OpenAI ChatCompletion."""
    choice = MagicMock()
    choice.message.content = content
    choice.message.role = role
    choice.finish_reason = finish_reason

    usage = MagicMock()
    usage.prompt_tokens = prompt_tokens
    usage.completion_tokens = completion_tokens
    usage.total_tokens = total_tokens

    completion = MagicMock()
    completion.choices = [choice]
    completion.model = model
    completion.usage = usage
    return completion


def _mock_stream_chunks(
    deltas: list[str],
    *,
    model: str = "gpt-4o-mini",
    include_usage: bool = False,
) -> AsyncMock:
    """Build a mock async stream of ChatCompletionChunks."""

    async def _aiter():
        for text in deltas:
            chunk = MagicMock()
            chunk.model = model
            chunk.usage = None
            delta = MagicMock()
            delta.content = text
            delta.role = "assistant"
            choice = MagicMock(delta=delta)
            choice.finish_reason = None
            chunk.choices = [choice]
            yield chunk
        # final chunk with finish_reason
        final = MagicMock()
        final.model = model
        final.usage = None
        final_delta = MagicMock()
        final_delta.content = None
        final_delta.role = None
        final_choice = MagicMock(delta=final_delta)
        final_choice.finish_reason = "stop"
        final.choices = [final_choice]
        yield final
        # usage-only chunk (no choices) — arrives after finish
        usage_chunk = MagicMock()
        usage_chunk.choices = []
        if include_usage:
            usage_chunk.usage = MagicMock(
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
            )
        else:
            usage_chunk.usage = None
        yield usage_chunk

    return _aiter()


# ── AIClient init ────────────────────────────────────────────────


class TestAIClientInit:
    def test_creates_with_api_key(self, ai_config: AIConfig) -> None:
        client = AIClient(ai_config)
        assert client._openai_client.api_key == "sk-test-123"

    def test_creates_with_custom_base_url(self) -> None:
        config = AIConfig(
            api_key="sk-test",
            base_url="https://api.openrouter.ai/v1",
        )
        client = AIClient(config)
        assert str(client._openai_client.base_url) == "https://api.openrouter.ai/v1/"

    def test_default_base_url_is_openai(self, ai_config: AIConfig) -> None:
        client = AIClient(ai_config)
        assert "api.openai.com" in str(client._openai_client.base_url)


# ── chat ─────────────────────────────────────────────────────────


class TestChat:
    @pytest.mark.asyncio
    async def test_returns_chat_response(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_completion(content="hi there", model="gpt-4o")
        )

        result = await ai_client.chat(
            model="gpt-4o",
            messages=[{"role": "user", "content": "hello"}],
        )

        assert isinstance(result, ChatResponse)
        assert result.content == "hi there"
        assert result.model == "gpt-4o"
        assert result.role == "assistant"
        assert result.finish_reason == "stop"

    @pytest.mark.asyncio
    async def test_includes_usage(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_completion(
                prompt_tokens=20, completion_tokens=10, total_tokens=30
            )
        )

        result = await ai_client.chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
        )

        assert result.usage is not None
        assert result.usage.prompt_tokens == 20
        assert result.usage.completion_tokens == 10
        assert result.usage.total_tokens == 30

    @pytest.mark.asyncio
    async def test_forwards_kwargs(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_completion()
        )

        await ai_client.chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            temperature=0.5,
            max_tokens=100,
        )

        ai_client._openai_client.chat.completions.create.assert_awaited_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            temperature=0.5,
            max_tokens=100,
        )


# ── stream_chat ──────────────────────────────────────────────────


class TestStreamChat:
    @pytest.mark.asyncio
    async def test_yields_chat_chunks(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["hello", " world"])
        )

        chunks = []
        async for chunk in ai_client.stream_chat(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
        ):
            chunks.append(chunk)

        # 2 content chunks + 1 final (is_last)
        assert len(chunks) == 3
        assert chunks[0].delta == "hello"
        assert chunks[1].delta == " world"
        assert chunks[2].is_last is True
        assert chunks[2].finish_reason == "stop"

    @pytest.mark.asyncio
    async def test_first_chunk_is_marked(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["hi"])
        )

        chunks = [
            c
            async for c in ai_client.stream_chat(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "hi"}],
            )
        ]

        assert chunks[0].is_first is True
        assert chunks[0].model == "gpt-4o-mini"
        assert chunks[1].is_first is False

    @pytest.mark.asyncio
    async def test_model_propagated_to_chunks(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["x"], model="gpt-4o")
        )

        chunks = [
            c
            async for c in ai_client.stream_chat(
                model="gpt-4o",
                messages=[{"role": "user", "content": "hi"}],
            )
        ]

        assert all(c.model == "gpt-4o" for c in chunks)

    @pytest.mark.asyncio
    async def test_usage_on_last_chunk(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["hi"], include_usage=True)
        )

        chunks = [
            c
            async for c in ai_client.stream_chat(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "hi"}],
            )
        ]

        last = chunks[-1]
        assert last.is_last is True
        assert last.usage is not None
        assert last.usage.prompt_tokens == 10
        assert last.usage.completion_tokens == 5
        assert last.usage.total_tokens == 15

    @pytest.mark.asyncio
    async def test_last_chunk_without_usage(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["hi"], include_usage=False)
        )

        chunks = [
            c
            async for c in ai_client.stream_chat(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "hi"}],
            )
        ]

        last = chunks[-1]
        assert last.is_last is True
        assert last.usage is None


# ── Usage model ──────────────────────────────────────────────────


class TestUsage:
    def test_vercel_ai_json(self) -> None:
        usage = Usage(prompt_tokens=10, completion_tokens=5, total_tokens=15)
        assert usage.vercel_ai_json() == {
            "promptTokens": 10,
            "completionTokens": 5,
            "totalTokens": 15,
        }

    def test_tanstack_ai_json(self) -> None:
        usage = Usage(prompt_tokens=10, completion_tokens=5, total_tokens=15)
        assert usage.tanstack_ai_json() == {
            "promptTokens": 10,
            "completionTokens": 5,
            "totalTokens": 15,
        }


# ── ChatResponse model ──────────────────────────────────────────


class TestChatResponse:
    def test_vercel_ai_json_structure(self) -> None:
        resp = ChatResponse(
            content="hello",
            model="gpt-4o",
            usage=Usage(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        )
        events = resp.vercel_ai_json(message_id="msg-123")

        assert len(events) == 6
        assert events[0]["type"] == "start"
        assert events[0]["messageId"] == "msg-123"
        assert events[1]["type"] == "text-start"
        assert events[2] == {
            "type": "text-delta",
            "id": "text-1",
            "delta": "hello",
        }
        assert events[3]["type"] == "text-end"
        assert events[4]["type"] == "finish"
        assert events[4]["messageMetadata"]["finishReason"] == "stop"
        assert events[4]["messageMetadata"]["usage"] == {
            "promptTokens": 10,
            "completionTokens": 5,
            "totalTokens": 15,
        }
        assert isinstance(events[5], SSEDone)

    def test_vercel_ai_json_without_usage(self) -> None:
        resp = ChatResponse(content="hi", model="gpt-4o")
        events = resp.vercel_ai_json()
        finish = events[-2]  # before SSEDone
        assert "usage" not in finish["messageMetadata"]

    def test_vercel_ai_json_maps_finish_reason(self) -> None:
        resp = ChatResponse(content="", model="gpt-4o", finish_reason="tool_calls")
        events = resp.vercel_ai_json()
        assert events[-2]["messageMetadata"]["finishReason"] == "tool-calls"

    def test_tanstack_ai_json_structure(self) -> None:
        resp = ChatResponse(
            content="hello",
            model="gpt-4o",
            usage=Usage(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        )
        events = resp.tanstack_ai_json(run_id="run-123", message_id="msg-456")

        assert len(events) == 6
        assert events[0] == {
            "type": "RUN_STARTED",
            "runId": "run-123",
            "model": "gpt-4o",
            "timestamp": events[0]["timestamp"],
        }
        assert events[1]["type"] == "TEXT_MESSAGE_START"
        assert events[1]["messageId"] == "msg-456"
        assert events[1]["role"] == "assistant"
        assert events[2]["type"] == "TEXT_MESSAGE_CONTENT"
        assert events[2]["delta"] == "hello"
        assert events[3]["type"] == "TEXT_MESSAGE_END"
        assert events[4]["type"] == "RUN_FINISHED"
        assert events[4]["finishReason"] == "stop"
        assert events[4]["usage"] == {
            "promptTokens": 10,
            "completionTokens": 5,
            "totalTokens": 15,
        }
        assert isinstance(events[5], SSEDone)

    def test_tanstack_ai_json_without_usage(self) -> None:
        resp = ChatResponse(content="hi", model="gpt-4o")
        events = resp.tanstack_ai_json()
        assert "usage" not in events[-2]  # before SSEDone


# ── ChatChunk model ──────────────────────────────────────────────


class TestChatChunk:
    def test_vercel_ai_json_middle_chunk(self) -> None:
        chunk = ChatChunk(delta="hello")
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert len(events) == 1
        assert events[0] == {"type": "text-delta", "id": "text-1", "delta": "hello"}

    def test_vercel_ai_json_first_chunk(self) -> None:
        chunk = ChatChunk(delta="hi", is_first=True)
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert len(events) == 3
        assert events[0] == {"type": "start", "messageId": "msg-1"}
        assert events[1] == {"type": "text-start", "id": "text-1"}
        assert events[2] == {"type": "text-delta", "id": "text-1", "delta": "hi"}

    def test_vercel_ai_json_last_chunk(self) -> None:
        chunk = ChatChunk(delta="", is_last=True, finish_reason="stop")
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert len(events) == 4
        assert events[0] == {"type": "text-delta", "id": "text-1", "delta": ""}
        assert events[1] == {"type": "text-end", "id": "text-1"}
        assert events[2]["type"] == "finish"
        assert events[2]["messageMetadata"]["finishReason"] == "stop"
        assert isinstance(events[3], SSEDone)

    def test_vercel_ai_json_maps_finish_reason(self) -> None:
        chunk = ChatChunk(delta="", is_last=True, finish_reason="tool_calls")
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert events[-2]["messageMetadata"]["finishReason"] == "tool-calls"

    def test_tanstack_ai_json_middle_chunk(self) -> None:
        chunk = ChatChunk(delta="hello", model="gpt-4o")
        events = chunk.tanstack_ai_json(message_id="msg-1")
        assert len(events) == 1
        assert events[0]["type"] == "TEXT_MESSAGE_CONTENT"
        assert events[0]["delta"] == "hello"
        assert events[0]["model"] == "gpt-4o"
        assert "timestamp" in events[0]

    def test_tanstack_ai_json_first_chunk(self) -> None:
        chunk = ChatChunk(delta="hi", model="gpt-4o", is_first=True)
        events = chunk.tanstack_ai_json(message_id="msg-1", run_id="run-1")
        assert len(events) == 3
        assert events[0]["type"] == "RUN_STARTED"
        assert events[0]["runId"] == "run-1"
        assert events[1]["type"] == "TEXT_MESSAGE_START"
        assert events[1]["messageId"] == "msg-1"
        assert events[2]["type"] == "TEXT_MESSAGE_CONTENT"

    def test_tanstack_ai_json_last_chunk(self) -> None:
        chunk = ChatChunk(delta="", model="gpt-4o", is_last=True, finish_reason="stop")
        events = chunk.tanstack_ai_json(message_id="msg-1", run_id="run-1")
        assert len(events) == 4
        assert events[0]["type"] == "TEXT_MESSAGE_CONTENT"
        assert events[1]["type"] == "TEXT_MESSAGE_END"
        assert events[2]["type"] == "RUN_FINISHED"
        assert events[2]["finishReason"] == "stop"
        assert isinstance(events[3], SSEDone)

    def test_tanstack_ai_json_without_model(self) -> None:
        chunk = ChatChunk(delta="hi")
        events = chunk.tanstack_ai_json(message_id="msg-1")
        assert "model" not in events[0]


# ── SSEEvent / SSEDone ───────────────────────────────────────────


class TestSSEEvent:
    def test_behaves_like_dict(self) -> None:
        event = SSEEvent({"type": "start", "messageId": "msg-1"})
        assert event["type"] == "start"
        assert event["messageId"] == "msg-1"

    def test_dump_formats_as_sse(self) -> None:
        event = SSEEvent({"type": "start", "id": "1"})
        assert event.dump() == 'data: {"type":"start","id":"1"}\n\n'

    def test_done_dump(self) -> None:
        done = SSEDone()
        assert done.dump() == "data: [DONE]\n\n"

    def test_is_instance_of_dict(self) -> None:
        event = SSEEvent({"type": "test"})
        assert isinstance(event, dict)

    def test_streaming_usage(self) -> None:
        """Verify the intended usage pattern works end to end."""
        resp = ChatResponse(content="hi", model="gpt-4o")
        events = resp.vercel_ai_json()
        output = "".join(e.dump() for e in events)
        assert output.startswith("data: ")
        assert output.endswith("data: [DONE]\n\n")
