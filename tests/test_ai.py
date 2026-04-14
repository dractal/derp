"""Tests for the AI client and models."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from derp.ai import (
    AIClient,
    ChatChunk,
    ChatResponse,
    JobState,
    JobStatus,
    Tool,
    ToolCall,
    Usage,
)
from derp.ai.exceptions import FalJobFailedError, FalMissingCredentialsError
from derp.ai.models import SSEDone, SSEEvent, ToolEventType, _snake_case
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
    tool_calls: list[Any] | None = None,
) -> MagicMock:
    """Build a mock OpenAI ChatCompletion."""
    choice = MagicMock()
    choice.message.content = content
    choice.message.role = role
    choice.message.tool_calls = tool_calls
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


def _mock_tool_call(
    tc_id: str = "call_123",
    name: str = "get_weather",
    arguments: str = '{"city":"London"}',
) -> MagicMock:
    """Build a mock OpenAI tool call object."""
    tc = MagicMock()
    tc.id = tc_id
    tc.function.name = name
    tc.function.arguments = arguments
    return tc


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
            delta.tool_calls = None
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
        final_delta.tool_calls = None
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


def _mock_stream_tool_call_chunks(
    tc_id: str = "call_abc",
    name: str = "get_weather",
    arg_fragments: list[str] | None = None,
    *,
    model: str = "gpt-4o-mini",
) -> AsyncMock:
    """Build a mock async stream with tool call deltas."""
    if arg_fragments is None:
        arg_fragments = ['{"city":', '"London"}']

    async def _aiter():
        # First chunk: tool call id + function name + first arg fragment
        first_tc = MagicMock()
        first_tc.index = 0
        first_tc.id = tc_id
        first_tc.function.name = name
        first_tc.function.arguments = arg_fragments[0] if arg_fragments else ""

        chunk0 = MagicMock()
        chunk0.model = model
        chunk0.usage = None
        delta0 = MagicMock()
        delta0.content = None
        delta0.role = "assistant"
        delta0.tool_calls = [first_tc]
        choice0 = MagicMock(delta=delta0)
        choice0.finish_reason = None
        chunk0.choices = [choice0]
        yield chunk0

        # Subsequent arg fragment chunks
        for frag in arg_fragments[1:]:
            tc_delta = MagicMock()
            tc_delta.index = 0
            tc_delta.id = None
            tc_delta.function.name = None
            tc_delta.function.arguments = frag

            chunk = MagicMock()
            chunk.model = model
            chunk.usage = None
            delta = MagicMock()
            delta.content = None
            delta.tool_calls = [tc_delta]
            choice = MagicMock(delta=delta)
            choice.finish_reason = None
            chunk.choices = [choice]
            yield chunk

        # Final chunk with finish_reason
        final = MagicMock()
        final.model = model
        final.usage = None
        final_delta = MagicMock()
        final_delta.content = None
        final_delta.tool_calls = None
        final_choice = MagicMock(delta=final_delta)
        final_choice.finish_reason = "tool_calls"
        final.choices = [final_choice]
        yield final

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


# ── Tool ─────────────────────────────────────────────────────────


class GetWeather(Tool):
    """Get the current weather for a city."""

    city: str
    unit: str = "celsius"

    async def run(self) -> dict[str, Any]:
        return {"temperature": 22, "unit": self.unit, "city": self.city}


class SendEmail(Tool):
    """Send an email to a recipient."""

    to: str
    body: str

    async def run(self) -> str:
        return f"Sent to {self.to}"


class StageRoom(Tool):
    """Stage a room with furniture."""

    design_style: str

    async def run(self, derp: Any, user_id: str) -> dict[str, Any]:
        return {
            "style": self.design_style,
            "user_id": user_id,
            "has_client": derp is not None,
        }


class TestSnakeCase:
    def test_simple(self) -> None:
        assert _snake_case("GetWeather") == "get_weather"

    def test_consecutive_caps(self) -> None:
        assert _snake_case("HTMLParser") == "html_parser"

    def test_single_word(self) -> None:
        assert _snake_case("Tool") == "tool"


class TestTool:
    def test_function_name(self) -> None:
        assert GetWeather.function_name() == "get_weather"
        assert SendEmail.function_name() == "send_email"

    def test_openai_schema(self) -> None:
        schema = GetWeather.openai_schema()
        assert schema["type"] == "function"
        assert schema["function"]["name"] == "get_weather"
        assert (
            schema["function"]["description"] == "Get the current weather for a city."
        )
        params = schema["function"]["parameters"]
        assert "city" in params["properties"]
        assert "unit" in params["properties"]
        assert params["required"] == ["city"]

    @pytest.mark.asyncio
    async def test_run(self) -> None:
        tool = GetWeather(city="London", unit="fahrenheit")
        result = await tool.run()
        assert result == {"temperature": 22, "unit": "fahrenheit", "city": "London"}


class TestToolCall:
    def test_attributes(self) -> None:
        tc = ToolCall(
            id="call_1",
            function_name="get_weather",
            arguments='{"city":"London"}',
        )
        assert tc.id == "call_1"
        assert tc.function_name == "get_weather"
        assert tc.arguments == '{"city":"London"}'
        assert tc.args is None

    @pytest.mark.asyncio
    async def test_run_with_parsed_args(self) -> None:
        tool_instance = GetWeather(city="Paris")
        tc = ToolCall(
            id="call_1",
            function_name="get_weather",
            arguments='{"city":"Paris"}',
            args=tool_instance,
        )
        result = await tc.run()
        assert result["city"] == "Paris"

    @pytest.mark.asyncio
    async def test_run_forwards_extra_args(self) -> None:
        tool_instance = StageRoom(design_style="modern")
        tc = ToolCall(
            id="call_1",
            function_name="stage_room",
            arguments='{"design_style":"modern"}',
            args=tool_instance,
        )
        fake_derp = object()
        result = await tc.run(fake_derp, "user-42")
        assert result == {
            "style": "modern",
            "user_id": "user-42",
            "has_client": True,
        }

    @pytest.mark.asyncio
    async def test_run_without_parsed_args_raises(self) -> None:
        tc = ToolCall(
            id="call_1",
            function_name="get_weather",
            arguments='{"city":"London"}',
        )
        with pytest.raises(TypeError, match="args were not parsed"):
            await tc.run()


# ── chat with tools ──────────────────────────────────────────────


class TestChatWithTools:
    @pytest.mark.asyncio
    async def test_passes_tool_schemas(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_completion(content="hi")
        )

        await ai_client.chat(
            model="gpt-4o",
            messages=[{"role": "user", "content": "hello"}],
            tools=[GetWeather],
        )

        call_kwargs = ai_client._openai_client.chat.completions.create.call_args
        assert "tools" in call_kwargs.kwargs
        schemas = call_kwargs.kwargs["tools"]
        assert len(schemas) == 1
        assert schemas[0]["function"]["name"] == "get_weather"

    @pytest.mark.asyncio
    async def test_parses_tool_calls(self, ai_client: AIClient) -> None:
        mock_tc = _mock_tool_call(
            tc_id="call_abc",
            name="get_weather",
            arguments='{"city":"London","unit":"celsius"}',
        )
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_completion(
                content="",
                finish_reason="tool_calls",
                tool_calls=[mock_tc],
            )
        )

        result = await ai_client.chat(
            model="gpt-4o",
            messages=[{"role": "user", "content": "weather?"}],
            tools=[GetWeather],
        )

        assert len(result.tool_calls) == 1
        tc = result.tool_calls[0]
        assert tc.id == "call_abc"
        assert tc.function_name == "get_weather"
        assert isinstance(tc.args, GetWeather)
        assert tc.args.city == "London"
        assert tc.args.unit == "celsius"

    @pytest.mark.asyncio
    async def test_no_tools_returns_empty_list(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_completion(content="hello")
        )

        result = await ai_client.chat(
            model="gpt-4o",
            messages=[{"role": "user", "content": "hi"}],
        )

        assert result.tool_calls == []


# ── stream_chat with tools ───────────────────────────────────────


class TestStreamChatWithTools:
    @pytest.mark.asyncio
    async def test_accumulates_tool_calls(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_tool_call_chunks(
                tc_id="call_xyz",
                name="get_weather",
                arg_fragments=['{"city":', '"London"}'],
            )
        )

        chunks = [
            c
            async for c in ai_client.stream_chat(
                model="gpt-4o",
                messages=[{"role": "user", "content": "weather?"}],
                tools=[GetWeather],
            )
        ]

        last = chunks[-1]
        assert last.is_last is True
        assert last.finish_reason == "tool_calls"
        assert len(last.tool_calls) == 1
        tc = last.tool_calls[0]
        assert tc.id == "call_xyz"
        assert tc.function_name == "get_weather"
        assert tc.arguments == '{"city":"London"}'
        assert isinstance(tc.args, GetWeather)
        assert tc.args.city == "London"

    @pytest.mark.asyncio
    async def test_no_tool_calls_empty(self, ai_client: AIClient) -> None:
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["hello"])
        )

        chunks = [
            c
            async for c in ai_client.stream_chat(
                model="gpt-4o",
                messages=[{"role": "user", "content": "hi"}],
            )
        ]

        last = chunks[-1]
        assert last.tool_calls == []


# ── run (agentic loop) ──────────────────────────────────────────


class TestRun:
    @pytest.mark.asyncio
    async def test_text_response_no_loop(self, ai_client: AIClient) -> None:
        """When no tool calls, run yields chunks and returns."""
        ai_client._openai_client.chat.completions.create = AsyncMock(
            return_value=_mock_stream_chunks(["hello", " world"])
        )

        chunks = [
            c
            async for c in ai_client.stream_agent(
                model="gpt-4o",
                messages=[{"role": "user", "content": "hi"}],
                tools=[GetWeather],
            )
        ]

        assert chunks[0].delta == "hello"
        assert chunks[1].delta == " world"
        assert chunks[-1].is_last is True

    @pytest.mark.asyncio
    async def test_tool_call_then_text(self, ai_client: AIClient) -> None:
        """Tool call is executed and result fed back, second turn returns text."""
        call_count = 0

        async def _create(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: model returns tool call
                return _mock_stream_tool_call_chunks(
                    tc_id="call_1",
                    name="get_weather",
                    arg_fragments=['{"city":"Tokyo"}'],
                )
            else:
                # Second call: model returns text
                return _mock_stream_chunks(["It's 22° in Tokyo"])

        ai_client._openai_client.chat.completions.create = AsyncMock(
            side_effect=_create
        )

        messages: list[dict[str, Any]] = [
            {"role": "user", "content": "What's the weather in Tokyo?"}
        ]
        chunks = [
            c
            async for c in ai_client.stream_agent(
                model="gpt-4o",
                messages=messages,
                tools=[GetWeather],
            )
        ]

        # Should have chunks from both turns
        text_chunks = [c for c in chunks if c.delta]
        assert any("22°" in c.delta for c in text_chunks)

        # Messages should have been mutated with tool call + result
        assert any(m.get("role") == "tool" for m in messages)
        tool_msg = next(m for m in messages if m.get("role") == "tool")
        assert "Tokyo" in tool_msg["content"]

    @pytest.mark.asyncio
    async def test_max_turns_respected(self, ai_client: AIClient) -> None:
        """Loop stops after max_turns even if model keeps calling tools."""
        ai_client._openai_client.chat.completions.create = AsyncMock(
            side_effect=lambda **_: _mock_stream_tool_call_chunks(
                tc_id="call_loop",
                name="get_weather",
                arg_fragments=['{"city":"X"}'],
            )
        )

        chunks = [
            c
            async for c in ai_client.stream_agent(
                model="gpt-4o",
                messages=[{"role": "user", "content": "loop"}],
                tools=[GetWeather],
                max_turns=2,
            )
        ]

        # Should have yielded chunks from exactly 2 turns
        last_chunks = [c for c in chunks if c.is_last]
        assert len(last_chunks) == 2


# ── fal_submit (renamed from fal_call) ─────────────────────────


class TestFalSubmit:
    @pytest.mark.asyncio
    async def test_submits_and_returns_request_id(self, ai_client: AIClient) -> None:
        mock_fal = AsyncMock()
        mock_fal.submit = AsyncMock(return_value=MagicMock(request_id="req-123"))
        ai_client._fal_client = mock_fal

        request_id = await ai_client.fal_submit("fal-ai/flux", inputs={"prompt": "cat"})

        assert request_id == "req-123"
        mock_fal.submit.assert_awaited_once_with(
            "fal-ai/flux",
            arguments={"prompt": "cat"},
            start_timeout=10.0,
        )

    @pytest.mark.asyncio
    async def test_raises_without_credentials(self, ai_client: AIClient) -> None:
        ai_client._fal_client = None
        with pytest.raises(FalMissingCredentialsError):
            await ai_client.fal_submit("fal-ai/flux", inputs={})


# ── fal_call (submit + poll + get) ─────────────────────────────


class TestFalCall:
    @pytest.mark.asyncio
    async def test_submits_polls_and_returns_result(self, ai_client: AIClient) -> None:
        ai_client.fal_submit = AsyncMock(return_value="req-123")
        ai_client.fal_poll = AsyncMock(
            side_effect=[
                JobStatus(state=JobState.QUEUED, position=1),
                JobStatus(state=JobState.IN_PROGRESS),
                JobStatus(state=JobState.COMPLETED),
            ]
        )
        expected = {"images": [{"url": "http://example.com/img.jpg"}]}
        ai_client.fal_get = AsyncMock(return_value=expected)

        result = await ai_client.fal_call(
            "fal-ai/flux",
            inputs={"prompt": "cat"},
            poll_interval=0,
        )

        assert result == expected
        ai_client.fal_submit.assert_awaited_once()
        assert ai_client.fal_poll.await_count == 3
        ai_client.fal_get.assert_awaited_once_with("fal-ai/flux", "req-123")

    @pytest.mark.asyncio
    async def test_raises_on_failure(self, ai_client: AIClient) -> None:
        ai_client.fal_submit = AsyncMock(return_value="req-123")
        ai_client.fal_poll = AsyncMock(
            return_value=JobStatus(state=JobState.FAILED, error="OOM")
        )

        with pytest.raises(FalJobFailedError, match="OOM"):
            await ai_client.fal_call("fal-ai/flux", inputs={}, poll_interval=0)

    @pytest.mark.asyncio
    async def test_timeout(self, ai_client: AIClient) -> None:
        ai_client.fal_submit = AsyncMock(return_value="req-123")
        ai_client.fal_poll = AsyncMock(
            return_value=JobStatus(state=JobState.IN_PROGRESS)
        )

        with pytest.raises(TimeoutError):
            await ai_client.fal_call(
                "fal-ai/flux",
                inputs={},
                poll_interval=0.01,
                timeout=0.05,
            )


# ── ChatChunk tool events ──────────────────────────────────────


class TestChatChunkToolEvents:
    def test_vercel_input_start(self) -> None:
        chunk = ChatChunk(
            delta="",
            tool_event=ToolEventType.INPUT_START,
            tool_call_id="call_1",
            tool_name="get_weather",
        )
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert len(events) == 1
        assert events[0] == {
            "type": "tool-input-start",
            "toolCallId": "call_1",
            "toolName": "get_weather",
        }

    def test_vercel_input_available(self) -> None:
        chunk = ChatChunk(
            delta="",
            tool_event=ToolEventType.INPUT_AVAILABLE,
            tool_call_id="call_1",
            tool_name="get_weather",
            tool_input={"city": "London"},
        )
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert len(events) == 1
        assert events[0] == {
            "type": "tool-input-available",
            "toolCallId": "call_1",
            "toolName": "get_weather",
            "input": {"city": "London"},
        }

    def test_vercel_output_available(self) -> None:
        chunk = ChatChunk(
            delta="",
            tool_event=ToolEventType.OUTPUT_AVAILABLE,
            tool_call_id="call_1",
            tool_output={"temperature": 22},
        )
        events = chunk.vercel_ai_json(message_id="msg-1")
        assert len(events) == 1
        assert events[0] == {
            "type": "tool-output-available",
            "toolCallId": "call_1",
            "output": {"temperature": 22},
        }

    def test_tanstack_input_start(self) -> None:
        chunk = ChatChunk(
            delta="",
            tool_event=ToolEventType.INPUT_START,
            tool_call_id="call_1",
            tool_name="get_weather",
        )
        events = chunk.tanstack_ai_json(message_id="msg-1")
        assert len(events) == 1
        assert events[0] == {
            "type": "TOOL_CALL_START",
            "toolCallId": "call_1",
            "toolCallName": "get_weather",
        }

    def test_tanstack_input_available(self) -> None:
        chunk = ChatChunk(
            delta="",
            tool_event=ToolEventType.INPUT_AVAILABLE,
            tool_call_id="call_1",
            tool_name="get_weather",
            tool_input={"city": "London"},
        )
        events = chunk.tanstack_ai_json(message_id="msg-1")
        assert len(events) == 2
        assert events[0]["type"] == "TOOL_CALL_ARGS"
        assert events[0]["toolCallId"] == "call_1"
        assert events[1] == {
            "type": "TOOL_CALL_END",
            "toolCallId": "call_1",
        }

    def test_tanstack_output_available_empty(self) -> None:
        chunk = ChatChunk(
            delta="",
            tool_event=ToolEventType.OUTPUT_AVAILABLE,
            tool_call_id="call_1",
            tool_output={"temp": 22},
        )
        events = chunk.tanstack_ai_json(message_id="msg-1")
        assert events == []


# ── stream_agent tool events ───────────────────────────────────


class TestStreamAgentToolEvents:
    @pytest.mark.asyncio
    async def test_yields_tool_lifecycle_events(self, ai_client: AIClient) -> None:
        call_count = 0

        async def _create(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_stream_tool_call_chunks(
                    tc_id="call_1",
                    name="get_weather",
                    arg_fragments=['{"city":"Tokyo"}'],
                )
            else:
                return _mock_stream_chunks(["It's 22°"])

        ai_client._openai_client.chat.completions.create = AsyncMock(
            side_effect=_create
        )

        chunks = [
            c
            async for c in ai_client.stream_agent(
                model="gpt-4o",
                messages=[{"role": "user", "content": "weather?"}],
                tools=[GetWeather],
            )
        ]

        tool_events = [c for c in chunks if c.tool_event is not None]
        assert len(tool_events) == 3
        assert tool_events[0].tool_event == ToolEventType.INPUT_START
        assert tool_events[0].tool_call_id == "call_1"
        assert tool_events[0].tool_name == "get_weather"
        assert tool_events[1].tool_event == ToolEventType.INPUT_AVAILABLE
        assert tool_events[1].tool_input == {"city": "Tokyo"}
        assert tool_events[2].tool_event == ToolEventType.OUTPUT_AVAILABLE
        assert tool_events[2].tool_output["city"] == "Tokyo"

    @pytest.mark.asyncio
    async def test_tool_events_serialize_to_vercel(self, ai_client: AIClient) -> None:
        """Tool event chunks produce valid Vercel AI SSE."""
        call_count = 0

        async def _create(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_stream_tool_call_chunks(
                    tc_id="call_1",
                    name="get_weather",
                    arg_fragments=['{"city":"London"}'],
                )
            else:
                return _mock_stream_chunks(["22° celsius"])

        ai_client._openai_client.chat.completions.create = AsyncMock(
            side_effect=_create
        )

        all_events: list[str] = []
        async for chunk in ai_client.stream_agent(
            model="gpt-4o",
            messages=[{"role": "user", "content": "weather?"}],
            tools=[GetWeather],
        ):
            for event in chunk.vercel_ai_json(message_id="msg-1"):
                all_events.append(event.dump())

        sse_text = "".join(all_events)
        assert "tool-input-start" in sse_text
        assert "tool-input-available" in sse_text
        assert "tool-output-available" in sse_text
        assert sse_text.endswith("data: [DONE]\n\n")

    @pytest.mark.asyncio
    async def test_tool_args_forwarded_to_run(self, ai_client: AIClient) -> None:
        """tool_args are passed through to Tool.run()."""
        call_count = 0

        async def _create(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_stream_tool_call_chunks(
                    tc_id="call_1",
                    name="stage_room",
                    arg_fragments=['{"design_style":"modern"}'],
                )
            else:
                return _mock_stream_chunks(["Staged!"])

        ai_client._openai_client.chat.completions.create = AsyncMock(
            side_effect=_create
        )

        fake_derp = object()
        chunks = [
            c
            async for c in ai_client.stream_agent(
                model="gpt-4o",
                messages=[{"role": "user", "content": "stage it"}],
                tools=[StageRoom],
                tool_args=[fake_derp, "user-42"],
            )
        ]

        output_events = [
            c for c in chunks if c.tool_event == ToolEventType.OUTPUT_AVAILABLE
        ]
        assert len(output_events) == 1
        result = output_events[0].tool_output
        assert result["style"] == "modern"
        assert result["user_id"] == "user-42"
        assert result["has_client"] is True
