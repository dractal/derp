AI
==

Async AI client with support for OpenAI-compatible providers, fal (image
generation), and Modal (serverless inference). Access it via ``derp.ai``.

Config
------

.. code-block:: toml

   # derp.toml
   [ai]
   api_key = "$OPENAI_API_KEY"
   # base_url = "https://api.openrouter.ai/v1"  # for other providers
   # fal_api_key = "$FAL_API_KEY"

   # [ai.modal_config]
   # token_id = "$MODAL_TOKEN_ID"
   # token_secret = "$MODAL_TOKEN_SECRET"
   # endpoint_url = "https://your-app.modal.run"

Chat
----

.. code-block:: python

   response = await derp.ai.chat(
       model="gpt-4o-mini",
       messages=[{"role": "user", "content": "Hello"}],
   )
   print(response.content)  # "Hi there!"
   print(response.usage)    # Usage(prompt_tokens=10, ...)

Returns a :class:`~derp.ai.models.ChatResponse` with ``content``, ``role``,
``model``, ``usage``, and ``finish_reason``.

Streaming
---------

.. code-block:: python

   async for chunk in derp.ai.stream_chat(
       model="gpt-4o-mini",
       messages=[{"role": "user", "content": "Hello"}],
   ):
       print(chunk.delta, end="")

Each :class:`~derp.ai.models.ChatChunk` carries ``delta``, ``role``,
``model``, and lifecycle flags (``is_first``, ``is_last``). The final chunk
includes ``finish_reason`` and ``usage`` (when the provider supports it).

Protocol Adapters
-----------------

Both ``ChatResponse`` and ``ChatChunk`` have ``vercel_ai_json()`` and
``tanstack_ai_json()`` methods that return ``SSEEvent`` objects ready for
streaming.

**Vercel AI SDK:**

.. code-block:: python

   from fastapi.responses import StreamingResponse

   @app.post("/api/chat")
   async def chat(request: ChatRequest):
       mid = f"msg-{uuid4().hex}"

       async def sse():
           async for chunk in derp.ai.stream_chat(
               model=request.model, messages=request.messages
           ):
               for event in chunk.vercel_ai_json(message_id=mid):
                   yield event.dump()

       return StreamingResponse(sse(), media_type="text/event-stream")

**TanStack AI:**

.. code-block:: python

   async def sse():
       async for chunk in derp.ai.stream_chat(
           model=request.model, messages=request.messages
       ):
           for event in chunk.tanstack_ai_json(message_id=mid):
               yield event.dump()

Non-streaming responses work the same way:

.. code-block:: python

   response = await derp.ai.chat(model="gpt-4o-mini", messages=messages)
   async def sse():
       for event in response.vercel_ai_json():
           yield event.dump()

Each ``SSEEvent`` is a ``dict`` subclass with a ``.dump()`` method that
serializes to ``data: {...}\n\n``. The final event in a complete sequence is
always ``SSEDone`` which dumps to ``data: [DONE]\n\n``.

Fal (Image Generation)
----------------------

.. code-block:: python

   # Submit a job
   request_id = await derp.ai.fal_call(
       application="fal-ai/flux",
       inputs={"prompt": "a cat in space"},
   )

   # Poll for status
   status = await derp.ai.fal_poll("fal-ai/flux", request_id)
   if status.is_completed:
       ...
   elif status.is_queued:
       print(f"Position: {status.position}")

   # Cancel
   result = await derp.ai.fal_cancel("fal-ai/flux", request_id)
   if result.is_cancelled and result.job_queued:
       # Never started, safe to skip billing
       ...

Modal
-----

Call Modal serverless endpoints directly:

.. code-block:: python

   result = await derp.ai.modal_call(
       endpoint="/inference",
       inputs={"text": "hello"},
       timeout=30.0,
   )

Requires ``[ai.modal_config]`` in config. The client connects/disconnects
with the ``DerpClient`` lifecycle.
