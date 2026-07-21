#!/usr/bin/env python3
"""Small dependency-free checks for the Customer Map Hermes plugin."""

import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import types
from pathlib import Path
from types import SimpleNamespace
from aiohttp import web

ROOT = Path(__file__).parent


def _load_adapter():
    gateway = types.ModuleType("gateway")
    config = types.ModuleType("gateway.config")
    platforms = types.ModuleType("gateway.platforms")
    base = types.ModuleType("gateway.platforms.base")

    class Platform(str):
        @property
        def value(self):
            return str(self)

    class BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform

        def build_source(self, **kwargs):
            return SimpleNamespace(**kwargs, delivered_via_upstream_relay=False)

        def _mark_connected(self):
            pass

        def _mark_disconnected(self):
            pass

        def _set_fatal_error(self, *args, **kwargs):
            pass

    class MessageEvent:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class MessageType:
        TEXT = "text"

    class SendResult:
        def __init__(self, success, error=None, retryable=False, message_id=None):
            self.success = success
            self.error = error
            self.retryable = retryable
            self.message_id = message_id

    config.Platform = Platform
    base.BasePlatformAdapter = BasePlatformAdapter
    base.MessageEvent = MessageEvent
    base.MessageType = MessageType
    base.SendResult = SendResult
    sys.modules.update({
        "gateway": gateway,
        "gateway.config": config,
        "gateway.platforms": platforms,
        "gateway.platforms.base": base,
    })
    spec = importlib.util.spec_from_file_location("customer_map_adapter_test", ROOT / "adapter.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _check_async_final_response():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    adapter._ws = WebSocket()

    async def handle_message(event):
        async def respond():
            await asyncio.sleep(0)
            await adapter.send(event.source.chat_id, "progress", metadata={})
            await adapter.send(event.source.chat_id, "final", metadata={"notify": True})

        asyncio.create_task(respond())

    adapter.handle_message = handle_message
    await adapter._run_job({"id": "job-1", "timeoutMs": 10000, "request": {"sessionId": "session-1", "input": []}})
    assert adapter._ws.messages == [{
        "type": "progress",
        "jobId": "job-1",
        "content": "progress",
        "pluginVersion": module.PLUGIN_VERSION,
    }, {
        "type": "progress",
        "jobId": "job-1",
        "content": "final",
        "pluginVersion": module.PLUGIN_VERSION,
    }, {
        "type": "complete",
        "jobId": "job-1",
        "response": {"output_text": "final"},
        "error": "",
        "pluginVersion": module.PLUGIN_VERSION,
    }]


async def _check_consecutive_session_turns():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    adapter._ws = WebSocket()

    async def handle_message(event):
        await asyncio.sleep(0.05 if event.message_id == "job-a" else 0)
        await adapter.send(event.source.chat_id, f"final-{event.message_id}", metadata={"notify": True})

    adapter.handle_message = handle_message
    first = asyncio.create_task(adapter._run_job({"id": "job-a", "timeoutMs": 10000, "request": {"sessionId": "same-session", "input": []}}))
    await asyncio.sleep(0.01)
    second = asyncio.create_task(adapter._run_job({"id": "job-b", "timeoutMs": 10000, "request": {"sessionId": "same-session", "input": []}}))
    await asyncio.gather(first, second)
    completed = [message for message in adapter._ws.messages if message.get("type") == "complete"]
    assert [message.get("jobId") for message in completed] == ["job-a", "job-b"]
    assert all(not message.get("error") for message in completed)


async def _check_completed_turn_without_notify_flag():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    adapter._ws = WebSocket()
    final_text = '{"reply":"done","continue":false,"actionReceipt":null}'

    async def handle_message(event):
        await adapter.send(event.source.chat_id, final_text, metadata={})
        await adapter.on_processing_complete(event, {})

    adapter.handle_message = handle_message
    await adapter._run_job({"id": "job-no-notify", "timeoutMs": 10000, "request": {"sessionId": "session-no-notify", "input": []}})
    assert adapter._ws.messages == [{
        "type": "progress",
        "jobId": "job-no-notify",
        "content": final_text,
        "pluginVersion": module.PLUGIN_VERSION,
    }, {
        "type": "complete",
        "jobId": "job-no-notify",
        "response": {"output_text": final_text},
        "error": "",
        "pluginVersion": module.PLUGIN_VERSION,
    }]


async def _check_direct_gog_send_action():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    recipient = "buyer@example.com"
    subject = "HTML table"
    plain_text = "Model | Price"
    html_body = "<table><tr><td>Model</td><td>Price</td></tr></table>"
    body_hash = module._mail_body_hash(recipient, subject, plain_text, html_body)
    action = {
        "version": 1,
        "actionId": "a" * 32,
        "kind": "sendEmail",
        "account": "sender@example.com",
        "recipient": recipient,
        "subject": subject,
        "plainTextBody": plain_text,
        "htmlBody": html_body,
        "bodyHash": body_hash,
    }
    captured = {}

    async def execute(value):
        captured.update(value)
        return module._mail_action_result(
            value,
            "succeeded",
            message_id="message-123",
            tool_version="0.11.0",
            exit_code=0,
        )

    async def reject_model_call(_event):
        raise AssertionError("Direct mail actions must bypass Hermes model execution")

    module._execute_gog_send = execute
    adapter.handle_message = reject_model_call
    adapter._ws = WebSocket()
    await adapter._run_job({
        "id": "job-mail",
        "timeoutMs": 10000,
        "request": {
            "sessionId": "mail-action-session",
            "input": [],
            "mailAction": action,
        },
    })
    assert captured["htmlBody"] == html_body
    complete = adapter._ws.messages[-1]
    payload = json.loads(complete["response"]["output_text"])
    receipt = payload["actionReceipt"]
    assert receipt["status"] == "succeeded"
    assert receipt["messageId"] == "message-123"
    assert receipt["bodyMode"] == "body-html+body"
    assert receipt["tool"] == "gog"

    args = module._build_gog_send_args(captured)
    assert any(value.startswith("--body-html=") for value in args)
    assert any(value.startswith("--body=") for value in args)
    assert not any("body-file" in value or "/dev/stdin" in value for value in args)


async def _check_rejects_stdin_body():
    module = _load_adapter()
    action = {
        "version": 1,
        "actionId": "b" * 32,
        "kind": "sendEmail",
        "account": "sender@example.com",
        "recipient": "buyer@example.com",
        "subject": "Unsafe body",
        "plainTextBody": "/dev/stdin",
        "htmlBody": "",
    }
    action["bodyHash"] = module._mail_body_hash(
        action["recipient"],
        action["subject"],
        action["plainTextBody"],
        action["htmlBody"],
    )
    try:
        module._normalize_mail_action(action)
    except ValueError as exc:
        assert "filesystem or stdin path" in str(exc)
    else:
        raise AssertionError("/dev/stdin must be rejected as an email body")


async def _check_websocket_reconnect():
    module = _load_adapter()
    completed = asyncio.get_running_loop().create_future()
    reconnected = asyncio.get_running_loop().create_future()
    connection_count = 0

    async def relay(request):
        nonlocal connection_count
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        connection_count += 1
        hello = await socket.receive_json()
        assert hello["type"] == "hello" and hello["runtime"] == "hermes"
        await socket.send_json({"type": "ready"})
        if connection_count == 1:
            await socket.send_json({"type": "job", "job": {"id": "job-2", "timeoutMs": 10000, "request": {"sessionId": "session-2", "input": []}}})
            while True:
                message = await socket.receive_json()
                if message.get("type") == "complete":
                    completed.set_result(message)
                    break
            await socket.close()
        else:
            reconnected.set_result(True)
            await asyncio.sleep(0.1)
        return socket

    app = web.Application()
    app.router.add_get("/customer-map", relay)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    values = {
        "CUSTOMER_MAP_HERMES_SITE": "http://127.0.0.1",
        "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": "token",
        "CUSTOMER_MAP_HERMES_CONNECTION_ID": "connection",
        "CUSTOMER_MAP_HERMES_RELAY_URL": f"ws://127.0.0.1:{port}/customer-map",
    }
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    adapter = module.CustomerMapAdapter({})

    async def handle_message(event):
        async def respond():
            await asyncio.sleep(0)
            await adapter.send(event.source.chat_id, "final", metadata={"notify": True})

        asyncio.create_task(respond())

    adapter.handle_message = handle_message
    try:
        assert await adapter.connect()
        message = await asyncio.wait_for(completed, timeout=3)
        assert message["response"]["output_text"] == "final"
        await asyncio.wait_for(reconnected, timeout=5)
    finally:
        await adapter.disconnect()
        await runner.cleanup()
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _check_env_write():
    spec = importlib.util.spec_from_file_location("customer_map_connect_test", ROOT / "connect.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory() as directory:
        previous = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = directory
        try:
            module._write_env({"CUSTOMER_MAP_HERMES_SITE": "https://example.com", "CUSTOMER_MAP_HERMES_CONNECTION_ID": "abc"})
            text = (Path(directory) / ".env").read_text(encoding="utf-8")
            assert "CUSTOMER_MAP_HERMES_SITE=https://example.com" in text
            assert "CUSTOMER_MAP_HERMES_CONNECTION_ID=abc" in text
        finally:
            if previous is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = previous


if __name__ == "__main__":
    _check_env_write()
    asyncio.run(_check_async_final_response())
    asyncio.run(_check_consecutive_session_turns())
    asyncio.run(_check_completed_turn_without_notify_flag())
    asyncio.run(_check_direct_gog_send_action())
    asyncio.run(_check_rejects_stdin_body())
    asyncio.run(_check_websocket_reconnect())
    print("Hermes plugin checks passed")
