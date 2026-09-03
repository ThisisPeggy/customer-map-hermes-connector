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
sys.path.insert(0, str(ROOT))

import mail_backends


def _load_adapter():
    gateway = types.ModuleType("gateway")
    config = types.ModuleType("gateway.config")
    platforms = types.ModuleType("gateway.platforms")
    base = types.ModuleType("gateway.platforms.base")
    gateway_run = types.ModuleType("gateway.run")
    gateway_session = types.ModuleType("gateway.session")
    hermes_cli = types.ModuleType("hermes_cli")
    tools_config = types.ModuleType("hermes_cli.tools_config")
    tools = types.ModuleType("tools")
    tools_registry = types.ModuleType("tools.registry")
    toolsets = types.ModuleType("toolsets")
    toolsets.TOOLSETS = {}
    toolsets.resolve_toolset = lambda name: list(toolsets.TOOLSETS.get(name, {}).get("tools", []))
    tools_registry.registry = SimpleNamespace(get_all_entries=lambda: [
        SimpleNamespace(name="mcp__my_firecrawl__firecrawl_scrape", toolset="mcp-my-firecrawl"),
        SimpleNamespace(name="mcp__my_firecrawl__firecrawl_search", toolset="mcp-my-firecrawl"),
        SimpleNamespace(name="mcp__other__firecrawl_search", toolset="mcp-other"),
    ])

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
    gateway_run._load_gateway_config = lambda: {"platform_toolsets": {"customer_map": ["customer-map-readonly", "no_mcp"]}}
    tools_config._get_platform_tools = lambda _config, _platform: {"customer-map-readonly"}
    tools_config._get_plugin_toolset_keys = lambda: set()
    gateway_session.build_session_key = lambda source, **_kwargs: f"agent:main:customer_map:dm:{source.chat_id}"
    sys.modules.update({
        "gateway": gateway,
        "gateway.config": config,
        "gateway.platforms": platforms,
        "gateway.platforms.base": base,
        "gateway.run": gateway_run,
        "gateway.session": gateway_session,
        "hermes_cli": hermes_cli,
        "hermes_cli.tools_config": tools_config,
        "tools": tools,
        "tools.registry": tools_registry,
        "toolsets": toolsets,
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


async def _check_streaming_response_frames():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    adapter._ws = WebSocket()
    adapter._pending["session-stream"] = {
        "job_id": "job-stream",
        "completion": asyncio.get_running_loop().create_future(),
        "last_content": "",
        "last_metadata": {},
    }

    assert adapter.supports_draft_streaming(chat_type="dm") is True
    draft = await adapter.send_draft("session-stream", 1, "第一段")
    edited = await adapter.edit_message("session-stream", "job-stream", "第一段第二段")
    assert draft.success and edited.success
    assert [message["content"] for message in adapter._ws.messages] == ["第一段", "第一段第二段"]
    assert adapter._pending["session-stream"]["last_content"] == "第一段第二段"


def _check_safe_tool_activity():
    module = _load_adapter()
    assert module._tool_activity("web_search", {"query": "Raute OYJ contact"}) == "正在搜索：Raute OYJ contact"
    assert module._tool_activity("web_extract", {"urls": ["https://www.raute.com/contact/?token=secret"]}) == "正在读取：raute.com"
    assert module._tool_activity("mcp__my_firecrawl__firecrawl_search", {"query": "Raute OYJ contact"}) == "正在深度搜索：Raute OYJ contact"
    assert module._tool_activity("mcp__my_firecrawl__firecrawl_scrape", {"url": "https://www.raute.com/contact/?token=secret"}) == "正在深度读取：raute.com"
    assert module._tool_activity("mcp__other__firecrawl_search", {"query": "blocked"}) == ""
    assert module._tool_activity("skill_view", {"name": "customer-research"}) == "正在加载技能：customer-research"
    assert module._tool_activity("terminal", {"command": "printenv SECRET"}) == ""


def _check_tool_call_boundary():
    module = _load_adapter()
    module._CUSTOMER_MAP_SESSIONS.add("customer-map-session")
    assert module._on_pre_tool_call(
        session_id="customer-map-session", tool_name="skills_list", args={}
    ) is None
    assert module._on_pre_tool_call(
        session_id="customer-map-session",
        tool_name="mcp__my_firecrawl__firecrawl_scrape",
        args={"url": "https://example.com/about"},
    ) is None
    blocked_action = module._on_pre_tool_call(
        session_id="customer-map-session",
        tool_name="mcp__my_firecrawl__firecrawl_scrape",
        args={"url": "https://example.com", "actions": [{"type": "click", "selector": "button"}]},
    )
    assert blocked_action["action"] == "block"
    assert "interactive browser actions" in blocked_action["message"]
    blocked_url = module._on_pre_tool_call(
        session_id="customer-map-session",
        tool_name="mcp__my_firecrawl__firecrawl_scrape",
        args={"url": "http://127.0.0.1/private"},
    )
    assert blocked_url["action"] == "block"
    assert "public HTTP(S)" in blocked_url["message"]
    blocked_tool = module._on_pre_tool_call(
        session_id="customer-map-session", tool_name="skill_manage", args={}
    )
    assert blocked_tool["action"] == "block"


async def _check_tool_activity_progress():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False
        def __init__(self):
            self.messages = []
        async def send_json(self, value):
            self.messages.append(value)

    adapter._ws = WebSocket()
    adapter._loop = asyncio.get_running_loop()
    adapter._pending["customer-map-session"] = {"job_id": "job-tool", "hermes_session_id": "", "completion": None, "last_content": "", "last_metadata": {}}
    with module._ACTIVE_ADAPTERS_LOCK:
        module._ACTIVE_ADAPTERS.add(adapter)
    try:
        module._on_session_start(platform="customer_map", session_id="hermes-session")
        module._on_pre_tool_call(session_id="hermes-session", tool_name="web_search", args={"query": "Raute OYJ"})
        await asyncio.sleep(0.01)
        assert adapter._ws.messages[-1]["content"] == "正在搜索：Raute OYJ"
        assert adapter._pending["customer-map-session"]["hermes_session_id"] == "hermes-session"
    finally:
        with module._ACTIVE_ADAPTERS_LOCK:
            module._ACTIVE_ADAPTERS.discard(adapter)


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


async def _check_active_job_cancellation():
    module = _load_adapter()
    adapter = module.CustomerMapAdapter({})
    adapter.config = SimpleNamespace(extra={})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    adapter._ws = WebSocket()
    cancelled = []

    async def handle_message(_event):
        return None

    async def cancel_session_processing(session_key):
        cancelled.append(session_key)

    adapter.handle_message = handle_message
    adapter.cancel_session_processing = cancel_session_processing
    task = asyncio.create_task(adapter._run_job({"id": "job-cancel", "timeoutMs": 10000, "request": {"sessionId": "session-cancel", "input": []}}))
    while "session-cancel" not in adapter._pending or not adapter._pending["session-cancel"].get("session_key"):
        await asyncio.sleep(0)
    await adapter._cancel_job("job-cancel")
    await task

    assert cancelled == ["agent:main:customer_map:dm:session-cancel"]
    assert adapter._ws.messages[-1]["type"] == "complete"
    assert adapter._ws.messages[-1]["jobId"] == "job-cancel"
    assert adapter._ws.messages[-1]["error"] == "Agent request cancelled by user."


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
    temporary_home = tempfile.TemporaryDirectory()
    previous_home = os.environ.get("HERMES_HOME")
    os.environ["HERMES_HOME"] = temporary_home.name
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
        return {
            "status": "succeeded",
            "provider": "gmail",
            "tool": "gog",
            "messageId": "message-123",
            "error": "",
            "toolVersion": "0.11.0",
            "bodyMode": "body-html+body",
            "exitCode": 0,
        }

    async def reject_model_call(_event):
        raise AssertionError("Direct mail actions must bypass Hermes model execution")

    module.execute_mail_action = execute
    adapter.handle_message = reject_model_call
    adapter._ws = WebSocket()
    try:
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

        args = mail_backends._build_gog_mail_args(captured)
        assert any(value.startswith("--body-html=") for value in args)
        assert any(value.startswith("--body=") for value in args)
        assert "--no-input" in args
        assert not any("body-file" in value or "/dev/stdin" in value for value in args)
    finally:
        temporary_home.cleanup()
        if previous_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = previous_home


async def _check_direct_gog_draft_action():
    module = _load_adapter()
    recipient = "buyer@example.com"
    subject = "Draft price list"
    plain_text = "Draft body"
    html_body = ""
    action = {
        "version": 1,
        "actionId": "d" * 32,
        "kind": "saveDraft",
        "account": "sender@example.com",
        "recipient": recipient,
        "subject": subject,
        "plainTextBody": plain_text,
        "htmlBody": html_body,
        "bodyHash": module._mail_body_hash(recipient, subject, plain_text, html_body),
    }
    normalized = module._normalize_mail_action(action)
    assert normalized["kind"] == "saveDraft"
    args = mail_backends._build_gog_mail_args(normalized)
    assert args[:4] == ["gog", "gmail", "drafts", "create"]
    assert "--force" not in args
    assert "--json" in args and "--no-input" in args
    result = module._mail_action_result(normalized, "succeeded", message_id="draft-123456", provider="gmail", tool="gog", tool_version="0.11.0", exit_code=0)
    assert result["actionReceipt"]["kind"] == "saveDraft"
    assert result["actionReceipt"]["messageId"] == "draft-123456"


async def _check_himalaya_backend_mapping():
    recipient = "buyer@example.com"
    action = {
        "version": 1,
        "actionId": "e" * 32,
        "kind": "saveDraft",
        "account": "sender@example.com",
        "recipient": recipient,
        "subject": "Himalaya draft",
        "plainTextBody": "Plain body",
        "htmlBody": "<p>HTML body</p>",
        "bodyHash": "f" * 64,
    }
    previous_account = os.environ.get("CUSTOMER_MAP_HERMES_HIMALAYA_ACCOUNT")
    original_version = mail_backends._read_tool_version
    original_run = mail_backends._run_process
    os.environ["CUSTOMER_MAP_HERMES_HIMALAYA_ACCOUNT"] = "sales"
    try:
        draft_args = mail_backends._build_himalaya_mail_args(action)
        assert draft_args[:3] == ["himalaya", "message", "add"]
        assert "--mailbox" in draft_args and "--flag" in draft_args and "draft" in draft_args
        assert "--send" not in draft_args
        assert draft_args[draft_args.index("--account") + 1] == "sales"
        raw_message, message_id = mail_backends._build_rfc5322_message(action)
        assert b"From: sender@example.com" in raw_message
        assert b"To: buyer@example.com" in raw_message
        assert b"Content-Type: multipart/alternative" in raw_message
        assert action["actionId"].encode() in raw_message

        async def fake_version(_tool, _minimum):
            return "2.0.0"

        async def fake_run(args, timeout, stdin=None):
            assert args == draft_args
            assert b"X-Customer-Map-Action-ID: " + action["actionId"].encode() in stdin
            assert b"HTML body" in stdin and b"Plain body" in stdin
            return {"exitCode": 0, "output": "{}", "timedOut": False, "launchError": ""}

        mail_backends._read_tool_version = fake_version
        mail_backends._run_process = fake_run
        result = await mail_backends._execute_himalaya_mail_action(action)
        assert result["status"] == "succeeded"
        assert result["messageId"] == message_id
        assert result["tool"] == "himalaya"
        assert result["bodyMode"] == "multipart-alternative"
    finally:
        mail_backends._read_tool_version = original_version
        mail_backends._run_process = original_run
        if previous_account is None:
            os.environ.pop("CUSTOMER_MAP_HERMES_HIMALAYA_ACCOUNT", None)
        else:
            os.environ["CUSTOMER_MAP_HERMES_HIMALAYA_ACCOUNT"] = previous_account


async def _check_mail_backend_auto_detection():
    original_version = mail_backends._read_tool_version
    previous_backend = os.environ.pop("CUSTOMER_MAP_HERMES_MAIL_BACKEND", None)
    try:
        async def only_himalaya(tool, _minimum):
            if tool == "himalaya":
                return "2.0.0"
            raise RuntimeError("not installed")

        mail_backends._read_tool_version = only_himalaya
        assert mail_backends.configured_mail_backend() == "auto"
        assert await mail_backends.resolve_mail_backends() == ["himalaya"]

        async def both_backends(tool, _minimum):
            return "0.11.0" if tool == "gog" else "2.0.0"

        mail_backends._read_tool_version = both_backends
        assert await mail_backends.resolve_mail_backends() == ["gog", "himalaya"]

        calls = []
        original_gog = mail_backends._execute_gog_mail_action
        original_himalaya = mail_backends._execute_himalaya_mail_action

        async def failed_gog(_action):
            calls.append("gog")
            return mail_backends._backend_result("failed", provider="gmail", tool="gog", error="not configured")

        async def working_himalaya(_action):
            calls.append("himalaya")
            return mail_backends._backend_result("succeeded", provider="email", tool="himalaya", message_id="ok")

        mail_backends._execute_gog_mail_action = failed_gog
        mail_backends._execute_himalaya_mail_action = working_himalaya
        failover = await mail_backends.execute_mail_action({})
        assert failover["status"] == "succeeded"
        assert failover["tool"] == "himalaya"
        assert calls == ["gog", "himalaya"]
        mail_backends._execute_gog_mail_action = original_gog
        mail_backends._execute_himalaya_mail_action = original_himalaya

        async def no_backends(_tool, _minimum):
            raise RuntimeError("not installed")

        mail_backends._read_tool_version = no_backends
        result = await mail_backends.execute_mail_action({})
        assert result["status"] == "failed"
        assert result["tool"] == ""
        assert "No supported mail backend" in result["error"]
        assert "gog is unavailable" not in result["error"]
    finally:
        mail_backends._read_tool_version = original_version
        if 'original_gog' in locals():
            mail_backends._execute_gog_mail_action = original_gog
            mail_backends._execute_himalaya_mail_action = original_himalaya
        if previous_backend is not None:
            os.environ["CUSTOMER_MAP_HERMES_MAIL_BACKEND"] = previous_backend


def _check_mail_backend_capability():
    original_which = mail_backends.shutil.which
    previous_backend = os.environ.pop("CUSTOMER_MAP_HERMES_MAIL_BACKEND", None)
    try:
        mail_backends.shutil.which = lambda tool: f"/bin/{tool}" if tool == "himalaya" else None
        assert mail_backends.mail_backend_capability() == ("declared", "himalaya")
        mail_backends.shutil.which = lambda _tool: None
        assert mail_backends.mail_backend_capability() == ("unavailable", "auto")
        mail_backends.shutil.which = lambda _tool: "/bin/tool"
        assert mail_backends.mail_backend_capability() == ("declared", "auto")
        mail_backends.shutil.which = lambda _tool: None
        os.environ["CUSTOMER_MAP_HERMES_MAIL_BACKEND"] = "gog"
        assert mail_backends.mail_backend_capability() == ("unavailable", "gog")
    finally:
        mail_backends.shutil.which = original_which
        if previous_backend is None:
            os.environ.pop("CUSTOMER_MAP_HERMES_MAIL_BACKEND", None)
        else:
            os.environ["CUSTOMER_MAP_HERMES_MAIL_BACKEND"] = previous_backend


async def _check_persistent_mail_action_idempotency():
    module = _load_adapter()
    with tempfile.TemporaryDirectory() as directory:
        previous_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = directory
        action = {
            "version": 1,
            "actionId": "c" * 32,
            "kind": "sendEmail",
            "account": "sender@example.com",
            "recipient": "buyer@example.com",
            "subject": "One delivery",
            "plainTextBody": "Body",
            "htmlBody": "",
        }
        action["bodyHash"] = module._mail_body_hash(action["recipient"], action["subject"], action["plainTextBody"], action["htmlBody"])
        calls = 0

        async def execute(_value):
            nonlocal calls
            calls += 1
            return {"status": "succeeded", "provider": "email", "tool": "himalaya", "messageId": "message-once", "error": "", "toolVersion": "2.0.0", "bodyMode": "plain", "exitCode": 0}

        module.execute_mail_action = execute
        try:
            first = await module.CustomerMapAdapter({})._run_direct_mail_action(action)
            second = await module.CustomerMapAdapter({})._run_direct_mail_action(action)
            assert first == second
            assert calls == 1
            assert (Path(directory) / ".customer-map-mail-actions.json").exists()
        finally:
            if previous_home is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = previous_home


async def _check_conversational_tool_boundary_fails_closed():
    module = _load_adapter()
    sys.modules["hermes_cli.tools_config"]._get_platform_tools = lambda _config, _platform: {"web", "terminal"}
    adapter = module.CustomerMapAdapter({})

    class WebSocket:
        closed = False

        def __init__(self):
            self.messages = []

        async def send_json(self, value):
            self.messages.append(value)

    async def reject_model_call(_event):
        raise AssertionError("unsafe toolsets must be rejected before Hermes model execution")

    adapter._ws = WebSocket()
    adapter.handle_message = reject_model_call
    await adapter._run_job({"id": "unsafe-tools", "timeoutMs": 10000, "request": {"sessionId": "unsafe", "input": []}})
    complete = adapter._ws.messages[-1]
    assert complete["type"] == "complete"
    assert "blocked unexpected toolsets: terminal" in complete["error"]


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
    temporary_home = tempfile.TemporaryDirectory()
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
        "HERMES_HOME": temporary_home.name,
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
        temporary_home.cleanup()


def _check_env_write():
    spec = importlib.util.spec_from_file_location("customer_map_connect_test", ROOT / "connect.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory() as directory:
        previous = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = directory
        try:
            module._write_env({"CUSTOMER_MAP_HERMES_SITE": "https://example.com", "CUSTOMER_MAP_HERMES_CONNECTION_ID": "abc"})
            module.ensure_customer_map_tool_boundary()
            text = (Path(directory) / ".env").read_text(encoding="utf-8")
            assert "CUSTOMER_MAP_HERMES_SITE=https://example.com" in text
            assert "CUSTOMER_MAP_HERMES_CONNECTION_ID=abc" in text
            config_text = (Path(directory) / "config.yaml").read_text(encoding="utf-8")
            assert "customer_map:" in config_text
            assert "- customer-map-readonly" in config_text
            assert "- no_mcp" in config_text
        finally:
            if previous is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = previous


def _check_safe_platform_composite():
    module = _load_adapter()
    module._register_safe_platform_toolset()
    definition = sys.modules["toolsets"].TOOLSETS["customer-map-readonly"]
    assert definition["tools"] == [
        "mcp__my_firecrawl__firecrawl_scrape", "mcp__my_firecrawl__firecrawl_search",
        "skill_view", "skills_list", "web_extract", "web_search",
    ]
    assert "terminal" not in definition["tools"]
    assert "skill_manage" not in definition["tools"]
    assert sys.modules["toolsets"].TOOLSETS["hermes-customer_map"] == definition
    assert "kanban_show" not in definition["tools"]
    assert "emailVerification" not in module._capabilities()


if __name__ == "__main__":
    _check_env_write()
    _check_safe_platform_composite()
    _check_safe_tool_activity()
    _check_tool_call_boundary()
    asyncio.run(_check_tool_activity_progress())
    _check_mail_backend_capability()
    asyncio.run(_check_async_final_response())
    asyncio.run(_check_streaming_response_frames())
    asyncio.run(_check_consecutive_session_turns())
    asyncio.run(_check_active_job_cancellation())
    asyncio.run(_check_completed_turn_without_notify_flag())
    asyncio.run(_check_direct_gog_send_action())
    asyncio.run(_check_direct_gog_draft_action())
    asyncio.run(_check_himalaya_backend_mapping())
    asyncio.run(_check_mail_backend_auto_detection())
    asyncio.run(_check_persistent_mail_action_idempotency())
    asyncio.run(_check_conversational_tool_boundary_fails_closed())
    asyncio.run(_check_rejects_stdin_body())
    asyncio.run(_check_websocket_reconnect())
    print("Hermes plugin checks passed")
