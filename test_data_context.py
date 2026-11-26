"""Customer Map query contract and dispatch isolation without live Hermes."""

import asyncio
from contextvars import ContextVar
import io
import json
import os
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
import urllib.error

from test_plugin import _load_adapter
import agent_data
import dispatch_context as context


_SESSION = ContextVar("fake_native_session", default={})
TEST_BRIDGE_TOKEN = "test-" + "bridge-" + "credential"


class DataContextTests(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory(prefix="customer-map-data-test-")
        self.env = patch.dict(os.environ, {
            "HERMES_HOME": self.home.name,
            "CUSTOMER_MAP_HERMES_SITE": "https://customer-map.test",
            "CUSTOMER_MAP_HERMES_CONNECTION_ID": "test-connection",
            "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": TEST_BRIDGE_TOKEN,
        })
        self.env.start()
        self.adapter_module = _load_adapter()
        native = types.ModuleType("gateway.session_context")
        native.get_session_env = lambda name, default="": _SESSION.get().get(name, default)
        self.native = patch.dict(sys.modules, {"gateway.session_context": native})
        self.native.start()
        context._DISPATCH.set(None)
        _SESSION.set({})

    def tearDown(self):
        context._DISPATCH.set(None)
        _SESSION.set({})
        self.native.stop()
        self.env.stop()
        self.home.cleanup()

    def event(self, platform="customer_map", chat_id="cm-chat", user_id="cm-user", chat_type="dm", relay=True):
        return types.SimpleNamespace(source=types.SimpleNamespace(
            platform=platform, chat_id=chat_id, user_id=user_id, chat_type=chat_type,
            delivered_via_upstream_relay=relay,
        ), channel_prompt="Existing channel instructions", text="今天报价多少？", internal=False)

    def dispatch(self, event):
        context.on_gateway_dispatch(event=event)
        source = event.source
        _SESSION.set({
            "HERMES_SESSION_PLATFORM": source.platform,
            "HERMES_SESSION_CHAT_ID": source.chat_id,
            "HERMES_SESSION_USER_ID": source.user_id,
            "HERMES_SESSION_KEY": f"agent:test:{source.platform}:{source.chat_type}:{source.chat_id}",
        })

    def authorize(self):
        self.dispatch(self.event())

    def test_other_platforms_groups_and_untrusted_dispatches_are_untouched(self):
        events = [self.event(platform="weixin"), self.event(chat_type="group"), self.event(platform="telegram"), self.event(relay=False)]
        internal = self.event()
        internal.internal = True
        events.append(internal)
        for event in events:
            with self.subTest(event=event):
                self.dispatch(event)
                self.assertIsNone(context.current_data_context())
                self.assertEqual(event.channel_prompt, "Existing channel instructions")

    def test_native_context_and_dispatch_must_agree(self):
        self.authorize()
        _SESSION.set({**_SESSION.get(), "HERMES_SESSION_USER_ID": "stranger"})
        self.assertIsNone(context.current_data_context())
        self.dispatch(self.event())
        with patch.dict(os.environ, {"CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": "changed"}):
            self.assertIsNone(context.current_data_context())
        # A stale global session environment alone is never sufficient.
        context._DISPATCH.set(None)
        self.assertIsNone(context.current_data_context())

    def test_concurrent_chats_keep_separate_authorization(self):
        async def check(event):
            self.dispatch(event)
            await asyncio.sleep(0)
            return await asyncio.to_thread(context.current_data_context)

        async def run():
            return await asyncio.gather(check(self.event()), check(self.event(platform="weixin")))

        owner, stranger = asyncio.run(run())
        self.assertIsNotNone(owner)
        self.assertIsNone(stranger)

    def test_data_tool_cannot_bypass_scoping_through_tool_call(self):
        for name, args in [("customer_map_query", {"operation": "customers"}),
                           ("tool_call", {"name": "customer_map_query", "arguments": '{"operation":"customers"}'})]:
            result = self.adapter_module._on_pre_tool_call(session_id="foreign", tool_name=name, args=args)
            self.assertEqual(result["action"], "block")
        self.authorize()
        self.assertIsNone(self.adapter_module._on_pre_tool_call(session_id="restored", tool_name="customer_map_query", args={"operation": "customers"}))

    def test_restored_customer_map_sessions_keep_mail_and_terminal_blocked(self):
        self.dispatch(self.event(platform="customer_map", chat_id="cm-chat", user_id="cm-user", relay=True))
        self.assertTrue(context.is_customer_map_turn())
        for name in ("terminal", "send_email", "send_message", "delegate_task"):
            result = self.adapter_module._on_pre_tool_call(session_id="not-seen-at-start", tool_name=name, args={})
            self.assertEqual(result["action"], "block")
        self.assertIsNone(self.adapter_module._on_pre_tool_call(session_id="not-seen-at-start", tool_name="customer_map_query", args={"operation": "customers"}))

    def test_queries_send_only_bound_credentials_and_typed_body(self):
        self.authorize()
        payload = {"ok": True, "version": 1, "operation": "work_summary", "metrics": {"sentEmails": 5}, "coverage": {"mail": "partial"}}
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            build.return_value.open.return_value = io.BytesIO(json.dumps(payload).encode())
            result = json.loads(agent_data.customer_map_query({"operation": "work_summary", "period": "today"}, task_id="test"))
            request = build.return_value.open.call_args.args[0]
            self.assertEqual(request.full_url, "https://customer-map.test/api/agent-data")
            self.assertEqual(request.get_header("Authorization"), f"Bearer {TEST_BRIDGE_TOKEN}")
            self.assertEqual(request.get_header("User-agent"), "Customer-Map-Hermes/0.10.0")
            self.assertEqual(json.loads(request.data), {"runtime": "hermes", "query": {"operation": "work_summary", "period": "today"}})
            self.assertEqual(build.return_value.open.call_args.kwargs["timeout"], 40)
            self.assertIsInstance(build.call_args.args[0], agent_data._NoRedirect)
            self.assertEqual(result, payload)

    def test_account_and_origin_overrides_are_rejected_before_network(self):
        self.authorize()
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            for key in ("userId", "token", "site", "table", "sql"):
                result = json.loads(agent_data.customer_map_query({"operation": "customers", key: "override"}))
                self.assertEqual(result["code"], "invalid_query")
            build.assert_not_called()

    def test_inbox_query_schema_has_server_scoped_message_locators(self):
        properties = agent_data.QUERY_SCHEMA["parameters"]["properties"]
        self.assertIn("mailboxes", agent_data.OPERATIONS)
        self.assertIn("inbox_search", agent_data.OPERATIONS)
        self.assertIn("inbox_message", agent_data.OPERATIONS)
        self.assertEqual(properties["messageId"]["maxLength"], 1000)

    def test_unauthorized_query_has_no_network_side_effect(self):
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            result = json.loads(agent_data.customer_map_query({"operation": "customers"}))
            self.assertEqual(result["code"], "data_access_denied")
            build.assert_not_called()

    def test_site_must_be_fixed_origin_and_redirects_do_not_forward_token(self):
        for origin in ("https://user:password@customer-map.test", "https://customer-map.test/path", "https://customer-map.test?next=evil", "http://customer-map.test", "file:///tmp/example"):
            with self.subTest(origin=origin), patch.dict(os.environ, {"CUSTOMER_MAP_HERMES_SITE": origin}):
                with self.assertRaises(ValueError):
                    agent_data._endpoint()
        redirect = agent_data._NoRedirect()
        self.assertIsNone(redirect.redirect_request(None, None, 302, "Moved", {}, "https://other.test"))
        self.authorize()
        error = urllib.error.HTTPError("https://customer-map.test/api/agent-data", 302, "Moved", {}, io.BytesIO(b""))
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            build.return_value.open.side_effect = error
            result = json.loads(agent_data.customer_map_query({"operation": "customers"}))
            self.assertEqual(result["code"], "redirect_blocked")
            build.return_value.open.assert_called_once()

    def test_outdated_or_large_responses_and_network_errors_are_honest(self):
        self.authorize()
        for raw, expected in [(b"<html>old website</html>", "data_api_unavailable"),
                              (b"x" * (agent_data.MAX_RESPONSE_BYTES + 1), "response_too_large"),
                              (b'{"ok":true,"version":2,"operation":"customers"}', "data_api_unavailable")]:
            with self.subTest(expected=expected), patch.object(agent_data.urllib.request, "build_opener") as build:
                build.return_value.open.return_value = io.BytesIO(raw)
                result = json.loads(agent_data.customer_map_query({"operation": "customers"}))
                self.assertEqual(result["code"], expected)
                self.assertNotIn("items", result)
        for exc, expected in [(TimeoutError(), "data_request_timeout"),
                              (urllib.error.URLError(TEST_BRIDGE_TOKEN), "data_connection_failed")]:
            with patch.object(agent_data.urllib.request, "build_opener") as build:
                build.return_value.open.side_effect = exc
                result = agent_data.customer_map_query({"operation": "customers"})
                self.assertEqual(json.loads(result)["code"], expected)
                self.assertNotIn(TEST_BRIDGE_TOKEN, result)

    def test_api_errors_preserve_the_reason_without_credentials(self):
        self.authorize()
        raw = json.dumps({"error": f"Project is not connected: {TEST_BRIDGE_TOKEN}"}).encode()
        error = urllib.error.HTTPError("https://customer-map.test/api/agent-data", 409, "Conflict", {}, io.BytesIO(raw))
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            build.return_value.open.side_effect = error
            result = agent_data.customer_map_query({"operation": "customers"})
            self.assertIn("Project is not connected", result)
            self.assertNotIn(TEST_BRIDGE_TOKEN, result)

    def test_plugin_registers_query_and_dispatch_hook(self):
        ctx = Mock()
        self.adapter_module.register(ctx)
        registered = {call.kwargs["name"]: call.kwargs["toolset"] for call in ctx.register_tool.call_args_list}
        self.assertEqual(registered, {
            "customer_map_query": "customer-map-data",
        })
        hooks = {call.args[0] for call in ctx.register_hook.call_args_list}
        self.assertIn("pre_gateway_dispatch", hooks)

if __name__ == "__main__":
    unittest.main()
