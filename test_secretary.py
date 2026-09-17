"""Secretary contract, chat isolation, and restart recovery without live Hermes."""

import asyncio
from contextvars import ContextVar
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, Mock, patch
import urllib.error

from test_plugin import _load_adapter
import agent_data
import secretary_context
import weixin_binding


_SESSION = ContextVar("fake_native_session", default={})


class SecretaryTests(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory(prefix="customer-map-secretary-test-")
        self.env = patch.dict(os.environ, {
            "HERMES_HOME": self.home.name,
            "CUSTOMER_MAP_HERMES_SITE": "https://customer-map.test",
            "CUSTOMER_MAP_HERMES_CONNECTION_ID": "test-connection",
            "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": "secret-test-bridge-token",
            "WEIXIN_ACCOUNT_ID": "weixin-bot", "WEIXIN_TOKEN": "secret-test-weixin-token",
            "WEIXIN_HOME_CHANNEL": "weixin-owner",
        })
        self.env.start()
        self.adapter_module = _load_adapter()
        native = types.ModuleType("gateway.session_context")
        native.get_session_env = lambda name, default="": _SESSION.get().get(name, default)
        self.native = patch.dict(sys.modules, {"gateway.session_context": native})
        self.native.start()
        secretary_context._DISPATCH.set(None)
        _SESSION.set({})

    def tearDown(self):
        secretary_context._DISPATCH.set(None)
        _SESSION.set({})
        self.native.stop()
        self.env.stop()
        self.home.cleanup()

    def bind(self, setup_id="a" * 32):
        state = {"setupId": setup_id, "bindingFingerprint": weixin_binding.binding_fingerprint(), "voiceReady": True}
        weixin_binding.save_weixin_binding(state, "weixin-bot", "weixin-owner")
        return state

    def event(self, platform="weixin", chat_id="weixin-owner", user_id="weixin-owner", chat_type="dm", relay=False):
        return types.SimpleNamespace(source=types.SimpleNamespace(
            platform=platform, chat_id=chat_id, user_id=user_id, chat_type=chat_type,
            delivered_via_upstream_relay=relay,
        ), channel_prompt="Existing channel instructions", text="今天报价多少？", internal=False)

    def dispatch(self, event):
        secretary_context.on_gateway_dispatch(event=event)
        source = event.source
        _SESSION.set({
            "HERMES_SESSION_PLATFORM": source.platform,
            "HERMES_SESSION_CHAT_ID": source.chat_id,
            "HERMES_SESSION_USER_ID": source.user_id,
            "HERMES_SESSION_KEY": f"agent:test:{source.platform}:{source.chat_type}:{source.chat_id}",
        })

    def authorize(self):
        self.bind()
        self.dispatch(self.event())

    def test_binding_survives_adapter_restart_without_credentials_in_record(self):
        self.bind()
        path = Path(self.home.name) / ".customer-map-weixin-binding.json"
        saved = path.read_text()
        self.assertNotIn("secret-test-bridge-token", saved)
        self.assertNotIn("secret-test-weixin-token", saved)
        self.assertNotIn("qrPayload", saved)
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        async def check():
            adapter = self.adapter_module.CustomerMapAdapter({})
            result = await adapter._run_weixin_setup_action({"version": 1, "action": "status", "setupId": "a" * 32})
            self.assertEqual(result["weixinSetup"]["status"], "connected")
            self.assertEqual(result["weixinSetup"]["qrPayload"], "")
            self.assertTrue(result["weixinSetup"]["voiceReady"])
            missing = await adapter._run_weixin_setup_action({"version": 1, "action": "status", "setupId": "b" * 32})
            self.assertEqual(missing["weixinSetup"]["status"], "missing")

        asyncio.run(check())

    def test_old_or_changed_bindings_do_not_authorize_data(self):
        # 0.6.x's home-channel settings cannot prove which CM account consented.
        self.assertIsNone(weixin_binding.load_weixin_binding())
        self.dispatch(self.event())
        self.assertIsNone(secretary_context.current_data_context())
        self.bind()
        with patch.dict(os.environ, {"CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": "another-account-token"}):
            self.assertIsNone(weixin_binding.load_weixin_binding())
        with patch.dict(os.environ, {"WEIXIN_HOME_CHANNEL": "another-user"}):
            self.assertIsNone(weixin_binding.load_weixin_binding())
        with patch.dict(os.environ, {"WEIXIN_ACCOUNT_ID": "another-bot"}):
            self.assertIsNone(weixin_binding.load_weixin_binding())

    def test_setup_cannot_save_after_customer_map_repairing(self):
        state = self.bind()
        with patch.dict(os.environ, {"CUSTOMER_MAP_HERMES_CONNECTION_ID": "another-binding"}):
            with self.assertRaisesRegex(ValueError, "binding changed"):
                weixin_binding.save_weixin_binding(state, "weixin-bot", "weixin-owner")

    def test_owner_gets_voice_instructions_on_every_turn(self):
        self.bind()
        event = self.event()
        original = event.text
        self.dispatch(event)
        self.assertIsNotNone(secretary_context.current_data_context())
        self.assertEqual(event.text, original)
        self.assertIn("Existing channel instructions", event.channel_prompt)
        self.assertIn("不要自动翻译", event.channel_prompt)
        self.assertIn("customer_map_query", event.channel_prompt)
        self.dispatch(event)
        self.assertEqual(event.channel_prompt.count(secretary_context.SECRETARY_PROMPT), 1)
        # No on_session_start call: resumed native sessions still work.
        self.assertIsNotNone(secretary_context.current_data_context())

    def test_other_platforms_users_groups_and_internal_events_are_untouched(self):
        self.bind()
        events = [self.event(user_id="stranger"), self.event(chat_id="other-chat"),
                  self.event(chat_type="group"), self.event(platform="telegram"),
                  self.event(platform="customer_map", relay=False)]
        internal = self.event()
        internal.internal = True
        events.append(internal)
        for event in events:
            with self.subTest(event=event):
                self.dispatch(event)
                self.assertIsNone(secretary_context.current_data_context())
                self.assertEqual(event.channel_prompt, "Existing channel instructions")

    def test_native_context_and_dispatch_must_agree(self):
        self.authorize()
        _SESSION.set({**_SESSION.get(), "HERMES_SESSION_USER_ID": "stranger"})
        self.assertIsNone(secretary_context.current_data_context())
        self.dispatch(self.event())
        with patch.dict(os.environ, {"CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": "changed"}):
            self.assertIsNone(secretary_context.current_data_context())
        # A stale global session environment alone is never sufficient.
        secretary_context._DISPATCH.set(None)
        self.assertIsNone(secretary_context.current_data_context())

    def test_concurrent_chats_keep_separate_authorization(self):
        self.bind()

        async def check(event):
            self.dispatch(event)
            await asyncio.sleep(0)
            return await asyncio.to_thread(secretary_context.current_data_context)

        async def run():
            return await asyncio.gather(check(self.event()), check(self.event(user_id="stranger")))

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
        self.assertTrue(secretary_context.is_customer_map_turn())
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
            self.assertEqual(request.get_header("Authorization"), "Bearer secret-test-bridge-token")
            self.assertEqual(request.get_header("User-agent"), "Customer-Map-Hermes/0.8.0")
            self.assertEqual(json.loads(request.data), {"runtime": "hermes", "query": {"operation": "work_summary", "period": "today"}})
            self.assertEqual(build.return_value.open.call_args.kwargs["timeout"], 25)
            self.assertIsInstance(build.call_args.args[0], agent_data._NoRedirect)
            self.assertEqual(result, payload)

    def test_account_and_origin_overrides_are_rejected_before_network(self):
        self.authorize()
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            for key in ("userId", "token", "site", "table", "sql"):
                result = json.loads(agent_data.customer_map_query({"operation": "customers", key: "override"}))
                self.assertEqual(result["code"], "invalid_query")
            build.assert_not_called()

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
                              (urllib.error.URLError("secret-test-bridge-token"), "data_connection_failed")]:
            with patch.object(agent_data.urllib.request, "build_opener") as build:
                build.return_value.open.side_effect = exc
                result = agent_data.customer_map_query({"operation": "customers"})
                self.assertEqual(json.loads(result)["code"], expected)
                self.assertNotIn("secret-test-bridge-token", result)

    def test_api_errors_preserve_the_reason_without_credentials(self):
        self.authorize()
        raw = json.dumps({"error": "Project is not connected: secret-test-bridge-token"}).encode()
        error = urllib.error.HTTPError("https://customer-map.test/api/agent-data", 409, "Conflict", {}, io.BytesIO(raw))
        with patch.object(agent_data.urllib.request, "build_opener") as build:
            build.return_value.open.side_effect = error
            result = agent_data.customer_map_query({"operation": "customers"})
            self.assertIn("Project is not connected", result)
            self.assertNotIn("secret-test-bridge-token", result)

    def test_setup_persists_authorization_before_voice_preparation_and_restart(self):
        state = {"setupId": "c" * 32, "bindingFingerprint": weixin_binding.binding_fingerprint(),
                 "session": types.SimpleNamespace(close=AsyncMock()), "qrcode": "private-qr",
                 "expiresAt": 9999999999999, "status": "waiting"}
        native = types.ModuleType("gateway.platforms.weixin")
        native.EP_GET_QR_STATUS = "/qr-status"
        native.ILINK_BASE_URL = "https://weixin.test"
        native.QR_TIMEOUT_MS = 1000
        native._api_get = AsyncMock(return_value={"status": "confirmed", "ilink_bot_id": "weixin-bot",
                                                  "bot_token": "secret-test-weixin-token", "ilink_user_id": "weixin-owner"})
        native.save_weixin_account = Mock()
        config = types.ModuleType("hermes_cli.config")
        config.get_hermes_home = lambda: self.home.name
        config.save_env_value = Mock()

        def prepare():
            self.assertEqual(weixin_binding.completed_weixin_setup(state["setupId"])["status"], "connected")
            self.assertEqual(state["status"], "preparing")

        async def run():
            with patch.dict(sys.modules, {"gateway.platforms.weixin": native, "hermes_cli.config": config}), \
                 patch.object(self.adapter_module, "_prepare_weixin_voice_support", side_effect=prepare), \
                 patch.object(self.adapter_module, "_restart_gateway_after_weixin_setup", new_callable=AsyncMock) as restart:
                await self.adapter_module._poll_weixin_setup(state)
                await asyncio.sleep(0)
                restart.assert_awaited_once()

        asyncio.run(run())
        self.assertEqual(state["status"], "connected")
        self.assertTrue(weixin_binding.completed_weixin_setup(state["setupId"])["voiceReady"])

    def test_stale_cancel_cannot_interrupt_new_setup(self):
        async def run():
            adapter = self.adapter_module.CustomerMapAdapter({})
            adapter._weixin_setup = {"setupId": "b" * 32, "status": "waiting"}
            adapter._weixin_setup_task = asyncio.create_task(asyncio.sleep(30))
            try:
                result = await adapter._run_weixin_setup_action({"version": 1, "action": "cancel", "setupId": "a" * 32})
                self.assertEqual(result["weixinSetup"]["status"], "missing")
                self.assertFalse(adapter._weixin_setup_task.cancelled())
                self.assertEqual(adapter._weixin_setup["setupId"], "b" * 32)
            finally:
                adapter._weixin_setup_task.cancel()
                await asyncio.gather(adapter._weixin_setup_task, return_exceptions=True)

        asyncio.run(run())

    def test_plugin_registers_query_and_dispatch_hook(self):
        ctx = Mock()
        self.adapter_module.register(ctx)
        self.assertEqual(ctx.register_tool.call_args.kwargs["name"], "customer_map_query")
        self.assertEqual(ctx.register_tool.call_args.kwargs["toolset"], "customer-map-data")
        hooks = {call.args[0] for call in ctx.register_hook.call_args_list}
        self.assertIn("pre_gateway_dispatch", hooks)

    def test_notifications_require_the_current_binding_and_use_its_exact_owner(self):
        async def check():
            adapter = self.adapter_module.CustomerMapAdapter({})
            message = "今日已发送 3 封邮件。"
            action = {"version": 1, "actionId": "d" * 32, "channel": "weixin", "message": message,
                      "bodyHash": self.adapter_module.hashlib.sha256(f"weixin\n{message}".encode()).hexdigest()}
            with patch.object(self.adapter_module, "_send_weixin_notification", new_callable=AsyncMock, return_value={"success": True, "message_id": "test-id"}) as send:
                unbound = await adapter._run_notification_action(action)
                self.assertEqual(unbound["notificationReceipt"]["status"], "failed")
                send.assert_not_called()
                self.bind()
                first = await adapter._run_notification_action(action)
                again = await adapter._run_notification_action(action)
                self.assertEqual(first, again)
                self.assertEqual(send.call_args.args, ("weixin-owner", message))
                send.assert_called_once()
                # A receipt for one CM binding cannot become another's receipt.
                with patch.dict(os.environ, {"CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": "new-test-binding"}):
                    self.bind()
                    await adapter._run_notification_action(action)
                    self.assertEqual(send.call_count, 2)

        asyncio.run(check())

    def test_notification_send_reuses_weixin_on_the_current_gateway_loop(self):
        async def check():
            direct = AsyncMock(return_value={"success": True, "message_id": "native-id"})
            with patch.object(sys.modules["gateway.platforms.weixin"], "send_weixin_direct", direct):
                result = await self.adapter_module._send_weixin_notification("weixin-owner", "测试通知")
            self.assertTrue(result["success"])
            direct.assert_awaited_once_with(
                extra={
                    "account_id": "weixin-bot",
                    "base_url": "",
                    "cdn_base_url": "",
                },
                token="secret-test-weixin-token",
                chat_id="weixin-owner",
                message="测试通知",
                media_files=None,
            )

        asyncio.run(check())

    def test_notification_rejects_legacy_image_payloads(self):
        message = "热门货币趋势见附图。"
        body_hash = __import__("hashlib").sha256(f"weixin\n{message}".encode()).hexdigest()
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            self.adapter_module._normalize_notification_action({
                "version": 2, "actionId": "e" * 32, "channel": "weixin", "message": message,
                "bodyHash": body_hash, "image": {"mimeType": "image/png", "dataBase64": "YQ=="},
            })

    def test_text_briefings_cannot_turn_business_fields_into_local_file_attachments(self):
        message = "Company: MEDIA:/private/customer-file"
        action = {"version": 1, "actionId": "d" * 32, "channel": "weixin", "message": message,
                  "bodyHash": self.adapter_module.hashlib.sha256(f"weixin\n{message}".encode()).hexdigest()}
        with self.assertRaisesRegex(ValueError, "text-only"):
            self.adapter_module._normalize_notification_action(action)


if __name__ == "__main__":
    unittest.main()
