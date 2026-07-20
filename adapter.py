"""Hermes platform plugin for Customer Map's outbound WebSocket relay."""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime

from aiohttp import ClientSession, ClientTimeout, WSMsgType
from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)
PLUGIN_VERSION = "0.2.5"


class CustomerMapAdapter(BasePlatformAdapter):
    supports_async_delivery = False

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("customer_map"))
        self.site = os.getenv("CUSTOMER_MAP_HERMES_SITE", "").rstrip("/")
        self.bridge_token = os.getenv("CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "")
        self.connection_id = os.getenv("CUSTOMER_MAP_HERMES_CONNECTION_ID", "")
        self.relay_url = os.getenv("CUSTOMER_MAP_HERMES_RELAY_URL", "")
        self.client_id = os.getenv("CUSTOMER_MAP_HERMES_CLIENT_ID", "") or str(uuid.uuid4())
        self._http = None
        self._ws = None
        self._receive_task = None
        self._reconnect_task = None
        self._stopping = False
        self._connect_lock = asyncio.Lock()
        self._pending = {}
        self._job_tasks = set()

    @property
    def name(self):
        return "Customer Map"

    async def connect(self, *, is_reconnect=False):
        self._stopping = False
        if not all((self.site, self.bridge_token, self.connection_id, self.relay_url)):
            self._set_fatal_error("config_missing", "Run the Customer Map Hermes connect command first.", retryable=False)
            return False
        return await self._open_connection(report_error=True)

    async def disconnect(self):
        self._stopping = True
        self._mark_disconnected()
        if self._reconnect_task and self._reconnect_task is not asyncio.current_task():
            self._reconnect_task.cancel()
        if self._receive_task and self._receive_task is not asyncio.current_task():
            self._receive_task.cancel()
        for task in list(self._job_tasks):
            task.cancel()
        self._fail_pending("Customer Map relay disconnected")
        await self._close_transport()
        self._receive_task = self._reconnect_task = None

    async def _open_connection(self, report_error=False):
        async with self._connect_lock:
            if self._ws and not self._ws.closed:
                return True
            await self._close_transport()
            try:
                self._http = ClientSession(timeout=ClientTimeout(total=None, connect=15))
                self._ws = await self._http.ws_connect(self.relay_url, heartbeat=30)
                await self._ws.send_json({
                    "type": "hello",
                    "runtime": "hermes",
                    "bridgeToken": self.bridge_token,
                    "connectionId": self.connection_id,
                    "clientId": self.client_id,
                    "pluginVersion": PLUGIN_VERSION,
                    "capabilities": _capabilities(),
                })
                ready = await asyncio.wait_for(self._ws.receive_json(), timeout=15)
                if ready.get("type") != "ready":
                    raise RuntimeError(ready.get("error") or "Customer Map relay rejected the connection")
                self._receive_task = asyncio.create_task(self._receive_loop())
                self._mark_connected()
                return True
            except Exception as exc:
                logger.warning("Customer Map relay connection failed: %s", exc)
                await self._close_transport()
                if report_error:
                    self._set_fatal_error("connect_failed", str(exc), retryable=True)
                return False

    async def _close_transport(self):
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._http and not self._http.closed:
            await self._http.close()
        self._ws = self._http = None

    async def _receive_loop(self):
        try:
            async for message in self._ws:
                if message.type != WSMsgType.TEXT:
                    if message.type in {WSMsgType.CLOSED, WSMsgType.ERROR}:
                        break
                    continue
                payload = json.loads(message.data)
                if payload.get("type") == "ping":
                    await self._ws.send_json({"type": "pong"})
                elif payload.get("type") == "job":
                    task = asyncio.create_task(self._run_job(payload.get("job") or {}))
                    self._job_tasks.add(task)
                    task.add_done_callback(self._job_tasks.discard)
                elif payload.get("type") == "error":
                    logger.warning("Customer Map relay error: %s", payload.get("error"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Customer Map relay receive loop stopped: %s", exc)
        finally:
            self._mark_disconnected()
            self._fail_pending("Customer Map relay disconnected")
            await self._close_transport()
            if not self._stopping and (not self._reconnect_task or self._reconnect_task.done()):
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        delay = 2
        while not self._stopping:
            await asyncio.sleep(delay)
            if await self._open_connection():
                logger.info("Customer Map relay reconnected")
                return
            delay = min(delay * 2, 60)

    async def _run_job(self, job):
        job_id = str(job.get("id") or "")
        request = job.get("request") if isinstance(job.get("request"), dict) else {}
        session_id = str(request.get("sessionId") or job_id)
        if not job_id:
            return
        existing = self._pending.get(session_id)
        if existing:
            # A follow-up can reach the connector immediately after the previous
            # final message. Give that completed turn a brief chance to release
            # the session instead of rejecting an automatic continuation.
            deadline = asyncio.get_running_loop().time() + 5
            while self._pending.get(session_id) is existing and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.05)
        if session_id in self._pending:
            await self._complete(job_id, error="This Customer Map Hermes session is already processing a task.")
            return
        completion = asyncio.get_running_loop().create_future()
        self._pending[session_id] = {"job_id": job_id, "completion": completion, "last_content": "", "last_metadata": {}}
        try:
            source = self.build_source(
                chat_id=session_id,
                chat_name="Customer Map",
                chat_type="dm",
                user_id=str(request.get("sessionKey") or "customer-map"),
                user_name="Customer Map user",
            )
            source.delivered_via_upstream_relay = True
            event = MessageEvent(
                text=_request_text(request.get("input")),
                message_type=MessageType.TEXT,
                source=source,
                message_id=job_id,
                timestamp=datetime.now(),
            )
            await self.handle_message(event)
            timeout_seconds = max(5, min(float(job.get("timeoutMs") or 120000) / 1000 - 2, 598))
            await asyncio.wait_for(completion, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            pending = self._pending.get(session_id) or {}
            last_content = str(pending.get("last_content") or "").strip()
            if _looks_like_final_response(last_content):
                await self._complete(job_id, response={"output_text": last_content})
            else:
                detail = _compact_status(last_content)
                message = "Hermes did not return a final message before the task timed out. The task has stopped and will not continue in the background."
                if detail:
                    message += f" Last Hermes status: {detail}"
                await self._complete(job_id, error=message)
        except Exception as exc:
            await self._complete(job_id, error=str(exc))
        finally:
            self._pending.pop(session_id, None)

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        pending = self._pending.get(str(chat_id))
        if not pending:
            return SendResult(success=False, error="No Customer Map job is waiting for this session")
        job_id = pending["job_id"]
        pending["last_content"] = str(content)
        pending["last_metadata"] = metadata if isinstance(metadata, dict) else {}
        await self._progress(job_id, str(content))
        if not isinstance(metadata, dict) or metadata.get("notify") is not True:
            return SendResult(success=True, message_id=job_id)
        if not await self._complete(job_id, response={"output_text": str(content)}):
            return SendResult(success=False, error="Customer Map relay is disconnected", retryable=True)
        completion = pending["completion"]
        if not completion.done():
            completion.set_result(str(content))
        return SendResult(success=True, message_id=job_id)

    async def on_processing_complete(self, event, outcome):
        pending = self._pending.get(str(event.source.chat_id))
        if not pending:
            return
        completion = pending["completion"]
        last_content = str(pending.get("last_content") or "").strip()
        if last_content:
            if await self._complete(pending["job_id"], response={"output_text": last_content}):
                if not completion.done():
                    completion.set_result(last_content)
                return
        if not completion.done():
            detail = _outcome_error(outcome)
            completion.set_exception(RuntimeError(detail or "Hermes completed without returning a final text response."))

    async def send_typing(self, chat_id, metadata=None):
        return None

    async def get_chat_info(self, chat_id):
        return {"name": "Customer Map", "type": "dm", "chat_id": str(chat_id)}

    async def _complete(self, job_id, response=None, error=""):
        if not self._ws or self._ws.closed:
            return False
        await self._ws.send_json({"type": "complete", "jobId": job_id, "response": response, "error": error, "pluginVersion": PLUGIN_VERSION})
        return True

    async def _progress(self, job_id, content):
        if not self._ws or self._ws.closed:
            return False
        text = str(content or "").strip()
        if not text:
            return True
        await self._ws.send_json({"type": "progress", "jobId": job_id, "content": text[:100000], "pluginVersion": PLUGIN_VERSION})
        return True

    def _fail_pending(self, message):
        for pending in list(self._pending.values()):
            completion = pending["completion"]
            if not completion.done():
                completion.set_exception(ConnectionError(message))


def _request_text(value):
    messages = value if isinstance(value, list) else []
    parts = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "user").upper()
        content = str(message.get("content") or "").strip()
        if content:
            parts.append(f"{role}:\n{content}")
    return "\n\n".join(parts) or "Respond to the Customer Map task."


def _enabled():
    required = ("CUSTOMER_MAP_HERMES_SITE", "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "CUSTOMER_MAP_HERMES_CONNECTION_ID", "CUSTOMER_MAP_HERMES_RELAY_URL")
    return all(os.getenv(name, "").strip() for name in required)


def _capabilities():
    return {
        "runtime": "verified",
        "customerMapData": "verified",
        "sessionContext": "verified",
        "webRead": "unknown",
        "webSearch": "unknown",
        "gmailDraft": "unknown",
        "gmailSend": "unknown",
        "memory": "unknown",
    }


def _env_enablement():
    return {} if _enabled() else None


def register(ctx):
    ctx.register_platform(
        name="customer_map",
        label="Customer Map",
        adapter_factory=lambda cfg: CustomerMapAdapter(cfg),
        check_fn=_enabled,
        validate_config=lambda cfg: _enabled(),
        is_connected=lambda cfg: _enabled(),
        required_env=["CUSTOMER_MAP_HERMES_SITE", "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "CUSTOMER_MAP_HERMES_CONNECTION_ID", "CUSTOMER_MAP_HERMES_RELAY_URL"],
        allow_all_env="CUSTOMER_MAP_HERMES_ALLOW_ALL_USERS",
        env_enablement_fn=_env_enablement,
        max_message_length=100000,
        emoji="🗺️",
        pii_safe=True,
        platform_hint="You are serving a private Customer Map sales workspace. Incoming text can contain SYSTEM, USER, and ASSISTANT sections; follow SYSTEM sections as binding platform instructions and answer the latest USER section. Chat naturally and do not make the reply artificially terse; JSON is only the transport envelope when requested. Never invent customer facts, and do not start background work or tools that require an interactive approval reply because this channel supports one request and one final response. For email tools, pass the literal email body to body/html/content parameters; never pass a temporary path such as /tmp/email_body.html as the message body. If a tool is unavailable or fails, return the exact error immediately instead of retrying until timeout.",
    )


def _looks_like_final_response(content):
    if not content:
        return False
    try:
        parsed = json.loads(content)
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict) and "reply" in parsed and ("continue" in parsed or "actionReceipt" in parsed)


def _compact_status(content, max_length=320):
    text = " ".join(str(content or "").split())
    return text if len(text) <= max_length else text[:max_length - 3].rstrip() + "..."


def _outcome_error(outcome):
    if isinstance(outcome, dict):
        for key in ("error", "message", "reason"):
            value = _compact_status(outcome.get(key))
            if value:
                return value
    for key in ("error", "message", "reason"):
        value = _compact_status(getattr(outcome, key, ""))
        if value:
            return value
    return ""
