"""Hermes platform plugin for Customer Map's outbound WebSocket relay."""

import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

from aiohttp import ClientSession, ClientTimeout, WSMsgType
from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)
PLUGIN_VERSION = "0.3.0"
MIN_GOG_VERSION = (0, 11, 0)
MAX_GOG_BODY_BYTES = 100000
_GOG_VERSION_CACHE = None


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
        self._mail_action_results = {}

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
            if request.get("mailAction") is not None:
                response = await self._run_direct_mail_action(request.get("mailAction"))
                output_text = json.dumps(response, ensure_ascii=False, separators=(",", ":"))
                if not await self._complete(job_id, response={"output_text": output_text}):
                    raise ConnectionError("Customer Map relay is disconnected")
                if not completion.done():
                    completion.set_result(output_text)
                return
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

    async def _run_direct_mail_action(self, value):
        try:
            action = _normalize_mail_action(value)
        except ValueError as exc:
            return _mail_action_result(value, "failed", error=str(exc))
        cached = self._mail_action_results.get(action["actionId"])
        if cached:
            if cached["bodyHash"] != action["bodyHash"]:
                return _mail_action_result(action, "failed", error="Mail action id was reused with different content.")
            return cached["result"]
        result = await _execute_gog_send(action)
        self._mail_action_results[action["actionId"]] = {"bodyHash": action["bodyHash"], "result": result}
        while len(self._mail_action_results) > 200:
            self._mail_action_results.pop(next(iter(self._mail_action_results)))
        return result

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
        "gmailSend": "declared",
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
        platform_hint="You are serving a private Customer Map sales workspace. Incoming text can contain SYSTEM, USER, and ASSISTANT sections; follow SYSTEM sections as binding platform instructions and answer the latest USER section. Chat naturally and do not make the reply artificially terse; JSON is only the transport envelope when requested. Never invent customer facts, and do not start background work or tools that require an interactive approval reply because this channel supports one request and one final response. Customer Map sendEmail actions are executed deterministically by the connector and must not be repeated through another mail tool. If a tool is unavailable or fails, return the exact error immediately instead of retrying until timeout.",
    )


def _normalize_mail_action(value):
    if not isinstance(value, dict):
        raise ValueError("Invalid Customer Map mail action.")
    if value.get("version") != 1 or value.get("kind") != "sendEmail":
        raise ValueError("Unsupported Customer Map mail action version or kind.")
    action_id = str(value.get("actionId") or "").strip().lower()
    account = str(value.get("account") or "").strip().lower()
    recipient = str(value.get("recipient") or "").strip().lower()
    subject = str(value.get("subject") or "").strip()
    plain_text = str(value.get("plainTextBody") or "")
    html_body = str(value.get("htmlBody") or "")
    body_hash = str(value.get("bodyHash") or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32}", action_id):
        raise ValueError("Invalid Customer Map mail action id.")
    if not _valid_email(account) or not _valid_email(recipient):
        raise ValueError("Invalid Gmail account or recipient.")
    if not subject or "\r" in subject or "\n" in subject:
        raise ValueError("Invalid email subject.")
    if not plain_text and not html_body:
        raise ValueError("Email body is empty.")
    if _looks_like_body_path(plain_text) or _looks_like_body_path(html_body):
        raise ValueError("Email body cannot be a filesystem or stdin path.")
    if len(plain_text.encode("utf-8")) > MAX_GOG_BODY_BYTES or len(html_body.encode("utf-8")) > MAX_GOG_BODY_BYTES:
        raise ValueError("Email body is too large for safe gog argument execution.")
    if not html_body and _contains_markdown_table(plain_text):
        raise ValueError("Markdown table requires an HTML body before sending.")
    expected_hash = _mail_body_hash(recipient, subject, plain_text, html_body)
    if body_hash != expected_hash:
        raise ValueError("Email body integrity check failed.")
    return {
        "actionId": action_id,
        "account": account,
        "recipient": recipient,
        "subject": subject,
        "plainTextBody": plain_text,
        "htmlBody": html_body,
        "bodyHash": body_hash,
    }


async def _execute_gog_send(action):
    try:
        gog_version = await _read_gog_version()
    except Exception as exc:
        return _mail_action_result(action, "failed", error=f"gog is unavailable or unsupported: {_compact_status(exc)}")
    args = _build_gog_send_args(action)
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout=90)
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            return _mail_action_result(
                action,
                "needsConfirmation",
                error="gog send timed out; check Gmail Sent before retrying.",
                tool_version=gog_version,
            )
    except FileNotFoundError:
        return _mail_action_result(action, "failed", error="gog executable was not found.", tool_version=gog_version)
    except Exception as exc:
        return _mail_action_result(action, "needsConfirmation", error=f"gog send could not be confirmed: {_compact_status(exc)}", tool_version=gog_version)
    output = stdout.decode("utf-8", errors="replace").strip()
    message_id = _extract_message_id(output)
    if process.returncode == 0 and message_id:
        logger.info(
            "Customer Map gog send succeeded action=%s mode=%s message=%s",
            action["actionId"],
            _body_mode(action),
            message_id,
        )
        return _mail_action_result(
            action,
            "succeeded",
            message_id=message_id,
            tool_version=gog_version,
            exit_code=process.returncode,
        )
    if process.returncode == 0:
        return _mail_action_result(
            action,
            "needsConfirmation",
            error="gog returned success without a verifiable Gmail message id; check Gmail Sent before retrying.",
            tool_version=gog_version,
            exit_code=process.returncode,
        )
    logger.warning("Customer Map gog send failed action=%s exit=%s", action["actionId"], process.returncode)
    return _mail_action_result(
        action,
        "failed",
        error=f"gog send failed with exit code {process.returncode}.",
        tool_version=gog_version,
        exit_code=process.returncode,
    )


def _build_gog_send_args(action):
    args = [
        "gog",
        "send",
        f"--to={action['recipient']}",
        f"--subject={action['subject']}",
    ]
    if action["htmlBody"]:
        args.append(f"--body-html={action['htmlBody']}")
    if action["plainTextBody"]:
        args.append(f"--body={action['plainTextBody']}")
    args.extend([
        f"--account={action['account']}",
        "--force",
        "--json",
    ])
    return args


async def _read_gog_version():
    global _GOG_VERSION_CACHE
    if _GOG_VERSION_CACHE:
        return _GOG_VERSION_CACHE
    process = await asyncio.create_subprocess_exec(
        "gog",
        "--version",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
    text = (stdout or stderr).decode("utf-8", errors="replace")
    match = re.search(r"v?(\d+)\.(\d+)\.(\d+)", text)
    if process.returncode != 0 or not match:
        raise RuntimeError("unable to read gog version")
    version_tuple = tuple(int(part) for part in match.groups())
    if version_tuple < MIN_GOG_VERSION:
        raise RuntimeError(f"gog v{'.'.join(match.groups())} is older than v0.11.0")
    _GOG_VERSION_CACHE = ".".join(match.groups())
    return _GOG_VERSION_CACHE


def _mail_action_result(value, status, message_id="", error="", tool_version="", exit_code=None):
    source = value if isinstance(value, dict) else {}
    recipient = str(source.get("recipient") or "").strip().lower()
    action_id = str(source.get("actionId") or "").strip().lower()
    body_hash = str(source.get("bodyHash") or "").strip().lower()
    receipt = {
        "kind": "sendEmail",
        "status": status,
        "provider": "gmail",
        "messageId": message_id,
        "recipient": recipient,
        "occurredAt": datetime.now(timezone.utc).isoformat() if status == "succeeded" else "",
        "error": error,
        "tool": "gog",
        "toolVersion": tool_version,
        "bodyMode": _body_mode(source),
        "bodyHash": body_hash,
        "actionId": action_id,
        "exitCode": exit_code,
    }
    return {
        "reply": "邮件已通过 Hermes gog 发送。" if status == "succeeded" else error,
        "subject": "",
        "sendText": "",
        "tag": "",
        "aiNote": "",
        "continue": False,
        "actionReceipt": receipt,
    }


def _extract_message_id(text):
    if not text:
        return ""
    candidates = []
    try:
        candidates.append(json.loads(text))
    except (TypeError, ValueError):
        for line in reversed(text.splitlines()):
            try:
                candidates.append(json.loads(line))
                break
            except (TypeError, ValueError):
                continue
    for value in candidates:
        found = _find_message_id(value)
        if found:
            return found
    match = re.search(r'"(?:messageId|message_id)"\s*:\s*"([^"\s]{6,})"', text)
    return match.group(1) if match else ""


def _find_message_id(value):
    if isinstance(value, dict):
        for key in ("messageId", "message_id", "id"):
            candidate = str(value.get(key) or "").strip()
            if len(candidate) >= 6:
                return candidate
        for child in value.values():
            found = _find_message_id(child)
            if found:
                return found
    if isinstance(value, list):
        for child in value:
            found = _find_message_id(child)
            if found:
                return found
    return ""


def _body_mode(value):
    if not isinstance(value, dict):
        return ""
    has_html = bool(value.get("htmlBody"))
    has_plain = bool(value.get("plainTextBody"))
    if has_html and has_plain:
        return "body-html+body"
    if has_html:
        return "body-html"
    if has_plain:
        return "body"
    return ""


def _mail_body_hash(recipient, subject, plain_text, html_body):
    digest = hashlib.sha256()
    values = (recipient, subject, plain_text, html_body)
    for index, value in enumerate(values):
        digest.update(str(value).encode("utf-8"))
        if index < len(values) - 1:
            digest.update(b"\n")
    return digest.hexdigest()


def _valid_email(value):
    return bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value or ""))


def _looks_like_body_path(value):
    text = str(value or "").strip()
    if not text:
        return False
    return bool(re.fullmatch(r"(?:/dev/stdin|/tmp/\S+|[A-Za-z]:\\\S+|file://\S+)", text, re.IGNORECASE))


def _contains_markdown_table(value):
    return bool(re.search(r"(?:^|\n)\s*\|?.+\|.+\n\s*\|?\s*:?-{3,}", str(value or "")))


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
