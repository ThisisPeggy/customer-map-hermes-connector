"""Deterministic, model-free mail backend adapters for Customer Map actions."""

import asyncio
import json
import os
import re
from email import policy
from email.message import EmailMessage

MIN_GOG_VERSION = (0, 11, 0)
MIN_HIMALAYA_VERSION = (2, 0, 0)
_VERSION_CACHE = {}


def configured_mail_backend():
    return str(os.getenv("CUSTOMER_MAP_HERMES_MAIL_BACKEND") or "gog").strip().lower()


async def execute_mail_action(action):
    backend = configured_mail_backend()
    if backend == "gog":
        return await _execute_gog_mail_action(action)
    if backend == "himalaya":
        return await _execute_himalaya_mail_action(action)
    return _backend_result(
        "failed",
        provider="email",
        tool=backend or "unknown",
        error=f"Unsupported Customer Map mail backend: {backend or 'empty'}.",
    )


async def _execute_gog_mail_action(action):
    try:
        tool_version = await _read_tool_version("gog", MIN_GOG_VERSION)
    except Exception as exc:
        return _backend_result("failed", provider="gmail", tool="gog", error=f"gog is unavailable or unsupported: {_compact_status(exc)}")
    args = _build_gog_mail_args(action)
    operation = "send" if action["kind"] == "sendEmail" else "draft create"
    mailbox = "Gmail Sent" if action["kind"] == "sendEmail" else "Gmail Drafts"
    process_result = await _run_process(args, timeout=90)
    if process_result["launchError"]:
        return _backend_result("failed", provider="gmail", tool="gog", tool_version=tool_version, error=f"gog {operation} could not start: {process_result['launchError']}")
    if process_result["timedOut"]:
        return _backend_result("needsConfirmation", provider="gmail", tool="gog", tool_version=tool_version, error=f"gog {operation} timed out; check {mailbox} before retrying.")
    output = process_result["output"]
    exit_code = process_result["exitCode"]
    message_id = _extract_message_id(output, action["kind"])
    if exit_code == 0 and message_id:
        return _backend_result("succeeded", provider="gmail", tool="gog", tool_version=tool_version, message_id=message_id, body_mode=_gog_body_mode(action), exit_code=exit_code)
    if exit_code == 0:
        return _backend_result("needsConfirmation", provider="gmail", tool="gog", tool_version=tool_version, error=f"gog {operation} returned success without a verifiable provider id; check {mailbox} before retrying.", body_mode=_gog_body_mode(action), exit_code=exit_code)
    return _backend_result("failed", provider="gmail", tool="gog", tool_version=tool_version, error=f"gog {operation} failed with exit code {exit_code}: {_compact_status(output)}", body_mode=_gog_body_mode(action), exit_code=exit_code)


async def _execute_himalaya_mail_action(action):
    try:
        tool_version = await _read_tool_version("himalaya", MIN_HIMALAYA_VERSION)
    except Exception as exc:
        return _backend_result("failed", provider="email", tool="himalaya", error=f"himalaya is unavailable or unsupported: {_compact_status(exc)}")
    try:
        raw_message, generated_message_id = _build_rfc5322_message(action)
        args = _build_himalaya_mail_args(action)
    except Exception as exc:
        return _backend_result("failed", provider="email", tool="himalaya", tool_version=tool_version, error=f"himalaya action is invalid: {_compact_status(exc)}")
    operation = "send" if action["kind"] == "sendEmail" else "draft add"
    process_result = await _run_process(args, timeout=90, stdin=raw_message)
    if process_result["launchError"]:
        return _backend_result("failed", provider="email", tool="himalaya", tool_version=tool_version, error=f"himalaya {operation} could not start: {process_result['launchError']}")
    if process_result["timedOut"]:
        return _backend_result("needsConfirmation", provider="email", tool="himalaya", tool_version=tool_version, error=f"himalaya {operation} timed out; check the configured mailbox for {generated_message_id} before retrying.", body_mode=_himalaya_body_mode(action))
    exit_code = process_result["exitCode"]
    output = process_result["output"]
    if exit_code == 0:
        message_id = _extract_message_id(output, action["kind"]) or generated_message_id
        return _backend_result("succeeded", provider="email", tool="himalaya", tool_version=tool_version, message_id=message_id, body_mode=_himalaya_body_mode(action), exit_code=exit_code)
    return _backend_result("failed", provider="email", tool="himalaya", tool_version=tool_version, error=f"himalaya {operation} failed with exit code {exit_code}: {_compact_status(output)}", body_mode=_himalaya_body_mode(action), exit_code=exit_code)


def _build_gog_mail_args(action):
    args = ["gog", "send"] if action["kind"] == "sendEmail" else ["gog", "gmail", "drafts", "create"]
    args.extend([f"--to={action['recipient']}", f"--subject={action['subject']}"])
    if action["htmlBody"]:
        args.append(f"--body-html={action['htmlBody']}")
    if action["plainTextBody"]:
        args.append(f"--body={action['plainTextBody']}")
    args.append(f"--account={action['account']}")
    if action["kind"] == "sendEmail":
        args.append("--force")
    args.extend(["--json", "--no-input"])
    return args


def _build_himalaya_mail_args(action):
    if action["kind"] == "sendEmail":
        args = ["himalaya", "message", "send"]
    else:
        mailbox = str(os.getenv("CUSTOMER_MAP_HERMES_HIMALAYA_DRAFT_MAILBOX") or "Drafts").strip()
        if not mailbox or "\n" in mailbox or "\r" in mailbox:
            raise ValueError("invalid Himalaya draft mailbox")
        args = ["himalaya", "message", "add", "--mailbox", mailbox, "--flag", "draft"]
    account = str(os.getenv("CUSTOMER_MAP_HERMES_HIMALAYA_ACCOUNT") or "").strip()
    config_path = str(os.getenv("CUSTOMER_MAP_HERMES_HIMALAYA_CONFIG") or "").strip()
    if account:
        args.extend(["--account", account])
    if config_path:
        args.extend(["--config", config_path])
    args.append("--json")
    return args


def _build_rfc5322_message(action):
    message = EmailMessage(policy=policy.SMTP)
    message_id = f"<customer-map-{action['actionId']}@{action['account'].split('@', 1)[1]}>"
    message["From"] = action["account"]
    message["To"] = action["recipient"]
    message["Subject"] = action["subject"]
    message["Message-ID"] = message_id
    message["X-Customer-Map-Action-ID"] = action["actionId"]
    message.set_content(action["plainTextBody"] or "This message contains HTML content.")
    if action["htmlBody"]:
        message.add_alternative(action["htmlBody"], subtype="html")
    return message.as_bytes(), message_id


async def _read_tool_version(tool, minimum):
    cached = _VERSION_CACHE.get(tool)
    if cached:
        return cached
    process_result = await _run_process([tool, "--version"], timeout=10)
    if process_result["launchError"]:
        raise RuntimeError(process_result["launchError"])
    match = re.search(r"v?(\d+)\.(\d+)\.(\d+)", process_result["output"])
    if process_result["exitCode"] != 0 or not match:
        raise RuntimeError(f"unable to read {tool} version")
    version_tuple = tuple(int(part) for part in match.groups())
    if version_tuple < minimum:
        required = ".".join(str(part) for part in minimum)
        raise RuntimeError(f"{tool} v{'.'.join(match.groups())} is older than v{required}")
    _VERSION_CACHE[tool] = ".".join(match.groups())
    return _VERSION_CACHE[tool]


async def _run_process(args, timeout, stdin=None):
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE if stdin is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(stdin), timeout=timeout)
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            return {"exitCode": None, "output": "", "timedOut": True, "launchError": ""}
    except FileNotFoundError:
        return {"exitCode": None, "output": "", "timedOut": False, "launchError": f"{args[0]} executable was not found"}
    except Exception as exc:
        return {"exitCode": None, "output": "", "timedOut": False, "launchError": _compact_status(exc)}
    output = "\n".join(part.decode("utf-8", errors="replace").strip() for part in (stdout, stderr) if part).strip()
    return {"exitCode": process.returncode, "output": output, "timedOut": False, "launchError": ""}


def _backend_result(status, *, provider, tool, message_id="", error="", tool_version="", body_mode="", exit_code=None):
    return {
        "status": status,
        "provider": provider,
        "tool": tool,
        "messageId": message_id,
        "error": error,
        "toolVersion": tool_version,
        "bodyMode": body_mode,
        "exitCode": exit_code,
    }


def _extract_message_id(text, kind="sendEmail"):
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
        found = _find_message_id(value, kind)
        if found:
            return found
    match = re.search(r'"(?:draftId|draft_id|messageId|message_id|id|uid)"\s*:\s*"?([^"\s,}]{1,})', text)
    return match.group(1) if match else ""


def _find_message_id(value, kind="sendEmail"):
    if isinstance(value, dict):
        keys = ("draftId", "draft_id", "id", "uid", "messageId", "message_id") if kind == "saveDraft" else ("messageId", "message_id", "id", "uid", "threadId", "thread_id")
        for key in keys:
            candidate = str(value.get(key) or "").strip()
            if candidate:
                return candidate
        for child in value.values():
            found = _find_message_id(child, kind)
            if found:
                return found
    if isinstance(value, list):
        for child in value:
            found = _find_message_id(child, kind)
            if found:
                return found
    return ""


def _gog_body_mode(action):
    if action.get("htmlBody") and action.get("plainTextBody"):
        return "body-html+body"
    if action.get("htmlBody"):
        return "body-html"
    return "body" if action.get("plainTextBody") else ""


def _himalaya_body_mode(action):
    return "multipart-alternative" if action.get("htmlBody") else "plain"


def _compact_status(content, max_length=320):
    text = " ".join(str(content or "").split())
    return text if len(text) <= max_length else text[:max_length - 3].rstrip() + "..."
