"""Per-dispatch Customer Map data access, isolated across chats."""

import hashlib
import json
import os
from contextlib import contextmanager
from contextvars import ContextVar


_DISPATCH = ContextVar("customer_map_dispatch", default=None)
_AUTHENTICATED_SOURCE = ContextVar("customer_map_authenticated_source", default=None)


def _binding_fingerprint():
    values = [os.getenv(name, "").strip() for name in (
        "CUSTOMER_MAP_HERMES_SITE", "CUSTOMER_MAP_HERMES_CONNECTION_ID", "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN",
    )]
    if not all(values):
        return ""
    values[0] = values[0].rstrip("/")
    return hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest()


@contextmanager
def authenticated_workspace_source(source):
    token = _AUTHENTICATED_SOURCE.set(source)
    try:
        yield
    finally:
        _AUTHENTICATED_SOURCE.reset(token)


def on_gateway_dispatch(**kwargs):
    # Capture the connector's authenticated job before native authorization; do not read data here.
    _DISPATCH.set(None)
    event = kwargs.get("event")
    source = getattr(event, "source", None)
    if not source or getattr(event, "internal", False):
        return
    platform = getattr(source, "platform", None)
    platform = str(getattr(platform, "value", platform) or "")
    if platform != "customer_map" or source is not _AUTHENTICATED_SOURCE.get():
        return
    chat_id = str(getattr(source, "chat_id", "") or "")
    user_id = str(getattr(source, "user_id", "") or "")
    if getattr(source, "chat_type", "") != "dm" or not chat_id or not user_id:
        return
    fingerprint = _binding_fingerprint()
    if fingerprint:
        _DISPATCH.set((platform, chat_id, user_id, fingerprint))


def current_data_context():
    """Require both our trusted dispatch and Hermes' concurrent session context.

    Checking the dispatch proof also prevents get_session_env's legacy global
    environment fallback from authorizing a CLI, cron, or unrelated chat.
    """
    dispatch = _DISPATCH.get()
    if not dispatch or dispatch[3] != _binding_fingerprint():
        return None
    try:
        from gateway.session_context import get_session_env
    except ImportError:
        return None
    actual = tuple(str(get_session_env(name, "") or "") for name in (
        "HERMES_SESSION_PLATFORM", "HERMES_SESSION_CHAT_ID", "HERMES_SESSION_USER_ID",
    ))
    if actual != dispatch[:3] or not get_session_env("HERMES_SESSION_KEY", ""):
        return None
    return dispatch


def is_customer_map_turn():
    context = current_data_context()
    return bool(context and context[0] == "customer_map")

