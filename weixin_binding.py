"""Profile-local proof that a Weixin owner authorized this Customer Map binding."""

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def binding_fingerprint():
    values = [os.getenv(name, "").strip() for name in (
        "CUSTOMER_MAP_HERMES_SITE", "CUSTOMER_MAP_HERMES_CONNECTION_ID", "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN",
    )]
    if not all(values):
        return ""
    values[0] = values[0].rstrip("/")
    return hashlib.sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest()


def _binding_path():
    return Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes") / ".customer-map-weixin-binding.json"


def load_weixin_binding():
    """Never adopt a legacy home channel as proof for an unrelated CM account."""
    try:
        path = _binding_path()
        if path.stat().st_size > 16_384:
            return None
        record = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(record, dict) or record.get("version") != 1:
            return None
        if not record.get("fingerprint") or record["fingerprint"] != binding_fingerprint():
            return None
        if not re.fullmatch(r"[0-9a-f]{32}", str(record.get("setupId") or "")):
            return None
        if not record.get("accountId") or record["accountId"] != os.getenv("WEIXIN_ACCOUNT_ID", ""):
            return None
        if not record.get("userId") or record["userId"] != os.getenv("WEIXIN_HOME_CHANNEL", ""):
            return None
        if not os.getenv("WEIXIN_TOKEN", ""):
            return None
        return record
    except (OSError, ValueError, TypeError):
        return None


def save_weixin_binding(state, account_id, user_id):
    fingerprint = binding_fingerprint()
    if not fingerprint or state.get("bindingFingerprint") != fingerprint:
        raise ValueError("Customer Map binding changed during Weixin authorization. Start again.")
    if not re.fullmatch(r"[0-9a-f]{32}", str(state.get("setupId") or "")) or not account_id or not user_id:
        raise ValueError("Incomplete Weixin authorization result.")
    record = {
        "version": 1, "fingerprint": fingerprint, "setupId": state["setupId"],
        "accountId": account_id, "userId": user_id,
        "authorizedAt": state.setdefault("authorizedAt", datetime.now(timezone.utc).isoformat()),
        "voiceReady": state.get("voiceReady") is True,
    }
    path = _binding_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, separators=(",", ":"))
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return record


def completed_weixin_setup(setup_id=""):
    record = load_weixin_binding()
    if not record or (setup_id and setup_id != record["setupId"]):
        return None
    return {
        "status": "connected", "setupId": record["setupId"], "qrPayload": "", "expiresAt": 0,
        "error": "", "voiceReady": record.get("voiceReady") is True,
    }
