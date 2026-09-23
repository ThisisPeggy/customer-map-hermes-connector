#!/usr/bin/env python3
"""Pair the local Hermes profile with Customer Map and save plugin env values."""

import argparse
import json
import os
import re
import socket
import tempfile
import urllib.request
from pathlib import Path
from urllib.parse import urlparse
import uuid

try:
    from .tool_boundary import ensure_customer_map_tool_boundary
except ImportError:
    from tool_boundary import ensure_customer_map_tool_boundary

PLUGIN_VERSION = "0.9.0"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--new-instance", action="store_true", help="Create a separate identity after copying a Hermes profile to another computer")
    args = parser.parse_args()
    site = args.site.rstrip("/")
    _require_safe_url(site, {"https"}, {"http"})
    client_id = _profile_client_id(new_instance=args.new_instance)
    request = urllib.request.Request(
        f"{site}/api/hermes-link",
        data=json.dumps({"action": "claim", "code": args.code, "clientId": client_id, "pluginVersion": PLUGIN_VERSION, "deviceName": socket.gethostname()[:60]}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": f"CustomerMap-Hermes/{PLUGIN_VERSION}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.load(response)
    except Exception as exc:
        raise SystemExit(f"Customer Map pairing failed: {exc}")
    _require_safe_url(str(data.get("relayUrl") or ""), {"wss"}, {"ws"})
    values = {
        "CUSTOMER_MAP_HERMES_SITE": site,
        "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN": data["bridgeToken"],
        "CUSTOMER_MAP_HERMES_CONNECTION_ID": data["connectionId"],
        "CUSTOMER_MAP_HERMES_RELAY_URL": data["relayUrl"],
        "CUSTOMER_MAP_HERMES_CLIENT_ID": client_id,
        "CUSTOMER_MAP_HERMES_ALLOW_ALL_USERS": "true",
    }
    _write_env(values)
    try:
        ensure_customer_map_tool_boundary()
    except Exception as exc:
        raise SystemExit(f"Hermes was paired, but Customer Map tool isolation could not be configured: {exc}")
    print("Hermes is paired with Customer Map. Restart the Hermes gateway to connect.")
    print("  hermes gateway restart")


def _profile_client_id(home=None, new_instance=False):
    """Reconnect the same profile without creating an orphan cloud device."""
    if not new_instance:
        profile = Path(home) if home is not None else Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")
        env_path = profile / ".env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key.strip() == "CUSTOMER_MAP_HERMES_CLIENT_ID":
                    value = value.strip().strip("\"'")
                    if re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
                        return value
    return str(uuid.uuid4())


def _require_safe_url(value, secure_schemes, local_schemes):
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    if parsed.scheme in secure_schemes and host:
        return
    if parsed.scheme in local_schemes and host in {"localhost", "127.0.0.1", "::1"}:
        return
    raise SystemExit(f"Refusing insecure Customer Map URL: {value}")


def _write_env(values):
    home = Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")
    home.mkdir(parents=True, exist_ok=True)
    env_path = home / ".env"
    existing = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    remaining = {str(key): str(value) for key, value in values.items()}
    output = []
    for line in existing:
        key = line.split("=", 1)[0].strip() if "=" in line and not line.lstrip().startswith("#") else ""
        if key in remaining:
            output.append(f"{key}={_env_value(remaining.pop(key))}")
        else:
            output.append(line)
    output.extend(f"{key}={_env_value(value)}" for key, value in remaining.items())
    fd, temp_path = tempfile.mkstemp(prefix=".env.", dir=home)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write("\n".join(output).rstrip() + "\n")
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, env_path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def _env_value(value):
    if any(char in value for char in "\r\n"):
        raise SystemExit("Customer Map returned an invalid multiline setting")
    return value


if __name__ == "__main__":
    main()
