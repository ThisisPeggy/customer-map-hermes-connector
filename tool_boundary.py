"""Fail-closed Hermes tool isolation for Customer Map conversational turns."""

import os
import stat
import tempfile
from pathlib import Path

SAFE_PLATFORM_TOOLSETS = ["web", "no_mcp"]
ALLOWED_EFFECTIVE_TOOLSETS = {"web"}
ALLOWED_EFFECTIVE_TOOLS = {"web_search", "web_extract"}


def ensure_customer_map_tool_boundary():
    """Persist a web-only, no-MCP allowlist for the Customer Map platform."""
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to configure Hermes tool isolation.") from exc

    home = Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")
    home.mkdir(parents=True, exist_ok=True)
    config_path = home / "config.yaml"
    try:
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    except Exception as exc:
        raise RuntimeError(f"Cannot read Hermes config.yaml: {exc}") from exc
    config = loaded if isinstance(loaded, dict) else {}
    platform_toolsets = config.get("platform_toolsets")
    if not isinstance(platform_toolsets, dict):
        platform_toolsets = {}
        config["platform_toolsets"] = platform_toolsets
    platform_toolsets["customer_map"] = list(SAFE_PLATFORM_TOOLSETS)

    # Hermes defaults newly discovered plugin toolsets to enabled. Mark every
    # currently loaded plugin toolset as known for this platform so an absent
    # entry remains disabled. The runtime assertion below still fails closed if
    # Hermes exposes any unexpected toolset later.
    discovered = _loaded_plugin_toolsets()
    known_map = config.get("known_plugin_toolsets")
    if not isinstance(known_map, dict):
        known_map = {}
        config["known_plugin_toolsets"] = known_map
    existing = known_map.get("customer_map")
    known = {str(value) for value in existing} if isinstance(existing, list) else set()
    known.update(discovered)
    known_map["customer_map"] = sorted(known)

    serialized = yaml.safe_dump(config, sort_keys=False, allow_unicode=True)
    current = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    if current == serialized:
        return False
    mode = stat.S_IMODE(config_path.stat().st_mode) if config_path.exists() else 0o600
    fd, temp_path = tempfile.mkstemp(prefix="config.yaml.", dir=home)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
        os.chmod(temp_path, mode)
        os.replace(temp_path, config_path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    return True


def assert_customer_map_tool_boundary():
    """Verify Hermes will expose only the connector's safe web toolset."""
    try:
        from gateway.run import _load_gateway_config
        from hermes_cli.tools_config import _get_platform_tools
        from toolsets import resolve_toolset

        config = _load_gateway_config()
        effective = set(_get_platform_tools(config, "customer_map"))
        effective_tools = {
            tool
            for toolset in effective
            for tool in resolve_toolset(toolset)
        }
    except Exception as exc:
        raise RuntimeError(f"Cannot verify Customer Map Hermes tool isolation: {exc}") from exc
    unexpected = sorted(effective - ALLOWED_EFFECTIVE_TOOLSETS)
    if unexpected:
        raise RuntimeError(
            "Customer Map Hermes tool isolation is not active; blocked unexpected toolsets: "
            + ", ".join(unexpected)
            + ". Re-run the Customer Map Hermes pairing command and restart the gateway."
        )
    unexpected_tools = sorted(effective_tools - ALLOWED_EFFECTIVE_TOOLS)
    if unexpected_tools:
        raise RuntimeError(
            "Customer Map Hermes tool isolation is not active; blocked unexpected tools: "
            + ", ".join(unexpected_tools)
            + ". Re-run the Customer Map Hermes pairing command and restart the gateway."
        )
    return effective


def _loaded_plugin_toolsets():
    try:
        from hermes_cli.tools_config import _get_plugin_toolset_keys

        return {str(value) for value in _get_plugin_toolset_keys()}
    except Exception:
        return set()
