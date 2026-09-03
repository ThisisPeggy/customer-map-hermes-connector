"""Fail-closed Hermes tool isolation for Customer Map conversational turns."""

import os
import stat
import tempfile
from pathlib import Path

SAFE_TOOLSET_NAME = "customer-map-readonly"
PLATFORM_DEFAULT_TOOLSET_NAME = "hermes-customer_map"
SAFE_PLATFORM_TOOLSETS = [SAFE_TOOLSET_NAME, "no_mcp"]
# Hermes reverse-maps the complete built-in web subset from this composite.
ALLOWED_EFFECTIVE_TOOLSETS = {SAFE_TOOLSET_NAME, "web"}
BASE_ALLOWED_EFFECTIVE_TOOLS = {
    "web_search",
    "web_extract",
    "skills_list",
    "skill_view",
}
FIRECRAWL_READ_TOOLS = {"firecrawl_search", "firecrawl_scrape"}


def allowed_effective_tools():
    return BASE_ALLOWED_EFFECTIVE_TOOLS | _registered_firecrawl_read_tools()


def firecrawl_operation(tool_name):
    if tool_name not in _registered_firecrawl_read_tools():
        return ""
    return next((name for name in FIRECRAWL_READ_TOOLS if tool_name == name or tool_name.endswith(f"__{name}")), "")


def register_customer_map_toolset():
    from toolsets import TOOLSETS

    definition = {
        "description": "Customer Map read-only web research and installed skill loading",
        "tools": sorted(allowed_effective_tools()),
        "includes": [],
    }
    # Unknown plugin platforms otherwise resolve their hermes-<platform>
    # default to the full core tool universe before explicit settings are
    # applied, which lets non-configurable sets such as kanban be recovered.
    TOOLSETS[PLATFORM_DEFAULT_TOOLSET_NAME] = definition
    TOOLSETS[SAFE_TOOLSET_NAME] = definition


def _registered_firecrawl_read_tools():
    try:
        from tools.registry import registry

        if hasattr(registry, "get_all_entries"):
            registered = ((entry.name, entry.toolset) for entry in registry.get_all_entries())
        else:
            registered = ((name, registry.get_toolset_for_tool(name)) for name in registry.get_all_tool_names())
        matches = set()
        for registered_name, registered_toolset in registered:
            toolset = str(registered_toolset or "").lower()
            name = str(registered_name or "")
            if "firecrawl" not in toolset:
                continue
            if any(name == raw or name.endswith(f"__{raw}") for raw in FIRECRAWL_READ_TOOLS):
                matches.add(name)
        return matches
    except Exception:
        return set()


def ensure_customer_map_tool_boundary():
    """Persist Customer Map's read-only research allowlist."""
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
    """Verify Hermes will expose only Customer Map's read-only tools."""
    try:
        from gateway.run import _load_gateway_config
        from hermes_cli.tools_config import _get_platform_tools
        from toolsets import resolve_toolset

        register_customer_map_toolset()
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
    unexpected_tools = sorted(effective_tools - allowed_effective_tools())
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
