# Customer Map for Hermes

This Hermes platform plugin connects a user-owned Hermes Agent to Customer Map through an outbound WebSocket. No public Hermes port or API key is required. It reconnects automatically after temporary network or relay interruptions. Customer Map polls queued/running relay jobs automatically and can run another foreground turn when Hermes explicitly returns `continue: true`. A timed-out task is terminal and does not continue in the background.

Version 0.5.10 keeps ordinary Customer Map turns on a fail-closed, read-only allowlist, including overriding Hermes' full-core fallback for unknown plugin platforms. Built-in web search and extraction remain available. If the user has an MCP server named with `firecrawl`, only its standard `firecrawl_search` and non-interactive `firecrawl_scrape` tools are exposed; the connector does not change that server's configuration or API key. Hermes' scoped `tool_search`, `tool_describe`, and `tool_call` bridge may load those deferred Firecrawl tools without bypassing the same target and argument checks. Scrape calls are limited to public HTTP(S) URLs. Installed skills can be listed and loaded through `skills_list` and `skill_view`, while `skill_manage` remains blocked. Raw `session_search` is not exposed because Hermes does not currently provide a Customer Map-only source filter. Terminal, files, code execution, delegation, kanban, cron, arbitrary MCP, memory writes, and model-driven mail commands remain unavailable. Customer Map mail actions bypass the model and enter dedicated adapters.

The connector still streams Hermes response drafts, reports visible research/skill activity, and supports true task cancellation: stopping a turn cancels the matching in-flight Hermes session instead of only stopping browser polling. When Firecrawl is absent or unavailable, Hermes can continue with the built-in web tools.

The default backend is `auto`: the connector executes the structured mail action through the locally available Hermes mail adapters. If an adapter is installed but not configured, it safely tries the next adapter. It never asks the model or Customer Map to select a tool. A timeout or uncertain result never falls through, which prevents duplicate delivery. An explicit override remains available for troubleshooting:

```bash
CUSTOMER_MAP_HERMES_MAIL_BACKEND=himalaya
CUSTOMER_MAP_HERMES_HIMALAYA_ACCOUNT=your-himalaya-account-name # optional; uses the default account when omitted
CUSTOMER_MAP_HERMES_HIMALAYA_DRAFT_MAILBOX=Drafts              # optional mailbox or alias
```

If no supported adapter is available, the connector returns a backend-neutral configuration error instead of assuming gog. Additional user mail tools require a deterministic Connector adapter that implements the same receipt contract.

The `gog` adapter uses fixed arguments and never uses a shell. The Himalaya adapter constructs an RFC 5322 message in memory and supplies it directly over the process stdin supported by Himalaya; no shell, temporary body file, or model-generated command is involved. Both adapters return the same verified receipt contract, and successful or uncertain actions are cached by `actionId` so relay retries do not repeat delivery.

Tested with Hermes Agent v0.18.2. Users on older releases should update Hermes before installing the plugin.

Install the plugin with Hermes, use the one-time pairing command shown by Customer Map, then restart the Hermes gateway.

```bash
hermes plugins install https://github.com/ThisisPeggy/customer-map-hermes-connector --enable
```

During local development:

```bash
mkdir -p ~/.hermes/plugins/customer-map-platform
cp plugin.yaml __init__.py adapter.py connect.py mail_backends.py tool_boundary.py ~/.hermes/plugins/customer-map-platform/
python3 ~/.hermes/plugins/customer-map-platform/connect.py --site https://your-customer-map.example --code CMAP-HERMES-...
hermes gateway restart
```

Verify the installation with:

```bash
hermes plugins list
python3 ~/.hermes/plugins/customer-map-platform/test_plugin.py
```
