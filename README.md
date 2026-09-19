# Customer Map for Hermes

This Hermes platform plugin connects a user-owned Hermes Agent to Customer Map through an outbound WebSocket. No public Hermes port or API key is required. It reconnects automatically after temporary network or relay interruptions. Customer Map polls queued/running relay jobs automatically and can run another foreground turn when Hermes explicitly returns `continue: true`. A timed-out task is terminal and does not continue in the background.

Version 0.7.0 adds the `customer_map_query` tool for live, read-only business queries from the Customer Map workspace and its authorized Weixin owner chat. Every owner-chat turn receives secretary instructions: understand text or a voice transcript as the user's request, query real records when needed, and answer directly. Translation is performed only when requested. Native Hermes still handles speech recognition and Weixin delivery; this plugin does not promise native outbound voice bubbles.

Successful QR authorization is saved before voice preparation and survives gateway restarts. The record is tied to the current Customer Map site, connection credential, Weixin bot, and scanning user, and contains no raw tokens or QR payload. Changing the Customer Map binding does not authorize an old Weixin recipient to read the new account. Notifications require the same binding, target that exact recipient, and deduplicate successful delivery per binding and action ID. Model-authored local attachment directives remain rejected.

Version 0.9.0 accepts integrity-bound file notifications from the paired Customer Map origin. Files are downloaded with the Connector binding token into a private temporary directory, delivered to the bound Weixin owner, and deleted immediately afterward. Arbitrary URLs, redirects, unsupported file types, and files larger than 15 MB are rejected. The Connector does not send voice replies.

The plugin retains SILK decoder and local speech-to-text preparation during QR setup, followed by an automatic gateway restart. Direct messages are restricted to the scanning user. Ordinary Customer Map turns retain a fail-closed allowlist, including on restored sessions: read-only business queries, confirmation-gated Customer Map actions, built-in web search/extraction, and installed skill loading. The action tool can stage customers, products, quotations, PIs, Task Assistant campaigns, email replies, and quote-file delivery; the server, not the model, validates and executes them after explicit confirmation. A configured Firecrawl MCP server contributes only `firecrawl_search` and non-interactive, public-URL `firecrawl_scrape`. The scoped `tool_search`, `tool_describe`, and `tool_call` bridge cannot bypass these checks. Terminal, arbitrary local files, code execution, delegation, kanban, cron, arbitrary MCP, memory writes, raw session search, and unconfirmed model-driven mail remain unavailable on the Customer Map platform. Native tools on unrelated Hermes platforms are not reconfigured.

## Business query API

Deploy Customer Map's matching `/api/agent-data` implementation before using 0.7.0 queries. Requests use `POST {"runtime":"hermes","query":{...}}` and the bound bridge token in the Authorization header. The server determines the account and calculates totals; the model cannot override identity, credentials, endpoints, tables, or SQL. Requests have a 25-second timeout and a bounded response size, and never follow redirects with the credential.

| Operation | Records returned |
| --- | --- |
| `capabilities` | Available queries and source coverage |
| `work_summary` | Exact new-customer, sent-mail, detected-reply, bounce, created-quote and completed-follow-up counts |
| `customers` | Company/country/current-status filters, IDs and pagination |
| `customer_detail` | One customer's business information |
| `product_lists` | Product-list pricing settings and, for one list ID, bounded product rows |
| `quotes` | Quotes for a customer; a quote ID includes line items |
| `follow_ups` | Scheduled customer and quote tasks, due dates and overdue filtering |
| `mail_activity` | Sent, detected-reply or recorded-bounce events |

Date ranges use the user's saved timezone unless explicitly overridden. A created quote may be a draft; detected replies cover synced events, not every mailbox. Current inquiry-state customers are queryable, but independent inquiry counts are unavailable. Charts, inquiry creation and custom task management are not implemented by this version. Customer-owned Supabase projects that the website server cannot access return an explicit error instead of querying a different project.

The tool checks both per-dispatch routing and Hermes' concurrent session context on every call. It refuses unrelated chats, groups, contextless calls and stale bindings, even if the tool is visible in another platform's tool catalog. It never reads data during the pre-dispatch hook, which runs before native gateway authorization.

## Upgrading from 0.6.x

Update with `hermes plugins update customer-map-platform --enable`, then restart the intended Hermes profile. `--enable` means the updated plugin remains enabled immediately; the user does not need to enable it again. Use **Authorize again / 重新扫码授权** in Customer Map's WeChat secretary settings once when upgrading an old 0.6.x authorization. No new Customer Map pairing is needed if the existing bridge binding is unchanged. An administrator's separate, explicit toolset-deny policy still takes precedence.

WeChat's authorization page currently calls this iLink connection **OpenClaw** (`bot_type=3` in the native QR request). Hermes still handles the conversation. That name is supplied by WeChat, not this plugin's label. When WeChat displays a replacement warning, confirming the new connection disconnects the previously linked assistant for that WeChat account.

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

For later releases, update without losing the enabled state:

```bash
hermes plugins update customer-map-platform --enable
hermes gateway restart
```

During local development:

```bash
mkdir -p ~/.hermes/plugins/customer-map-platform
cp plugin.yaml __init__.py adapter.py connect.py mail_backends.py tool_boundary.py agent_data.py operations.py secretary_context.py weixin_binding.py ~/.hermes/plugins/customer-map-platform/
python3 ~/.hermes/plugins/customer-map-platform/connect.py --site https://your-customer-map.example --code CMAP-HERMES-...
hermes gateway restart
```

Check the installation on the intended Hermes machine with:

```bash
hermes plugins list
```

Run source tests independently of any installed profile (requires `aiohttp` and `PyYAML`):

```bash
python3 -B test_secretary.py
python3 -B test_plugin.py
```

Both suites isolate profile state in temporary directories and mock messages, credentials, voice preparation and gateway restarts. The existing reconnect test additionally listens on a temporary loopback socket. No suite pairs or operates a live Hermes profile.
