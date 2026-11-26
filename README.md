# Customer Map for Hermes

This Hermes platform plugin connects a user-owned Hermes Agent to Customer Map through an outbound WebSocket. No public Hermes port or API key is required. It reconnects automatically after temporary network or relay interruptions. Customer Map polls queued/running relay jobs automatically and can run another foreground turn when Hermes explicitly returns `continue: true`. A timed-out task is terminal and does not continue in the background.

The `customer_map_query` tool reads live business records only for requests from the paired Customer Map workspace.

Version 0.12.2 removes the separate Customer Map WeChat setup, notifications, file delivery, and WeChat-only business tools. Use Hermes' own messaging setup for ordinary WeChat conversations. Updating does not reset native Hermes credentials or the Customer Map pairing.

Version 0.10.0 adds read-only discovery of connected mailboxes plus bounded inbox search and single-message reading. Only mailboxes enabled in Customer Map settings are available. Search and read never send mail.

Version 0.12.0 returns research evidence tied to each Customer Map request. Only the native Hermes `web_search` and `web_extract` tools supply this evidence. A URL must have been requested and successfully read; a link in model output alone is not evidence. Missing or ambiguous session bindings and missing tool evidence fail closed. This records what the tools read, not whether the source or the model's claim is true.

Customer Map workbench requests retain a fail-closed allowlist, including on restored sessions: read-only business queries, built-in web search and extraction, and installed skill loading. A configured Firecrawl MCP server contributes only `firecrawl_search` and non-interactive, public-URL `firecrawl_scrape`. The scoped tool bridge cannot bypass these checks, and native tools on other Hermes platforms are not reconfigured.

## Business query API

Deploy Customer Map's matching `/api/agent-data` implementation before using 0.7.0 queries. Requests use `POST {"runtime":"hermes","query":{...}}` and the bound bridge token in the Authorization header. The server determines the account and calculates totals; the model cannot override identity, credentials, endpoints, tables, or SQL. Requests have a 40-second timeout and a bounded response size, and never follow redirects with the credential.

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
| `mailboxes` | Connected mailbox IDs, providers, addresses and access granted to Hermes |
| `inbox_search` | Live read-only search across enabled connected inboxes; summaries only |
| `inbox_message` | Live content for one message selected by mailbox ID and message ID |

Date ranges use the user's saved timezone unless explicitly overridden. Inbox search defaults to the last 30 days and can use `all` when a narrower search needs older mail. Search and message bodies are returned only for the current request and are not copied into customer records. A created quote may be a draft; detected replies cover synced events, not every mailbox. Current inquiry-state customers are queryable, but independent inquiry counts are unavailable. Charts, inquiry creation and custom task management are not implemented by this version. Customer-owned Supabase projects that the website server cannot access return an explicit error instead of querying a different project.

The tool checks both per-dispatch routing and Hermes' concurrent session context on every call. It refuses unrelated chats, groups, contextless calls and stale bindings, even if the tool is visible in another platform's tool catalog. It never reads data during the pre-dispatch hook, which runs before native gateway authorization.

## Updating

Update with `hermes plugins update customer-map-platform`, then restart the intended Hermes profile. This preserves the existing pairing and enabled state unless Hermes' plugin security scan disables the plugin.

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
hermes plugins update customer-map-platform
hermes gateway restart
```

Connector 0.12.1 uses Hermes' built-in YAML support when available. If your Gateway runs without PyYAML, update the Connector and restart Gateway; an existing pairing is kept.

During local development:

```bash
mkdir -p ~/.hermes/plugins/customer-map-platform
cp plugin.yaml __init__.py adapter.py connect.py mail_backends.py tool_boundary.py agent_data.py dispatch_context.py ~/.hermes/plugins/customer-map-platform/
python3 ~/.hermes/plugins/customer-map-platform/connect.py --site https://your-customer-map.example --code CMAP-HERMES-...
hermes gateway restart
```

Check the installation on the intended Hermes machine with:

```bash
hermes plugins list
```

Run source tests independently of any installed profile (requires `aiohttp` and `PyYAML`):

```bash
python3 -B test_data_context.py
python3 -B test_plugin.py
```

These suites isolate profile state in temporary directories. The reconnect test additionally listens on a temporary loopback socket. No suite pairs or operates a live Hermes profile.
