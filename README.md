# Customer Map for Hermes

This Hermes platform plugin connects a user-owned Hermes Agent to Customer Map through an outbound WebSocket. No public Hermes port or API key is required. It reconnects automatically after temporary network or relay interruptions. Customer Map polls queued/running relay jobs automatically and can run another foreground turn when Hermes explicitly returns `continue: true`. A timed-out task is terminal and does not continue in the background.

Version 0.5.1 separates conversational reasoning from email side effects. Ordinary Customer Map turns run on a fail-closed Hermes platform allowlist (`web`, with MCP and all local execution toolsets removed). They cannot access terminal, files, code execution, delegation, cron, or mail commands. Customer Map `sendEmail` and `saveDraft` buttons bypass the model and enter a deterministic Mail Backend adapter.

The default backend is `auto`: the connector detects supported local mail adapters and uses the only available one. It never asks the model or Customer Map to select a tool. If both gog and Himalaya are installed, choose one explicitly and restart Hermes:

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
