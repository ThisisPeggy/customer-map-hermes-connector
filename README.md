# Customer Map for Hermes

This Hermes platform plugin connects a user-owned Hermes Agent to Customer Map through an outbound WebSocket. No public Hermes port or API key is required. It reconnects automatically after temporary network or relay interruptions. Customer Map polls queued/running relay jobs automatically and can run another foreground turn when Hermes explicitly returns `continue: true`. A timed-out task is terminal and does not continue in the background.

Version 0.3.0 executes Customer Map `sendEmail` actions directly through `gog send` with fixed argument mapping. HTML always uses `--body-html`, plain-text fallback always uses `--body`, and the connector never uses shell redirection, heredocs, body files, or `/dev/stdin`. Ordinary Hermes chat and non-send tasks retain the existing streaming and relay behavior.

Tested with Hermes Agent v0.18.2. Users on older releases should update Hermes before installing the plugin.

Install the plugin with Hermes, use the one-time pairing command shown by Customer Map, then restart the Hermes gateway.

```bash
hermes plugins install https://github.com/ThisisPeggy/customer-map-hermes-connector --enable
```

During local development:

```bash
mkdir -p ~/.hermes/plugins/customer-map-platform
cp plugin.yaml __init__.py adapter.py connect.py ~/.hermes/plugins/customer-map-platform/
python3 ~/.hermes/plugins/customer-map-platform/connect.py --site https://your-customer-map.example --code CMAP-HERMES-...
hermes gateway restart
```

Verify the installation with:

```bash
hermes plugins list
python3 ~/.hermes/plugins/customer-map-platform/test_plugin.py
```
