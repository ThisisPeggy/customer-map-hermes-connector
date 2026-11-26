#!/usr/bin/env bash
set -euo pipefail

base="https://raw.githubusercontent.com/ThisisPeggy/customer-map-hermes-connector/main"
target="${HERMES_HOME:-$HOME/.hermes}/plugins/customer-map-platform"
mkdir -p "$target"
chmod 700 "$target"
for file in plugin.yaml __init__.py adapter.py connect.py mail_backends.py tool_boundary.py agent_data.py dispatch_context.py test_plugin.py test_data_context.py; do
  curl -fsSL "$base/$file" -o "$target/$file"
done
chmod 600 "$target/plugin.yaml" "$target/__init__.py" "$target/adapter.py" "$target/mail_backends.py" "$target/tool_boundary.py" "$target/agent_data.py" "$target/dispatch_context.py"
chmod 700 "$target/connect.py" "$target/test_plugin.py" "$target/test_data_context.py"
hermes plugins enable customer-map-platform --no-allow-tool-override
echo "Customer Map plugin installed at $target"
