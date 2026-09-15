"""A bounded read-only Customer Map tool; credentials never enter model arguments."""

import json
import os
import socket
import urllib.error
import urllib.request
from urllib.parse import urlsplit

try:
    from .secretary_context import current_data_context
except ImportError:
    from secretary_context import current_data_context


DATA_TOOLSET = "customer-map-data"
TOOL_NAME = "customer_map_query"
OPERATIONS = ("capabilities", "work_summary", "customers", "customer_detail", "quotes", "follow_ups", "mail_activity")
MAX_RESPONSE_BYTES = 512_000
QUERY_SCHEMA = {
    "name": TOOL_NAME,
    "description": "Read the bound user's Customer Map cloud records. Use work_summary for exact activity counts, customers to resolve company names to IDs, and the other operations for paginated details. Honor period, metric definitions, and source coverage; unavailable is not zero. Read-only: cannot send mail, create inquiries, or schedule tasks.",
    "parameters": {
        "type": "object", "additionalProperties": False, "required": ["operation"],
        "properties": {
            "operation": {"type": "string", "enum": list(OPERATIONS)},
            "period": {"type": "string", "enum": ["today", "yesterday", "tomorrow", "last7days", "last30days", "custom", "all"], "description": "Defaults to today for work_summary/mail_activity/follow_ups, all for lists. Custom requires from and through. Capabilities and customer_detail have no date filter. Future periods are only meaningful for follow-ups; summary cannot use all."},
            "from": {"type": "string", "description": "Inclusive YYYY-MM-DD for a custom period (at most one year)."},
            "through": {"type": "string", "description": "Inclusive YYYY-MM-DD for a custom period."},
            "timezone": {"type": "string", "description": "IANA timezone; omit to use the user's secretary settings."},
            "search": {"type": "string", "maxLength": 160, "description": "Company substring; customers or quotes only."},
            "country": {"type": "string", "maxLength": 100, "description": "Exact saved country value; customers only."},
            "customerId": {"type": "string", "maxLength": 160, "description": "Real ID from customers. Required for customer_detail; optional for work_summary, quotes, follow_ups, mail_activity."},
            "quoteId": {"type": "string", "maxLength": 160, "description": "Real quote ID; quotes (returns line items) or follow_ups only."},
            "relationshipStatus": {"type": "string", "enum": ["有回复", "有兴趣", "有询价", "成交过"], "description": "customers only: CURRENT relationship state, not reply/inquiry events during a period."},
            "mailKind": {"type": "string", "enum": ["sent", "reply", "bounce"], "description": "mail_activity only; defaults to sent."},
            "includeOverdue": {"type": "boolean", "description": "follow_ups only; defaults to true. False restricts to tasks due within the selected period."},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Page size, default 20. Totals are independent of this limit."},
            "offset": {"type": "integer", "minimum": 0, "maximum": 100000, "description": "Use the response nextOffset. Follow-ups paginate customerTasks and quoteTasks separately with this same offset."},
        },
    },
}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _endpoint():
    site = os.getenv("CUSTOMER_MAP_HERMES_SITE", "").strip().rstrip("/")
    try:
        url = urlsplit(site)
        host = (url.hostname or "").lower()
        secure = url.scheme == "https" or (url.scheme == "http" and host in {"localhost", "127.0.0.1", "::1"})
        if not secure or not host or url.username is not None or url.password is not None or url.path or url.query or url.fragment:
            raise ValueError
        _ = url.port
    except ValueError:
        raise ValueError("Customer Map site must be an HTTPS origin (HTTP is allowed only on loopback).") from None
    return site + "/api/agent-data"


def _failure(code, message):
    return json.dumps({"ok": False, "code": code, "error": message}, ensure_ascii=False)


def customer_map_query(args, **_kwargs):
    if not current_data_context():
        return _failure("data_access_denied", "Customer Map data is available only in its authenticated workspace or the Weixin owner chat authorized for this binding. Open Customer Map's Weixin setup and scan again after upgrading from 0.6.x.")
    if not isinstance(args, dict) or args.get("operation") not in OPERATIONS:
        return _failure("invalid_query", "Choose a supported Customer Map query operation.")
    if set(args) - set(QUERY_SCHEMA["parameters"]["properties"]):
        return _failure("invalid_query", "Unsupported query fields. Account identity and endpoint cannot be overridden.")
    token = os.getenv("CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "").strip()
    try:
        endpoint = _endpoint()
        body = json.dumps({"runtime": "hermes", "query": args}, ensure_ascii=False, allow_nan=False).encode()
        if len(body) > 16_000:
            return _failure("invalid_query", "Customer Map query is too large.")
        request = urllib.request.Request(endpoint, data=body, method="POST", headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json",
            # Cloudflare rejects urllib's generic Python user agent before the
            # Customer Map function can validate the delegated bridge token.
            "User-Agent": "Customer-Map-Hermes/0.7.1",
        })
        # In particular, never forward the bridge credential to a redirected host.
        opener = urllib.request.build_opener(_NoRedirect())
        with opener.open(request, timeout=25) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                return _failure("response_too_large", "Customer Map returned too much data. Narrow the query or reduce the page size.")
            result = json.loads(raw)
            if not isinstance(result, dict) or result.get("ok") is not True or result.get("version") != 1 or result.get("operation") != args["operation"]:
                return _failure("data_api_unavailable", "The Customer Map site has not enabled a compatible Agent data API. Deploy the matching website update.")
            return json.dumps(result, ensure_ascii=False)
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read(4_096)
        except (OSError, ValueError):
            raw = b""
        finally:
            exc.close()
        if 300 <= exc.code < 400:
            return _failure("redirect_blocked", "Customer Map redirected the data request. Configure the canonical site origin and pair again.")
        if exc.code in {404, 405}:
            return _failure("data_api_unavailable", "The Customer Map site has not enabled the Agent data API. Deploy the matching website update.")
        message = "Customer Map could not complete this query."
        code = f"http_{exc.code}"
        try:
            error = json.loads(raw)
            if isinstance(error, dict) and isinstance(error.get("error"), str):
                message = error["error"].replace(token, "[redacted]")[:500]
        except (ValueError, UnicodeError):
            pass
        return _failure(code, message)
    except (socket.timeout, TimeoutError):
        return _failure("data_request_timeout", "Customer Map data query timed out. No result is available.")
    except (urllib.error.URLError, OSError):
        return _failure("data_connection_failed", "Cannot reach the configured Customer Map site.")
    except (ValueError, TypeError, UnicodeError):
        return _failure("data_api_unavailable", "Invalid query, site origin, or incompatible Customer Map data response. Check the binding and deploy the matching website update.")


def register_data_tool(ctx, check_fn):
    ctx.register_tool(
        name=TOOL_NAME, toolset=DATA_TOOLSET, schema=QUERY_SCHEMA, handler=customer_map_query,
        check_fn=check_fn, requires_env=["CUSTOMER_MAP_HERMES_SITE", "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "CUSTOMER_MAP_HERMES_CONNECTION_ID"],
        description="Read-only Customer Map business data", emoji="🗺️",
    )
