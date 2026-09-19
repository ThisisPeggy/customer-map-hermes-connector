"""Confirmed Customer Map business actions for the authorized Weixin owner."""

import json
import os
import socket
import urllib.error
import urllib.request

try:
    from .agent_data import _NoRedirect, _endpoint, _failure
    from .secretary_context import current_data_context, current_user_source, current_user_text
except ImportError:
    from agent_data import _NoRedirect, _endpoint, _failure
    from secretary_context import current_data_context, current_user_source, current_user_text


TOOL_NAME = "customer_map_action"
TOOLSET = "customer-map-actions"
KINDS = ("create", "confirm", "cancel")
OPERATION_TYPES = ("customer_create", "product_list_create", "product_item_create", "quote_create", "quote_to_pi", "quote_deliver_weixin", "campaign_create", "mail_reply")
SCHEMA = {
    "name": TOOL_NAME,
    "description": "Create a Customer Map operation preview, or confirm/cancel one only when the owner's current message explicitly contains its confirmation code. Never claim completion unless the tool succeeds.",
    "parameters": {
        "type": "object", "additionalProperties": False, "required": ["kind"],
        "properties": {
            "kind": {"type": "string", "enum": list(KINDS)},
            "operationType": {"type": "string", "enum": list(OPERATION_TYPES)},
            "actionId": {"type": "string", "description": "Stable UUID idempotency ID for create; reuse it only when retrying the identical request."},
            "operationId": {"type": "string", "description": "Operation UUID returned by create."},
            "payload": {"type": "object", "description": "Fields by operation: customer_create={company,country,website?,email?,contact?,phone?,shippingAddress?,notes?}; product_list_create={name,costCurrency?,saleCurrency?,exchangeRate?,marginPct?,priceDecimals?,note?,items:[{model,costPrice,leadTime?,extra?}]}; product_item_create={productListId,model,costPrice,leadTime?,extra?,sortOrder?}; quote_create={customerId,subject?,salutation?,introText?,validity?,paymentTerms?,deliveryTerms?,warranty?,paymentFee?,freightFee?,includeBankDetails?,bankBeneficiary?,bankName?,bankAddress?,bankAccount?,bankSwift?,bankChargesNote?,signerName?,signerEmail?,deliverToWeixin?,items:[{model,qty,value,valueCurrency?,brand?,leadTime?,condition?,extra?}]}; quote_to_pi={quoteId,deliverToWeixin?}; quote_deliver_weixin={quoteId}; campaign_create={title,goal,customerIds,productListId?,productListName?}; mail_reply={recipient,subject,body}. IDs must come from customer_map_query results."},
        },
    },
}


def customer_map_action(args, **_kwargs):
    context = current_data_context()
    if not context or context[0] != "weixin":
        return _failure("action_access_denied", "Customer Map actions are available only in the authorized Weixin owner chat.")
    if not isinstance(args, dict) or args.get("kind") not in KINDS or set(args) - set(SCHEMA["parameters"]["properties"]):
        return _failure("invalid_action", "Choose a supported Customer Map action.")
    kind = args["kind"]
    if kind == "create" and args.get("operationType") not in OPERATION_TYPES:
        return _failure("invalid_action", "Choose a supported operation type.")
    if kind != "create" and not args.get("operationId"):
        return _failure("invalid_action", "The operation ID is required.")
    action = dict(args)
    action["originalInstruction"] = current_user_text()
    action["source"] = current_user_source()
    if kind in {"confirm", "cancel"}:
        action["confirmationText"] = current_user_text()
    token = os.getenv("CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "").strip()
    try:
        body = json.dumps({"runtime": "hermes", "action": action}, ensure_ascii=False, allow_nan=False).encode()
        if len(body) > 210_000:
            return _failure("action_too_large", "The Customer Map action is too large.")
        request = urllib.request.Request(_endpoint(), data=body, method="POST", headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json",
            "User-Agent": "Customer-Map-Hermes/0.9.0",
        })
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=45) as response:
            raw = response.read(512_001)
            if len(raw) > 512_000:
                return _failure("action_response_too_large", "Customer Map returned an oversized action response.")
            result = json.loads(raw)
            if not isinstance(result, dict) or result.get("ok") is not True or not isinstance(result.get("operation"), dict):
                return _failure("action_api_unavailable", "Customer Map returned an incompatible action response.")
            return json.dumps(result, ensure_ascii=False)
    except urllib.error.HTTPError as exc:
        raw = exc.read(4096)
        exc.close()
        try:
            result = json.loads(raw)
            return _failure(str(result.get("code") or f"http_{exc.code}"), str(result.get("error") or "Customer Map could not complete this action.")[:500])
        except (ValueError, UnicodeError):
            return _failure(f"http_{exc.code}", "Customer Map could not complete this action.")
    except (socket.timeout, TimeoutError):
        return _failure("action_timeout", "Customer Map action timed out; check the operation history before retrying.")
    except (urllib.error.URLError, OSError):
        return _failure("action_connection_failed", "Cannot reach the configured Customer Map site.")


def register_action_tool(ctx, check_fn):
    ctx.register_tool(
        name=TOOL_NAME, toolset=TOOLSET, schema=SCHEMA, handler=customer_map_action,
        check_fn=check_fn, requires_env=["CUSTOMER_MAP_HERMES_SITE", "CUSTOMER_MAP_HERMES_BRIDGE_TOKEN", "CUSTOMER_MAP_HERMES_CONNECTION_ID"],
        description="Confirmed Customer Map business operations", emoji="✅",
    )
