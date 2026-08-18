"""Deterministic adapter for a user-owned open-source email verifier."""
import asyncio
import os
import re
import aiohttp

EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

def email_verifier_capability():
    return "declared" if os.getenv("CUSTOMER_MAP_HERMES_EMAIL_VERIFIER_URL", "").strip() else "unavailable"

async def verify_emails(emails):
    normalized = []
    for value in emails if isinstance(emails, list) else []:
        email = str(value or "").strip().lower()
        if email and email not in normalized:
            normalized.append(email)
    normalized = normalized[:100]
    if not normalized:
        raise ValueError("No email addresses were provided.")
    url = os.getenv("CUSTOMER_MAP_HERMES_EMAIL_VERIFIER_URL", "").strip()
    if not url:
        return [_result(email, "undeliverable", "Invalid email syntax", False) if not EMAIL_PATTERN.match(email) else _result(email, "unknown", "Configure CUSTOMER_MAP_HERMES_EMAIL_VERIFIER_URL to enable mailbox verification", True) for email in normalized]
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=5)) as session:
        return await asyncio.gather(*[_verify_one(session, url, email) for email in normalized])

async def _verify_one(session, url, email):
    if not EMAIL_PATTERN.match(email):
        return _result(email, "undeliverable", "Invalid email syntax", False)
    headers = {"Content-Type": "application/json"}
    secret = os.getenv("CUSTOMER_MAP_HERMES_EMAIL_VERIFIER_SECRET", "").strip()
    if secret:
        headers["Authorization"] = f"Bearer {secret}"
    try:
        async with session.post(url, json={"to_email": email, "email": email}, headers=headers) as response:
            if not 200 <= response.status < 300:
                return _result(email, "unknown", f"Verifier returned HTTP {response.status}", True)
            return _normalize(email, await response.json())
    except Exception as exc:
        return _result(email, "unknown", f"Verifier unavailable: {str(exc)[:160]}", True)

def _normalize(email, value):
    reachable = str(value.get("is_reachable") or value.get("status") or "").lower()
    deliverable = reachable in {"safe", "deliverable", "valid"} or value.get("deliverable") is True
    rejected = reachable in {"invalid", "undeliverable"} or value.get("deliverable") is False
    risky = reachable == "risky" or value.get("is_disposable") is True or value.get("is_role_account") is True
    status = "deliverable" if deliverable else "undeliverable" if rejected else "risky" if risky else "unknown"
    reason = str(value.get("reason") or value.get("message") or reachable or "Verifier returned no conclusive mailbox result")[:300]
    return _result(email, status, reason, True)

def _result(email, status, reason, syntax):
    mailbox = "accepted" if status == "deliverable" else "rejected" if status == "undeliverable" and syntax else "unknown"
    return {"email": email, "status": status, "reason": reason, "source": "hermes-connector", "checks": {"syntax": syntax, "mx": None, "mailbox": mailbox}}
