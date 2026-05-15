"""
Thin wrapper around the Jambonz REST API.

Single self-hosted Jambonz instance shared by dev + prod (env distinguishes by
Application name suffix `-dev` / `-prod`, NOT by separate accounts).

Required env vars:
    JAMBONZ_API_BASE_URL   e.g. https://jambonz.speako.ai
    JAMBONZ_ACCOUNT_SID    the single account UUID under which all our objects live
    JAMBONZ_API_KEY        bearer token from Jambonz dashboard → Account → API Keys

All POST/PUT bodies follow https://api.jambonz.org/. Responses are JSON.
Methods return parsed dicts or raise JambonzAPIError on non-2xx.

Idempotency helpers (find_application_by_name, find_client_by_username, etc.)
let the provisioning task adopt existing objects after a partial failure.
"""

import logging
import os
from typing import Any

import requests


class JambonzAPIError(RuntimeError):
    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


def _base_url() -> str:
    url = os.getenv("JAMBONZ_API_BASE_URL")
    if not url:
        raise JambonzAPIError("JAMBONZ_API_BASE_URL not set")
    return url.rstrip("/")


def _account_sid() -> str:
    sid = os.getenv("JAMBONZ_ACCOUNT_SID")
    if not sid:
        raise JambonzAPIError("JAMBONZ_ACCOUNT_SID not set")
    return sid


def _api_key() -> str:
    key = os.getenv("JAMBONZ_API_KEY")
    if not key:
        raise JambonzAPIError("JAMBONZ_API_KEY not set")
    return key


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _request(method: str, path: str, *, json_body: dict | None = None, timeout: int = 20) -> Any:
    url = f"{_base_url()}{path}"
    resp = requests.request(method, url, headers=_headers(), json=json_body, timeout=timeout)
    if not resp.ok:
        body = resp.text[:1000]
        logging.warning("[Jambonz] %s %s -> %s %s", method, path, resp.status_code, body)
        raise JambonzAPIError(
            f"Jambonz {method} {path} failed: {resp.status_code}",
            status=resp.status_code,
            body=body,
        )
    if resp.status_code == 204 or not resp.text:
        return None
    try:
        return resp.json()
    except ValueError:
        return resp.text


# =============================================================================
# Applications
# =============================================================================

def list_applications() -> list[dict]:
    """Return all applications under our account."""
    data = _request("GET", f"/v1/Accounts/{_account_sid()}/Applications")
    return data if isinstance(data, list) else []


def find_application_by_name(name: str) -> dict | None:
    for app in list_applications():
        if app.get("name") == name:
            return app
    return None


def create_application(
    *,
    name: str,
    call_hook_url: str,
    call_status_hook_url: str,
    webhook_user: str | None = None,
    webhook_pass: str | None = None,
) -> dict:
    """
    Speech vendor fields are filled with harmless Jambonz defaults even
    though Jambonz never invokes them — we run Azure Voice Live on our
    WebSocket side, so all TTS/STT happens out-of-band. We set defaults
    rather than null because some Jambonz installs reject null vendor on
    CREATE (e.g. ours: `Column 'speech_synthesis_vendor' cannot be null`).
    """
    body: dict = {
        "account_sid": _account_sid(),
        "name": name,
        "call_hook": _hook(call_hook_url, webhook_user, webhook_pass),
        "call_status_hook": _hook(call_status_hook_url, webhook_user, webhook_pass),
        "speech_synthesis_vendor": "google",
        "speech_synthesis_language": "en-US",
        "speech_synthesis_voice": "en-US-Wavenet-C",
        "speech_recognizer_vendor": "google",
        "speech_recognizer_language": "en-US",
    }
    return _request("POST", "/v1/Applications", json_body=body)


def update_application(
    application_sid: str,
    *,
    name: str,
    call_hook_url: str,
    call_status_hook_url: str,
    webhook_user: str | None = None,
    webhook_pass: str | None = None,
) -> None:
    body = {
        "name": name,
        "call_hook": _hook(call_hook_url, webhook_user, webhook_pass),
        "call_status_hook": _hook(call_status_hook_url, webhook_user, webhook_pass),
    }
    _request("PUT", f"/v1/Applications/{application_sid}", json_body=body)


def get_application(application_sid: str) -> dict | None:
    try:
        return _request("GET", f"/v1/Applications/{application_sid}")
    except JambonzAPIError as e:
        if e.status == 404:
            return None
        raise


def delete_application(application_sid: str) -> None:
    _request("DELETE", f"/v1/Applications/{application_sid}")


def _hook(url: str, user: str | None, password: str | None) -> dict:
    h = {"url": url, "method": "POST"}
    if user:
        h["username"] = user
    if password:
        h["password"] = password
    return h


# =============================================================================
# SIP Clients (registered softphone users)
# =============================================================================

def list_clients() -> list[dict]:
    data = _request("GET", f"/v1/Clients?account_sid={_account_sid()}")
    return data if isinstance(data, list) else []


def find_client_by_username(username: str) -> dict | None:
    for c in list_clients():
        if c.get("username") == username:
            return c
    return None


def create_client(*, username: str, password: str, allow_direct_app_calling: bool = True) -> dict:
    body = {
        "account_sid": _account_sid(),
        "username": username,
        "password": password,
        "is_active": True,
        "allow_direct_app_calling": allow_direct_app_calling,
        "allow_direct_queue_calling": False,
        "allow_direct_user_calling": False,
    }
    return _request("POST", "/v1/Clients", json_body=body)


def update_client_password(client_sid: str, *, password: str) -> None:
    _request("PUT", f"/v1/Clients/{client_sid}", json_body={"password": password})


def delete_client(client_sid: str) -> None:
    _request("DELETE", f"/v1/Clients/{client_sid}")


# =============================================================================
# VoIP Carriers (register=true → outbound register to customer PBX)
# =============================================================================

def list_voip_carriers() -> list[dict]:
    data = _request("GET", f"/v1/VoipCarriers?account_sid={_account_sid()}")
    return data if isinstance(data, list) else []


def find_voip_carrier_by_name(name: str) -> dict | None:
    for c in list_voip_carriers():
        if c.get("name") == name:
            return c
    return None


def create_voip_carrier(
    *,
    name: str,
    register_username: str,
    register_password: str,
    register_sip_realm: str,
) -> dict:
    """
    Carrier in 'register mode': Jambonz acts as a SIP UAC and REGISTERs to
    the customer's PBX. Inbound calls hitting that SIP user are routed via
    a PhoneNumber → Application.
    """
    body = {
        "account_sid": _account_sid(),
        "name": name,
        "is_active": True,
        "requires_register": True,
        "register_username": register_username,
        "register_password": register_password,
        "register_sip_realm": register_sip_realm,
        "register_from_user": register_username,
        "register_from_domain": register_sip_realm,
        "register_status": {"status": "unknown"},
    }
    return _request("POST", "/v1/VoipCarriers", json_body=body)


def update_voip_carrier(
    voip_carrier_sid: str,
    *,
    register_username: str,
    register_password: str,
    register_sip_realm: str,
) -> None:
    body = {
        "register_username": register_username,
        "register_password": register_password,
        "register_sip_realm": register_sip_realm,
        "register_from_user": register_username,
        "register_from_domain": register_sip_realm,
        "requires_register": True,
        "is_active": True,
    }
    _request("PUT", f"/v1/VoipCarriers/{voip_carrier_sid}", json_body=body)


def get_voip_carrier(voip_carrier_sid: str) -> dict | None:
    try:
        return _request("GET", f"/v1/VoipCarriers/{voip_carrier_sid}")
    except JambonzAPIError as e:
        if e.status == 404:
            return None
        raise


def delete_voip_carrier(voip_carrier_sid: str) -> None:
    _request("DELETE", f"/v1/VoipCarriers/{voip_carrier_sid}")


# =============================================================================
# SIP Gateways (carrier's destination endpoints)
# =============================================================================

def list_sip_gateways(voip_carrier_sid: str) -> list[dict]:
    data = _request("GET", f"/v1/SipGateways?voip_carrier_sid={voip_carrier_sid}")
    return data if isinstance(data, list) else []


def create_sip_gateway(
    *,
    voip_carrier_sid: str,
    ipv4: str,
    port: int = 5060,
    inbound: bool = True,
    outbound: bool = True,
) -> dict:
    body = {
        "voip_carrier_sid": voip_carrier_sid,
        "ipv4": ipv4,
        "port": port,
        "netmask": 32,
        "inbound": inbound,
        "outbound": outbound,
        "is_active": True,
    }
    return _request("POST", "/v1/SipGateways", json_body=body)


def delete_sip_gateway(sip_gateway_sid: str) -> None:
    _request("DELETE", f"/v1/SipGateways/{sip_gateway_sid}")


# =============================================================================
# Phone Numbers
# =============================================================================

def list_phone_numbers() -> list[dict]:
    data = _request("GET", f"/v1/PhoneNumbers?account_sid={_account_sid()}")
    return data if isinstance(data, list) else []


def find_phone_number_by_number(number: str) -> dict | None:
    for pn in list_phone_numbers():
        if pn.get("number") == number:
            return pn
    return None


def create_phone_number(
    *,
    number: str,
    application_sid: str,
    voip_carrier_sid: str | None = None,
) -> dict:
    body = {
        "account_sid": _account_sid(),
        "number": number,
        "application_sid": application_sid,
    }
    if voip_carrier_sid:
        body["voip_carrier_sid"] = voip_carrier_sid
    return _request("POST", "/v1/PhoneNumbers", json_body=body)


def update_phone_number(
    phone_number_sid: str,
    *,
    application_sid: str | None = None,
    voip_carrier_sid: str | None = None,  # accepted for API symmetry but never PUT
) -> None:
    """Update a Jambonz PhoneNumber. Note: ``voip_carrier_sid`` is immutable
    in Jambonz (PUT returns 400 ``voip_carrier_sid may not be modified``), so
    we silently ignore it on update. To change a PhoneNumber's carrier, the
    only path is delete + recreate.
    """
    _ = voip_carrier_sid  # explicitly unused
    body: dict = {}
    if application_sid is not None:
        body["application_sid"] = application_sid
    if not body:
        return
    _request("PUT", f"/v1/PhoneNumbers/{phone_number_sid}", json_body=body)


def delete_phone_number(phone_number_sid: str) -> None:
    _request("DELETE", f"/v1/PhoneNumbers/{phone_number_sid}")
