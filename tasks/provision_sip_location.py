"""
Provision SIP / Jambonz objects for a location.

Two independent modes (a location may have both enabled):

  softphone — zero-input. Generates a Jambonz SIP Client (username/password)
              that QA can register to from Zoiper/Linphone, plus the
              Application that routes the call to our voice-ai webhook.

  pbx       — connect to a customer's existing PBX (e.g. 3CX). Takes 5
              fields from the telephony provider, creates a Jambonz
              VoipCarrier with register=true (Jambonz logs into the
              customer's PBX as that extension), a SipGateway pointing at
              the PBX hostname, and a PhoneNumber matching the extension
              digits that routes inbound calls into our Application.

Both modes share the same Application (one per location).

Idempotency: each step looks for an existing object by saved SID first,
then by name (Application/Carrier) or username (Client) or number (PhoneNumber),
adopting any match. A second invocation of `provision` either no-ops or
refreshes URLs/credentials in place.

Naming convention (matches plan):
    <slugified-location-name>-<location_id>-<env_label>
    e.g. wonder-sushi-bankstown-35-prod
"""

import logging
import os
import re

from celery import shared_task
from celery.utils.log import get_task_logger

from tasks.utils import jambonz_client as jb
from tasks.utils.jambonz_client import JambonzAPIError
from tasks.utils.sip_secrets import encrypt_sql, get_passphrase
from tasks.utils.task_db import _get_conn, mark_task_failed, mark_task_running, mark_task_succeeded

logger = get_task_logger(__name__)


# =============================================================================
# Helpers
# =============================================================================

def _env_label() -> str:
    """Return 'dev' or 'prod'. Mirrors detection used in call_lifecycle.py."""
    if os.getenv("ENVIRONMENT", "").lower() in ("dev", "development"):
        return "dev"
    if os.getenv("RENDER_SERVICE_NAME", "").endswith("-dev"):
        return "dev"
    if os.getenv("SIP_ENV_LABEL", "").lower() == "dev":
        return "dev"
    return "prod"


def _voice_ai_webhook_base() -> str:
    """Base URL for app_jambonz_azure.py — different per env."""
    base = os.getenv("VOICE_AI_WEBHOOK_BASE_URL")
    if not base:
        raise RuntimeError(
            "VOICE_AI_WEBHOOK_BASE_URL not set — required to register Jambonz call-hook URL"
        )
    return base.rstrip("/")


def _sip_realm() -> str:
    realm = os.getenv("JAMBONZ_SIP_REALM")
    if not realm:
        raise RuntimeError("JAMBONZ_SIP_REALM not set — required for SIP Client realm")
    return realm


def _webhook_auth() -> tuple[str | None, str | None]:
    return os.getenv("JAMBONZ_WEBHOOK_USER"), os.getenv("JAMBONZ_WEBHOOK_PASS")


def _slugify(text: str) -> str:
    """lowercase, alnum-or-dash, collapsed, trimmed to 30 chars."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", text or "").strip("-").lower()
    s = re.sub(r"-+", "-", s)
    return s[:30].rstrip("-") or "location"


def _object_name(location_name: str, location_id: int) -> str:
    return f"{_slugify(location_name)}-{location_id}-{_env_label()}"


def _softphone_dial_prefix() -> str:
    """Prefix on softphone dial codes so dev/prod don't collide on the
    single shared Jambonz server. Dev locations dial 88<id>, prod 99<id>.
    """
    return "88" if _env_label() == "dev" else "99"


def _softphone_dial_code(location_id: int) -> str:
    """Numeric code testers dial from the shared Jambonz softphone account
    to reach this location's voice agent.
    """
    return f"{_softphone_dial_prefix()}{location_id}"


def _find_test_loopback_carrier_sid() -> str:
    """Locate Jambonz's built-in 'Test Loopback' carrier. Softphone PhoneNumbers
    are wired through it (it's the carrier that lets registered SIP clients
    dial inbound to our applications without going via the PSTN)."""
    for c in jb.list_voip_carriers():
        if c.get("name") == "Test Loopback":
            return c.get("voip_carrier_sid") or c.get("sid")
    raise RuntimeError(
        "Jambonz 'Test Loopback' carrier not found — it should ship with every "
        "Jambonz install. Check the portal under Carriers."
    )


def _fetch_location(location_id: int) -> dict:
    """Return {location_id, tenant_id, name} from the locations table."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT location_id, tenant_id, name FROM locations WHERE location_id = %s",
                (location_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"Location {location_id} not found")
            return {"location_id": row[0], "tenant_id": row[1], "name": row[2]}
    finally:
        conn.close()


def _fetch_sip_config(location_id: int) -> dict | None:
    """Return current location_sip_config row or None if not present."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    location_id, tenant_id, enabled,
                    sip_extension, sip_pbx_hostname, sip_pbx_port, sip_transport,
                    jambonz_application_sid, jambonz_carrier_sid,
                    jambonz_phone_number_sid,
                    jambonz_client_username, jambonz_sip_realm,
                    sip_register_username
                FROM location_sip_config WHERE location_id = %s
                """,
                (location_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [
                "location_id", "tenant_id", "enabled",
                "sip_extension", "sip_pbx_hostname", "sip_pbx_port", "sip_transport",
                "jambonz_application_sid", "jambonz_carrier_sid",
                "jambonz_phone_number_sid",
                "jambonz_client_username", "jambonz_sip_realm",
                "sip_register_username",
            ]
            return dict(zip(cols, row))
    finally:
        conn.close()


def _upsert_sip_config(location_id: int, tenant_id: int, fields: dict) -> None:
    """INSERT or UPDATE location_sip_config with the given fields.

    `fields` may include any column. Encrypted password columns are written
    via pgcrypto using SIP_SECRETS_PASSPHRASE — pass plaintext under the keys
    `jambonz_client_password` or `sip_register_password` and we'll encrypt
    on the way in.
    """
    plaintext_client_pw = fields.pop("jambonz_client_password", None)
    plaintext_register_pw = fields.pop("sip_register_password", None)
    passphrase = get_passphrase() if (plaintext_client_pw or plaintext_register_pw) else None

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Ensure a row exists for the location.
                cur.execute(
                    """
                    INSERT INTO location_sip_config (location_id, tenant_id, enabled)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (location_id) DO NOTHING
                    """,
                    (location_id, tenant_id),
                )

                # Build a SET clause from `fields`.
                set_clauses = []
                params: list = []
                for col, val in fields.items():
                    set_clauses.append(f"{col} = %s")
                    params.append(val)

                if plaintext_client_pw is not None:
                    set_clauses.append(
                        f"jambonz_client_password_encrypted = {encrypt_sql()}"
                    )
                    params.extend([plaintext_client_pw, passphrase])

                if plaintext_register_pw is not None:
                    set_clauses.append(
                        f"sip_register_password_encrypted = {encrypt_sql()}"
                    )
                    params.extend([plaintext_register_pw, passphrase])

                set_clauses.append("updated_at = now()")
                params.append(location_id)

                cur.execute(
                    f"UPDATE location_sip_config SET {', '.join(set_clauses)} "
                    f"WHERE location_id = %s",
                    params,
                )
    finally:
        conn.close()


# =============================================================================
# Mode helpers
# =============================================================================

def _ensure_application(*, name: str, location_id: int, existing_sid: str | None) -> dict:
    """Find or create the Jambonz Application for this location.

    Returns the application dict (must contain 'application_sid' or 'sid').
    Always refreshes the call-hook URLs to canonical values, so manual edits
    in the Jambonz dashboard cannot drift.
    """
    call_hook = f"{_voice_ai_webhook_base()}/jambonz/call-hook/{location_id}"
    status_hook = f"{_voice_ai_webhook_base()}/jambonz/call-status/{location_id}"
    user, password = _webhook_auth()

    # 1. Try saved SID
    if existing_sid:
        app = jb.get_application(existing_sid)
        if app:
            jb.update_application(
                existing_sid,
                name=name,
                call_hook_url=call_hook,
                call_status_hook_url=status_hook,
                webhook_user=user,
                webhook_pass=password,
            )
            return {"application_sid": existing_sid, "name": name, "adopted": "saved_sid"}

    # 2. Try lookup by name
    existing = jb.find_application_by_name(name)
    if existing:
        sid = existing.get("application_sid") or existing.get("sid")
        jb.update_application(
            sid,
            name=name,
            call_hook_url=call_hook,
            call_status_hook_url=status_hook,
            webhook_user=user,
            webhook_pass=password,
        )
        return {"application_sid": sid, "name": name, "adopted": "by_name"}

    # 3. Create fresh
    created = jb.create_application(
        name=name,
        call_hook_url=call_hook,
        call_status_hook_url=status_hook,
        webhook_user=user,
        webhook_pass=password,
    )
    sid = created.get("application_sid") or created.get("sid")
    return {"application_sid": sid, "name": name, "adopted": "created"}


def _ensure_softphone_phone_number(
    *,
    dial_code: str,
    application_sid: str,
    voip_carrier_sid: str,
) -> str:
    """Find-or-create a Jambonz PhoneNumber for softphone testing.

    Looks up by the dial code (e.g. '8835'); if it exists, refreshes its
    application/carrier wiring; otherwise creates fresh. Returns the
    phone_number_sid.
    """
    existing = jb.find_phone_number_by_number(dial_code)
    if existing:
        pn_sid = existing.get("phone_number_sid") or existing.get("sid")
        jb.update_phone_number(
            pn_sid,
            application_sid=application_sid,
            voip_carrier_sid=voip_carrier_sid,
        )
        return pn_sid

    created = jb.create_phone_number(
        number=dial_code,
        application_sid=application_sid,
        voip_carrier_sid=voip_carrier_sid,
    )
    return created.get("phone_number_sid") or created.get("sid")


def _ensure_pbx_carrier(
    *,
    name: str,
    pbx_hostname: str,
    pbx_port: int,
    register_username: str,
    register_password: str,
    existing_carrier_sid: str | None,
) -> dict:
    """Create or refresh a VoipCarrier + SipGateway for a customer's PBX."""
    realm = pbx_hostname  # Jambonz uses the PBX hostname as the register realm.

    # 1. Carrier — try saved SID, then by name, else create.
    carrier_sid: str | None = None
    if existing_carrier_sid:
        carrier = jb.get_voip_carrier(existing_carrier_sid)
        if carrier:
            carrier_sid = existing_carrier_sid
            jb.update_voip_carrier(
                carrier_sid,
                register_username=register_username,
                register_password=register_password,
                register_sip_realm=realm,
            )

    if not carrier_sid:
        existing = jb.find_voip_carrier_by_name(name)
        if existing:
            carrier_sid = existing.get("voip_carrier_sid") or existing.get("sid")
            jb.update_voip_carrier(
                carrier_sid,
                register_username=register_username,
                register_password=register_password,
                register_sip_realm=realm,
            )
        else:
            created = jb.create_voip_carrier(
                name=name,
                register_username=register_username,
                register_password=register_password,
                register_sip_realm=realm,
            )
            carrier_sid = created.get("voip_carrier_sid") or created.get("sid")

    # 2. SipGateway — make sure exactly one exists for (pbx_hostname, pbx_port).
    gateways = jb.list_sip_gateways(carrier_sid)
    match = next(
        (g for g in gateways if g.get("ipv4") == pbx_hostname and int(g.get("port") or 0) == int(pbx_port)),
        None,
    )
    if not match:
        jb.create_sip_gateway(
            voip_carrier_sid=carrier_sid,
            ipv4=pbx_hostname,
            port=pbx_port,
            inbound=True,
            outbound=True,
        )

    return {"voip_carrier_sid": carrier_sid}


def _ensure_pbx_phone_number(
    *,
    number: str,
    application_sid: str,
    voip_carrier_sid: str,
    existing_phone_number_sid: str | None,
) -> str:
    """Ensure a PhoneNumber for `number` is wired to (Application, Carrier)."""
    # By saved SID
    if existing_phone_number_sid:
        try:
            jb.update_phone_number(
                existing_phone_number_sid,
                application_sid=application_sid,
                voip_carrier_sid=voip_carrier_sid,
            )
            return existing_phone_number_sid
        except JambonzAPIError as e:
            if e.status != 404:
                raise

    # By number
    existing = jb.find_phone_number_by_number(number)
    if existing:
        pn_sid = existing.get("phone_number_sid") or existing.get("sid")
        jb.update_phone_number(
            pn_sid,
            application_sid=application_sid,
            voip_carrier_sid=voip_carrier_sid,
        )
        return pn_sid

    created = jb.create_phone_number(
        number=number,
        application_sid=application_sid,
        voip_carrier_sid=voip_carrier_sid,
    )
    return created.get("phone_number_sid") or created.get("sid")


# =============================================================================
# Public task
# =============================================================================

@shared_task(name="tasks.provision_sip_location.provision_sip_location", bind=True)
def provision_sip_location(
    self,
    *,
    location_id: int,
    mode: str,                    # "softphone" | "pbx"
    action: str = "provision",    # "provision" | "rotate" | "disable"
    pbx_params: dict | None = None,
    speako_task_id: str | None = None,
) -> dict:
    """
    Idempotent SIP/Jambonz provisioning for a single location.

    pbx_params (required when mode='pbx' and action='provision') keys:
        sip_extension, sip_register_username, sip_register_password,
        sip_pbx_hostname, sip_pbx_port, sip_transport
    """
    celery_task_id = self.request.id
    if speako_task_id:
        mark_task_running(task_id=speako_task_id, celery_task_id=celery_task_id)

    try:
        if mode not in ("softphone", "pbx"):
            raise ValueError(f"unsupported mode: {mode}")
        if action not in ("provision", "rotate", "disable"):
            raise ValueError(f"unsupported action: {action}")

        loc = _fetch_location(location_id)
        existing = _fetch_sip_config(location_id) or {}
        name = _object_name(loc["name"], location_id)

        result: dict = {"location_id": location_id, "mode": mode, "action": action, "name": name}

        if mode == "softphone":
            result.update(_run_softphone(loc, existing, name, action))
        else:
            result.update(_run_pbx(loc, existing, name, action, pbx_params))

        if speako_task_id:
            mark_task_succeeded(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                output={"sip_provision": result},
            )
        return result

    except Exception as e:
        logger.exception("[provision_sip_location] failed: %s", e)
        if speako_task_id:
            mark_task_failed(
                task_id=speako_task_id,
                celery_task_id=celery_task_id,
                error_code="sip_provision_failed",
                error_message=str(e)[:500],
            )
        raise


def _run_softphone(loc: dict, existing: dict, name: str, action: str) -> dict:
    """
    Softphone mode (revised):

    Testers register Linphone/Zoiper against ONE shared Jambonz SIP user
    (managed manually in the Jambonz portal — not by us). Each location gets
    its own *PhoneNumber* with an env-prefixed dial code; the tester dials
    that number from the shared softphone account to reach the location's
    voice agent. Dial codes are `88<location_id>` in dev and `99<location_id>`
    in prod so the two environments can coexist on a single Jambonz server
    without colliding on overlapping location_ids.

    Per-location Client / per-location password are no longer created.
    `jambonz_client_username` is repurposed to store the dial code (e.g.
    "8835") so the UI can detect provisioned state without an extra column.
    """
    location_id = loc["location_id"]
    tenant_id = loc["tenant_id"]
    dial_code = _softphone_dial_code(location_id)

    if action == "disable":
        existing_pn = jb.find_phone_number_by_number(dial_code)
        if existing_pn:
            jb.delete_phone_number(
                existing_pn.get("phone_number_sid") or existing_pn.get("sid")
            )
        _upsert_sip_config(location_id, tenant_id, {
            "jambonz_client_username": None,
            "jambonz_sip_realm": None,
        })
        return {"softphone": "disabled", "dial_code_removed": dial_code}

    if action == "rotate":
        # Nothing per-location to rotate — the shared SIP user lives in the
        # Jambonz portal, rotated there manually if/when needed.
        return {
            "softphone": "noop",
            "note": "Softphone uses shared Jambonz credentials; nothing per-location to rotate.",
        }

    # action == "provision"
    app_result = _ensure_application(
        name=name,
        location_id=location_id,
        existing_sid=existing.get("jambonz_application_sid"),
    )

    carrier_sid = _find_test_loopback_carrier_sid()

    phone_number_sid = _ensure_softphone_phone_number(
        dial_code=dial_code,
        application_sid=app_result["application_sid"],
        voip_carrier_sid=carrier_sid,
    )

    _upsert_sip_config(location_id, tenant_id, {
        "enabled": True,
        "jambonz_application_sid": app_result["application_sid"],
        "jambonz_client_username": dial_code,  # repurposed: the softphone dial code
        "jambonz_sip_realm": _sip_realm(),
    })

    return {
        "softphone": "ok",
        "application": app_result,
        "dial_code": dial_code,
        "phone_number_sid": phone_number_sid,
        "voip_carrier_sid": carrier_sid,
    }


def _run_pbx(loc: dict, existing: dict, name: str, action: str, pbx_params: dict | None) -> dict:
    location_id = loc["location_id"]
    tenant_id = loc["tenant_id"]

    if action == "disable":
        carrier_sid = existing.get("jambonz_carrier_sid")
        phone_number_sid = existing.get("jambonz_phone_number_sid")
        if phone_number_sid:
            try:
                jb.delete_phone_number(phone_number_sid)
            except JambonzAPIError as e:
                if e.status != 404:
                    raise
        if carrier_sid:
            try:
                jb.delete_voip_carrier(carrier_sid)
            except JambonzAPIError as e:
                if e.status != 404:
                    raise
        _upsert_sip_config(location_id, tenant_id, {
            "jambonz_carrier_sid": None,
            "jambonz_phone_number_sid": None,
            "sip_register_username": None,
            "sip_register_password_encrypted": None,
            "sip_extension": None,
            "sip_pbx_hostname": None,
        })
        return {"pbx": "disabled"}

    if not pbx_params:
        raise ValueError("pbx_params required for mode=pbx action=provision/rotate")

    required = ("sip_extension", "sip_register_username", "sip_register_password",
                "sip_pbx_hostname", "sip_pbx_port")
    missing = [k for k in required if not pbx_params.get(k)]
    if missing:
        raise ValueError(f"pbx_params missing fields: {missing}")

    extension = str(pbx_params["sip_extension"]).strip()
    register_username = str(pbx_params["sip_register_username"]).strip()
    register_password = str(pbx_params["sip_register_password"])
    pbx_hostname = str(pbx_params["sip_pbx_hostname"]).strip()
    pbx_port = int(pbx_params["sip_pbx_port"])
    transport = str(pbx_params.get("sip_transport", "UDP")).upper()

    app_result = _ensure_application(
        name=name,
        location_id=location_id,
        existing_sid=existing.get("jambonz_application_sid"),
    )

    carrier_result = _ensure_pbx_carrier(
        name=name,
        pbx_hostname=pbx_hostname,
        pbx_port=pbx_port,
        register_username=register_username,
        register_password=register_password,
        existing_carrier_sid=existing.get("jambonz_carrier_sid"),
    )

    phone_number_sid = _ensure_pbx_phone_number(
        number=extension,
        application_sid=app_result["application_sid"],
        voip_carrier_sid=carrier_result["voip_carrier_sid"],
        existing_phone_number_sid=existing.get("jambonz_phone_number_sid"),
    )

    _upsert_sip_config(location_id, tenant_id, {
        "enabled": True,
        "jambonz_application_sid": app_result["application_sid"],
        "jambonz_carrier_sid": carrier_result["voip_carrier_sid"],
        "jambonz_phone_number_sid": phone_number_sid,
        "sip_extension": extension,
        "sip_pbx_hostname": pbx_hostname,
        "sip_pbx_port": pbx_port,
        "sip_transport": transport,
        "sip_register_username": register_username,
        "sip_register_password": register_password,
    })

    return {
        "pbx": "ok",
        "application": app_result,
        "voip_carrier_sid": carrier_result["voip_carrier_sid"],
        "phone_number_sid": phone_number_sid,
        "extension": extension,
        "pbx_hostname": pbx_hostname,
    }
