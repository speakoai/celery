import os
import hashlib


def build_knowledge_prompt(knowledge_type: str) -> str:
    """Return a strict JSON-only extraction prompt per knowledge type.

    This mirrors the prompt logic used in app.py but lives here for reuse by Celery tasks.
    """
    if knowledge_type == 'menu':
        return (
            "You are given a document that may contain a service or food menu. "
            "Extract menu items into the following strict JSON schema. Output ONLY JSON, no prose.\n"
            "{\n"
            "  \"type\": \"menu\",\n"
            "  \"items\": [\n"
            "    {\n"
            "      \"name\": string,\n"
            "      \"description\": string|null,\n"
            "      \"prices\": [ { \"label\": string|null, \"amount\": number, \"currency\": string|null } ],\n"
            "      \"options\": [ { \"name\": string, \"price_delta\": number|null } ],\n"
            "      \"allergens\": [ string ]\n"
            "    }\n"
            "  ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    if knowledge_type == 'faq':
        return (
            "You are given a document that may contain FAQ content. "
            "Extract FAQs into the following strict JSON schema. Output ONLY JSON.\n"
            "{\n"
            "  \"type\": \"faq\",\n"
            "  \"faqs\": [ { \"question\": string, \"answer\": string } ],\n"
            "  \"source_confidence\": number\n"
            "}"
        )
    # policy
    return (
        "You are given a document that may contain policies and terms & conditions. "
        "Extract into the following strict JSON schema. Output ONLY JSON.\n"
        "{\n"
        "  \"type\": \"policy\",\n"
        "  \"policies\": [ { \"title\": string, \"body\": string } ],\n"
        "  \"terms\": [ { \"title\": string, \"body\": string } ],\n"
        "  \"source_confidence\": number\n"
        "}"
    )


def parse_model_json_output(analysis_text: str):
    """Try to parse JSON text that may be wrapped in code fences.

    Returns (parsed_json_or_none, raw_text)
    """
    if not analysis_text:
        return None, analysis_text

    txt = analysis_text.strip()
    if txt.startswith('```'):
        # Remove starting/ending backticks and optional language hint
        txt = txt.strip('`')
        first_nl = txt.find('\n')
        if first_nl != -1:
            txt = txt[first_nl + 1:]

    try:
        import json as _json
        return _json.loads(txt), analysis_text
    except Exception:
        return None, analysis_text


def build_analysis_artifact_key(tenant_id: str, location_id: str, unique_filename: str) -> str:
    """Return the R2 key for storing the analysis JSON next to the uploaded file.

    Example:
    knowledges/{tenant}/{location}/analysis/{unique_filename}.json
    """
    base = os.path.splitext(unique_filename)[0]
    return f"knowledges/{tenant_id}/{location_id}/analysis/{base}.json"
