import os
import io
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


def _bytes_to_text(data: bytes) -> str:
    """Decode bytes to text using utf-8 with fallback to latin-1."""
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        return data.decode('latin-1', errors='replace')


def preprocess_for_model(file_bytes: bytes, filename: str, content_type: str) -> dict:
    """Return a dict describing how to feed this content to the model.

    Modes:
    - { 'mode': 'file', 'filename': str, 'file_bytes': bytes }
      Use OpenAI Files+Responses with input_file (currently best for PDF).

    - { 'mode': 'text', 'text': str, 'note': str }
      Provide extracted/converted text as input_text.

    Supported:
    - PDF (.pdf) => mode=file
    - CSV (.csv) => mode=text (raw text)
    - JSON (.json) => mode=text (raw text)
    - Excel (.xlsx/.xls) => mode=text (CSV text of sheets)
    - Word (.docx) => mode=text (paragraph text)
    - Word (.doc) => unsupported (return {'mode':'unsupported', 'reason': 'doc_not_supported'})
    """
    ext = os.path.splitext(filename.lower())[1]

    # PDF: let OpenAI parse as a file attachment
    if ext == '.pdf' or content_type == 'application/pdf':
        return { 'mode': 'file', 'filename': filename, 'file_bytes': file_bytes }

    # CSV: treat as plain text
    if ext == '.csv' or content_type in ('text/csv',):
        return { 'mode': 'text', 'text': _bytes_to_text(file_bytes), 'note': 'csv-as-text' }

    # JSON: treat as plain text (preserve structure)
    if ext == '.json' or content_type in ('application/json',):
        return { 'mode': 'text', 'text': _bytes_to_text(file_bytes), 'note': 'json-as-text' }

    # Excel: convert to CSV text using pandas
    if ext in ('.xlsx', '.xls') or content_type in (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.ms-excel'
    ):
        try:
            import pandas as pd
            # Read all sheets, concatenate with sheet headers
            excel_buffer = io.BytesIO(file_bytes)
            xls = pd.ExcelFile(excel_buffer)
            parts = []
            for sheet_name in xls.sheet_names:
                df = xls.parse(sheet_name)
                csv_text = df.to_csv(index=False)
                parts.append(f"# Sheet: {sheet_name}\n{csv_text}\n")
            text = "\n".join(parts)
            return { 'mode': 'text', 'text': text, 'note': 'excel-as-csv-text' }
        except Exception as e:
            return { 'mode': 'unsupported', 'reason': f'excel_parse_failed: {e}' }

    # Word: .docx supported via python-docx
    if ext == '.docx' or content_type in (
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    ):
        try:
            import docx
            buf = io.BytesIO(file_bytes)
            document = docx.Document(buf)
            paragraphs = [p.text for p in document.paragraphs if p.text]
            text = "\n".join(paragraphs)
            return { 'mode': 'text', 'text': text, 'note': 'docx-as-text' }
        except Exception as e:
            return { 'mode': 'unsupported', 'reason': f'docx_parse_failed: {e}' }

    # Legacy .doc not supported without heavy dependencies
    if ext == '.doc' or content_type in ('application/msword',):
        return { 'mode': 'unsupported', 'reason': 'doc_not_supported' }

    # Default: try as text
    return { 'mode': 'text', 'text': _bytes_to_text(file_bytes), 'note': 'default-text' }

