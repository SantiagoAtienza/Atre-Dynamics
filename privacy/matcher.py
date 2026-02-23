import hashlib
import io
import re
import unicodedata
from urllib.parse import urlparse

import pandas as pd


NULL_LIKE = {"", "nan", "none", "null", "n/a", "na"}
PROMPT_COLUMN_HINTS = {
    "email": ["email", "correo", "mail", "e-mail"],
    "domain": ["domain", "dominio", "web", "website", "url", "sitio"],
    "company": ["company", "empresa", "razon social", "cliente", "nombre", "name"],
    "phone": ["phone", "telefono", "tel", "movil", "mobile"],
    "tax_id": ["cif", "nif", "vat", "tax", "fiscal"],
}


def _normalize_text(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()
    return re.sub(r"\s+", " ", normalized).strip()


def _canonical_value(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    normalized = _normalize_text(text)
    if normalized in NULL_LIKE:
        return ""

    if "@" in normalized and "." in normalized:
        return normalized

    if normalized.startswith("http://") or normalized.startswith("https://"):
        host = urlparse(normalized).netloc.replace("www.", "").strip(".")
        if host:
            return host

    if "." in normalized and " " not in normalized and "/" not in normalized and len(normalized) >= 5:
        host = normalized.replace("www.", "").strip(".")
        if host.count(".") >= 1:
            return host

    digits = re.sub(r"\D", "", normalized)
    if len(digits) >= 7:
        return digits

    clean = re.sub(r"[^a-z0-9]+", " ", normalized).strip()
    return clean


def _hash_value(value: str, secret: str):
    payload = f"{secret}::{value}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _ordered_unique(items: list[str]):
    seen = set()
    output = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def likely_key_columns(columns: list[str]):
    normalized_columns = [(_normalize_text(column), column) for column in columns]
    selected = []
    for _, hints in PROMPT_COLUMN_HINTS.items():
        for normalized, original in normalized_columns:
            if any(hint in normalized for hint in hints):
                selected.append(original)
    return _ordered_unique(selected)[:6]


def infer_columns_from_prompt(columns: list[str], prompt: str):
    if not prompt.strip():
        return likely_key_columns(columns)

    normalized_prompt = _normalize_text(prompt)
    selected = []
    for column in columns:
        normalized_column = _normalize_text(column)
        if normalized_column and normalized_column in normalized_prompt:
            selected.append(column)

    for _, hints in PROMPT_COLUMN_HINTS.items():
        if any(hint in normalized_prompt for hint in hints):
            for column in columns:
                normalized_column = _normalize_text(column)
                if any(hint in normalized_column for hint in hints):
                    selected.append(column)

    selected = _ordered_unique(selected)
    return selected or likely_key_columns(columns)


def infer_match_mode_from_prompt(prompt: str):
    normalized_prompt = _normalize_text(prompt)
    if re.search(r"\b(exacto|exact|todos|all|and|y)\b", normalized_prompt):
        return "all"
    if re.search(r"\b(cualquiera|any|or|o)\b", normalized_prompt):
        return "any"
    return "hybrid"


def _row_values(row, selected_columns: list[str]):
    values = []
    for column in selected_columns:
        if column not in row:
            continue
        value = _canonical_value(row[column])
        if value:
            values.append(value)
    return _ordered_unique(values)


def _row_fingerprints(values: list[str], secret: str, mode: str):
    fingerprints = []
    if not values:
        return fingerprints

    if mode in {"all", "hybrid"}:
        fingerprints.append(_hash_value("all::" + "|".join(values), secret))
    if mode in {"any", "hybrid"}:
        for value in values:
            fingerprints.append(_hash_value("any::" + value, secret))
    return _ordered_unique(fingerprints)


def _row_identifier(row, idx: int, row_id_column: str | None, row_prefix: str):
    if row_id_column and row_id_column in row:
        value = str(row[row_id_column]).strip()
        if value and _normalize_text(value) not in NULL_LIKE:
            return value
    return f"{row_prefix}_{idx + 1}"


def build_private_fingerprint_file(
    dataframe: pd.DataFrame,
    selected_columns: list[str],
    secret: str,
    mode: str = "hybrid",
    row_id_column: str | None = None,
    row_prefix: str = "row",
):
    if not selected_columns:
        raise ValueError("No columns selected for matching.")
    if not secret:
        raise ValueError("Secret key is required to build private fingerprints.")

    rows = []
    for idx, (_, row) in enumerate(dataframe.iterrows()):
        values = _row_values(row, selected_columns)
        fingerprints = _row_fingerprints(values, secret=secret, mode=mode)
        if not fingerprints:
            continue
        rows.append(
            {
                "row_id": _row_identifier(row, idx, row_id_column=row_id_column, row_prefix=row_prefix),
                "fingerprints": "|".join(fingerprints),
                "fingerprint_count": len(fingerprints),
            }
        )
    return pd.DataFrame(rows)


def _parse_fingerprints(raw: str):
    return {token.strip() for token in str(raw).split("|") if token.strip()}


def compare_private_fingerprint_files(client_private: pd.DataFrame, leads_private: pd.DataFrame):
    client_sets = [_parse_fingerprints(value) for value in client_private.get("fingerprints", [])]
    client_token_universe = set().union(*client_sets) if client_sets else set()

    lead_rows = []
    existing_count = 0
    for _, row in leads_private.iterrows():
        row_id = str(row.get("row_id", ""))
        lead_tokens = _parse_fingerprints(row.get("fingerprints", ""))
        overlap = client_token_universe.intersection(lead_tokens)
        status = "existing_client" if overlap else "new_lead"
        if status == "existing_client":
            existing_count += 1
        lead_rows.append(
            {
                "lead_row_id": row_id,
                "status": status,
                "matched_fingerprints": len(overlap),
            }
        )

    total_leads = len(lead_rows)
    new_count = total_leads - existing_count
    summary = {
        "client_records_compared": int(len(client_private)),
        "lead_records_compared": int(total_leads),
        "existing_clients_found": int(existing_count),
        "new_leads_found": int(new_count),
        "existing_ratio": round((existing_count / total_leads) if total_leads else 0, 4),
    }
    return {
        "summary": summary,
        "lead_status": pd.DataFrame(lead_rows),
    }


def compare_raw_dataframes(
    client_df: pd.DataFrame,
    leads_df: pd.DataFrame,
    client_columns: list[str],
    lead_columns: list[str],
    secret: str,
    mode: str = "hybrid",
    client_id_column: str | None = None,
    lead_id_column: str | None = None,
):
    client_private = build_private_fingerprint_file(
        dataframe=client_df,
        selected_columns=client_columns,
        secret=secret,
        mode=mode,
        row_id_column=client_id_column,
        row_prefix="client",
    )
    leads_private = build_private_fingerprint_file(
        dataframe=leads_df,
        selected_columns=lead_columns,
        secret=secret,
        mode=mode,
        row_id_column=lead_id_column,
        row_prefix="lead",
    )
    result = compare_private_fingerprint_files(client_private=client_private, leads_private=leads_private)
    result["client_private"] = client_private
    result["leads_private"] = leads_private
    return result


def load_table(uploaded_file):
    file_name = uploaded_file.name.lower()
    file_bytes = uploaded_file.getvalue()
    stream = io.BytesIO(file_bytes)
    if file_name.endswith(".csv"):
        return pd.read_csv(stream, dtype=str, keep_default_na=False)
    if file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        return pd.read_excel(stream, dtype=str).fillna("")
    raise ValueError("Unsupported format. Use CSV or XLSX.")
