import json
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_FILE = Path("leads.jsonl")


def _already_saved(url: str, output_file: Path):
    if not output_file.exists():
        return False
    try:
        with output_file.open("r", encoding="utf-8") as file:
            for line in file:
                try:
                    record = json.loads(line)
                except Exception:
                    continue
                if record.get("url") == url:
                    return True
    except Exception:
        return False
    return False


def _build_record(url: str, signal: dict, page_data: dict | None = None):
    page_data = page_data or {}
    return {
        "url": url,
        "title": page_data.get("title", ""),
        "domain": page_data.get("domain", ""),
        "meta_description": page_data.get("meta_description", ""),
        "matched_keywords": signal.get("matched_keywords", []),
        "matched_industry_terms": signal.get("matched_industry_terms", []),
        "matched_seller_terms": signal.get("matched_seller_terms", []),
        "matched_query_tokens": signal.get("matched_query_tokens", []),
        "contact_found": signal.get("contact_found", False),
        "emails": signal.get("emails", []),
        "phones": signal.get("phones", []),
        "social": signal.get("social", []),
        "score": signal.get("score"),
        "strength": signal.get("strength"),
        "priority": signal.get("priority"),
        "lead_type": signal.get("lead_type"),
        "source_type": signal.get("source_type", "web"),
        "source_name": signal.get("source_name", ""),
        "source_query": signal.get("source_query", ""),
        "detection_mode": signal.get("detection_mode", "strict"),
        "seller_detected": signal.get("seller_detected", False),
        "reasons": signal.get("reasons", []),
        "ai_used": signal.get("ai_used", False),
        "ai_model": signal.get("ai_model", ""),
        "ai_classification": signal.get("ai_classification", "neutral"),
        "ai_need_detected": signal.get("ai_need_detected", False),
        "ai_need_score": signal.get("ai_need_score", 0),
        "ai_need_stage": signal.get("ai_need_stage", "unknown"),
        "ai_buyer_probability": signal.get("ai_buyer_probability", 0),
        "ai_supplier_probability": signal.get("ai_supplier_probability", 0),
        "ai_confidence": signal.get("ai_confidence", 0),
        "ai_summary": signal.get("ai_summary", ""),
        "ai_evidence": signal.get("ai_evidence", []),
        "ai_company_candidate": signal.get("ai_company_candidate", ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def save_lead(url, signal, page_data=None, output_file: str = "leads.jsonl"):
    output_path = Path(output_file)
    if _already_saved(url, output_path):
        return False
    output_path.parent.mkdir(parents=True, exist_ok=True)
    record = _build_record(url=url, signal=signal, page_data=page_data)
    with output_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")
    return True


def save_many_leads(leads: list[dict], output_file: str = "leads.jsonl"):
    saved_count = 0
    for lead in leads:
        was_saved = save_lead(
            url=lead["url"],
            signal=lead["signal"],
            page_data=lead.get("page", {}),
            output_file=output_file,
        )
        if was_saved:
            saved_count += 1
    return saved_count
