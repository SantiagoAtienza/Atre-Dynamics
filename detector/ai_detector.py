import json
import math
import os
import re
import unicodedata

from config import settings
from config.keywords import INDUSTRY_TERMS, NEED_INTENT_TERMS, SELLER_INTENT_TERMS

#ca
BUYER_CONTEXT_TERMS = [
    "planta",
    "fabrica",
    "linea de produccion",
    "parada",
    "mantenimiento",
    "operacion",
    "facility",
    "warehouse",
    "plant",
    "production line",
    "maintenance team",
]

PROCUREMENT_TERMS = [
    "presupuesto",
    "rfq",
    "solicitud de oferta",
    "cotizacion",
    "need quote",
    "quote request",
]

INTENT_TAXONOMY = {
    "problema": ["problema", "averia", "fallo", "issue", "fault", "not working"],
    "mantenimiento": ["mantenimiento", "maintenance", "preventivo", "correctivo", "service"],
    "parada_produccion": ["parada", "downtime", "linea parada", "line stopped", "shutdown"],
    "compra": ["cotizacion", "rfq", "presupuesto", "quote", "procurement", "purchase"],
}

SUPPLIER_URL_TERMS = [
    "/producto",
    "/productos",
    "/catalog",
    "/catalogo",
    "/shop",
    "/tienda",
    "/distributor",
    "/proveedor",
    "/fabricante",
]


def _normalize(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(c for c in normalized if not unicodedata.combining(c)).lower()


def _compact_text(text: str):
    text = re.sub(r"\s+", " ", text or "").strip()
    return text[: settings.AI_TEXT_LIMIT]


def _split_sentences(text: str):
    raw = re.split(r"(?<=[\.\!\?\n])\s+", text or "")
    return [s.strip() for s in raw if len(s.strip()) >= 20][:120]


def _sigmoid(x: float):
    return 1.0 / (1.0 + math.exp(-x))


def _extract_company_candidate(title: str, organizations: list[str], domain: str):
    if organizations:
        return organizations[0]
    if title:
        first = re.split(r"[|\-:]", title)[0].strip()
        if len(first) >= 3:
            return first
    if domain:
        host = domain.replace("www.", "")
        return host.split(".")[0].replace("-", " ").strip().title()
    return ""


def _sentence_scores(sentences: list[str], keywords: list[str]):
    normalized_keywords = [_normalize(keyword) for keyword in keywords]
    scored = []
    for sentence in sentences:
        normalized = _normalize(sentence)
        kw_hits = sum(1 for keyword in normalized_keywords if keyword and keyword in normalized)
        need_hits = sum(1 for token in NEED_INTENT_TERMS if _normalize(token) in normalized)
        buyer_hits = sum(1 for token in BUYER_CONTEXT_TERMS if _normalize(token) in normalized)
        industry_hits = sum(1 for token in INDUSTRY_TERMS if _normalize(token) in normalized)
        seller_hits = sum(1 for token in SELLER_INTENT_TERMS if _normalize(token) in normalized)
        procurement_hits = sum(1 for token in PROCUREMENT_TERMS if _normalize(token) in normalized)

        need_value = kw_hits * 10 + need_hits * 13 + buyer_hits * 7 + industry_hits * 5 + procurement_hits * 9
        seller_value = seller_hits * 14
        net = need_value - seller_value
        scored.append(
            {
                "sentence": sentence,
                "need_value": need_value,
                "seller_value": seller_value,
                "net": net,
            }
        )
    scored.sort(key=lambda item: item["net"], reverse=True)
    return scored


def _infer_intent_type(text: str, need_stage: str, procurement_hits: int):
    normalized = _normalize(text)
    if procurement_hits >= 1 or any(term in normalized for term in INTENT_TAXONOMY["compra"]):
        return "compra"
    if any(term in normalized for term in INTENT_TAXONOMY["parada_produccion"]):
        return "parada_produccion"
    if any(term in normalized for term in INTENT_TAXONOMY["mantenimiento"]):
        return "mantenimiento"
    if need_stage == "active_pain" or any(term in normalized for term in INTENT_TAXONOMY["problema"]):
        return "problema"
    return "problema"


def _heuristic_need_analysis(
    text: str,
    keywords: list[str],
    title: str = "",
    meta_description: str = "",
    url: str = "",
    source_type: str = "web",
    organizations: list[str] | None = None,
):
    organizations = organizations or []
    normalized_text = _normalize(text)
    normalized_url = _normalize(url)

    sentences = _split_sentences(text)
    scored_sentences = _sentence_scores(sentences=sentences, keywords=keywords)
    evidence = [item["sentence"][:180] for item in scored_sentences[:4] if item["net"] > 0]

    keyword_hits = sum(1 for keyword in keywords if _normalize(keyword) in normalized_text)
    need_hits = sum(1 for token in NEED_INTENT_TERMS if _normalize(token) in normalized_text)
    seller_hits = sum(1 for token in SELLER_INTENT_TERMS if _normalize(token) in normalized_text)
    buyer_hits = sum(1 for token in BUYER_CONTEXT_TERMS if _normalize(token) in normalized_text)
    procurement_hits = sum(1 for token in PROCUREMENT_TERMS if _normalize(token) in normalized_text)

    url_seller_hits = sum(1 for token in SUPPLIER_URL_TERMS if token in normalized_url)
    source_bonus = {
        "web": 6,
        "news": 4,
        "review": 1,
        "forum": -4,
        "social": -5,
    }.get(source_type, 0)
    title_bonus = 5 if any(_normalize(keyword) in _normalize(title + " " + meta_description) for keyword in keywords) else 0
    org_bonus = 5 if organizations else 0

    need_signal = (
        keyword_hits * 8
        + need_hits * 14
        + buyer_hits * 7
        + procurement_hits * 9
        + source_bonus
        + title_bonus
        + org_bonus
    )
    seller_signal = seller_hits * 11 + url_seller_hits * 14

    need_score = int(max(0, min(100, need_signal - seller_signal * 0.55)))
    buyer_probability = round(_sigmoid((need_signal - seller_signal - 20) / 12), 3)
    supplier_probability = round(_sigmoid((seller_signal - need_signal + 10) / 11), 3)

    if supplier_probability >= 0.62 and need_score < 65:
        classification = "supplier_offer"
    elif need_score >= 40 and buyer_probability >= 0.52:
        classification = "prospect_need"
    else:
        classification = "neutral"

    need_detected = classification == "prospect_need"
    if need_hits >= 2:
        need_stage = "active_pain"
    elif procurement_hits >= 1:
        need_stage = "procurement"
    elif buyer_hits >= 1:
        need_stage = "planning"
    else:
        need_stage = "unknown"

    if need_detected:
        summary = "Potential buyer need detected from problem/procurement context."
    elif classification == "supplier_offer":
        summary = "Page looks like supplier/vendor offering products or services."
    else:
        summary = "No strong buyer need signal."

    intent_type = _infer_intent_type(
        text=text,
        need_stage=need_stage,
        procurement_hits=procurement_hits,
    )

    return {
        "ai_used": False,
        "model": "advanced-heuristic-v2",
        "classification": classification,
        "need_detected": need_detected,
        "need_score": need_score,
        "buyer_probability": buyer_probability,
        "supplier_probability": supplier_probability,
        "confidence": round(max(buyer_probability, supplier_probability), 3),
        "need_stage": need_stage,
        "intent_type": intent_type,
        "need_summary": summary,
        "evidence": evidence[:3],
        "company_candidate": _extract_company_candidate(
            title=title,
            organizations=organizations,
            domain=urlparse_domain(url),
        ),
        "seller_hits": seller_hits,
        "need_hits": need_hits,
    }


def urlparse_domain(url: str):
    try:
        parsed = re.sub(r"^https?://", "", url or "").split("/")[0]
        return parsed.strip().lower()
    except Exception:
        return ""


def _call_openai(
    text: str,
    keywords: list[str],
    title: str = "",
    meta_description: str = "",
    url: str = "",
    source_type: str = "web",
    organizations: list[str] | None = None,
    heuristic: dict | None = None,
):
    organizations = organizations or []
    heuristic = heuristic or {}
    try:
        from openai import OpenAI
    except Exception as error:
        result = dict(heuristic)
        result["ai_error"] = f"openai package missing: {error}"
        return result

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return heuristic

    evidence = heuristic.get("evidence", [])
    evidence_block = "\n".join(f"- {item}" for item in evidence[:4])
    context = (
        f"URL: {url}\n"
        f"SOURCE_TYPE: {source_type}\n"
        f"TITLE: {title}\n"
        f"META: {meta_description}\n"
        f"ORGANIZATION_CANDIDATES: {', '.join(organizations[:5])}\n"
        f"KEYWORDS: {', '.join(keywords)}\n"
        f"HEURISTIC_CLASSIFICATION: {heuristic.get('classification')}\n"
        f"HEURISTIC_NEED_SCORE: {heuristic.get('need_score')}\n"
        f"HEURISTIC_EVIDENCE:\n{evidence_block}\n\n"
        f"TEXT:\n{_compact_text(text)}"
    )

    prompt = (
        "Classify whether the page expresses BUYER NEED for the keywords. "
        "Reject supplier catalogs, distributors, and service offering pages as leads. "
        "Return strict JSON only with keys: "
        "classification (prospect_need|supplier_offer|neutral), "
        "need_score (0-100 int), "
        "buyer_probability (0-1 float), "
        "supplier_probability (0-1 float), "
        "confidence (0-1 float), "
        "need_stage (active_pain|procurement|planning|unknown), "
        "intent_type (problema|mantenimiento|parada_produccion|compra), "
        "need_summary (string), "
        "evidence (array max 3 strings), "
        "company_candidate (string)."
    )

    client = OpenAI(api_key=api_key)
    try:
        completion = client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are an expert B2B buyer-intent analyst."},
                {"role": "user", "content": prompt + "\n\n" + context},
            ],
        )
        content = completion.choices[0].message.content or "{}"
        payload = json.loads(content)
    except Exception as error:
        result = dict(heuristic)
        result["ai_error"] = f"openai request failed: {error}"
        return result

    classification = str(payload.get("classification", "")).strip().lower()
    if classification not in {"prospect_need", "supplier_offer", "neutral"}:
        classification = heuristic.get("classification", "neutral")

    need_score = int(payload.get("need_score", heuristic.get("need_score", 0)))
    need_score = max(0, min(100, need_score))
    buyer_probability = float(payload.get("buyer_probability", heuristic.get("buyer_probability", 0.5)))
    buyer_probability = max(0.0, min(1.0, buyer_probability))
    supplier_probability = float(payload.get("supplier_probability", heuristic.get("supplier_probability", 0.5)))
    supplier_probability = max(0.0, min(1.0, supplier_probability))
    confidence = float(payload.get("confidence", max(buyer_probability, supplier_probability)))
    confidence = max(0.0, min(1.0, confidence))

    evidence = payload.get("evidence", [])
    if not isinstance(evidence, list):
        evidence = []

    company_candidate = str(payload.get("company_candidate", "")).strip()
    if not company_candidate:
        company_candidate = heuristic.get("company_candidate", "")

    intent_type = str(payload.get("intent_type", "")).strip().lower()
    if intent_type not in {"problema", "mantenimiento", "parada_produccion", "compra"}:
        intent_type = heuristic.get("intent_type", "problema")

    return {
        "ai_used": True,
        "model": settings.OPENAI_MODEL,
        "classification": classification,
        "need_detected": classification == "prospect_need",
        "need_score": need_score,
        "buyer_probability": round(buyer_probability, 3),
        "supplier_probability": round(supplier_probability, 3),
        "confidence": round(confidence, 3),
        "need_stage": str(payload.get("need_stage", heuristic.get("need_stage", "unknown"))).strip().lower(),
        "intent_type": intent_type,
        "need_summary": str(payload.get("need_summary", heuristic.get("need_summary", "")))[:320],
        "evidence": [str(item)[:180] for item in evidence[:3]] or heuristic.get("evidence", []),
        "company_candidate": company_candidate,
        "seller_hits": heuristic.get("seller_hits", 0),
        "need_hits": heuristic.get("need_hits", 0),
        "heuristic_need_score": heuristic.get("need_score", 0),
    }


def analyze_need(
    text: str,
    keywords: list[str],
    title: str = "",
    meta_description: str = "",
    url: str = "",
    source_type: str = "web",
    organizations: list[str] | None = None,
):
    heuristic = _heuristic_need_analysis(
        text=text,
        keywords=keywords,
        title=title,
        meta_description=meta_description,
        url=url,
        source_type=source_type,
        organizations=organizations or [],
    )
    if settings.USE_OPENAI:
        return _call_openai(
            text=text,
            keywords=keywords,
            title=title,
            meta_description=meta_description,
            url=url,
            source_type=source_type,
            organizations=organizations or [],
            heuristic=heuristic,
        )
    return heuristic
