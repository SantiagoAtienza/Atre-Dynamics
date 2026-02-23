import re
import unicodedata
from urllib.parse import urlparse

from config import settings
from config.keywords import INDUSTRY_TERMS, KEYWORDS, SELLER_INTENT_TERMS
from detector.ai_detector import analyze_need

PHONE_RE = re.compile(r"\+?\d[\d\s\-()]{6,}\d")
EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")

HARD_SELLER_DOMAINS = {
    "directindustry.es",
    "europages.es",
    "kompass.com",
    "solostocks.com",
    "amazon.es",
    "mercadolibre.com",
    "alibaba.com",
}


def _normalize(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(c for c in normalized if not unicodedata.combining(c)).lower()


def _count_terms(text: str, terms: list):
    text_l = _normalize(text)
    count = 0
    matched = []
    for term in terms:
        normalized = _normalize(term)
        if normalized in text_l:
            matched.append(term)
            count += text_l.count(normalized)
    return count, matched


def _extract_query_tokens(terms: list[str]):
    stopwords = {"de", "la", "el", "los", "las", "para", "por", "and", "the", "with"}
    tokens = set()
    for term in terms:
        for token in _normalize(term).split():
            if len(token) >= 4 and token not in stopwords:
                tokens.add(token)
    return sorted(tokens)


def _map_strength(score: int):
    if score >= 85:
        return "strong"
    if score >= 55:
        return "medium"
    return "weak"


def _map_priority(score: int):
    if score >= 95:
        return "A"
    if score >= 65:
        return "B"
    return "C"


def _is_hard_seller_domain(url: str):
    domain = urlparse(url or "").netloc.lower()
    return any(domain == blocked or domain.endswith(f".{blocked}") for blocked in HARD_SELLER_DOMAINS)


def detect_signal(
    parsed_page: dict,
    query_keywords: list[str] | None = None,
    source_info: dict | None = None,
    include_suppliers: bool = False,
    min_need_score: int | None = None,
    strict_mode: bool = True,
):
    source_info = source_info or {}
    text = parsed_page.get("text", "") or ""
    if not text.strip():
        return None

    url = parsed_page.get("url", "") or source_info.get("url", "")
    source_type = source_info.get("source_type", "web")
    has_reviews = bool(parsed_page.get("has_reviews", False))
    has_forum_context = bool(parsed_page.get("has_forum_context", False))
    active_keywords = query_keywords or KEYWORDS

    keyword_count, matched_keywords = _count_terms(text, active_keywords)
    industry_count, matched_industry = _count_terms(text, INDUSTRY_TERMS)
    seller_term_count, matched_seller_terms = _count_terms(text, SELLER_INTENT_TERMS)

    normalized_text = _normalize(text)
    query_tokens = _extract_query_tokens(active_keywords)
    matched_query_tokens = [token for token in query_tokens if token in normalized_text]

    contact_found = bool(
        PHONE_RE.search(text)
        or EMAIL_RE.search(text)
        or "contacto" in normalized_text
        or "contact" in normalized_text
    )

    ai_result = analyze_need(
        text=text,
        keywords=active_keywords,
        title=parsed_page.get("title", ""),
        meta_description=parsed_page.get("meta_description", ""),
        url=url,
        source_type=source_type,
        organizations=parsed_page.get("organization_candidates", []),
    )

    need_score = int(ai_result.get("need_score", 0))
    buyer_probability = float(ai_result.get("buyer_probability", 0.0))
    supplier_probability = float(ai_result.get("supplier_probability", 0.0))
    classification = str(ai_result.get("classification", "neutral"))

    supplier_detected = (
        classification == "supplier_offer"
        or supplier_probability > settings.SUPPLIER_MAX_PROB
        or len(matched_seller_terms) >= 4
        or _is_hard_seller_domain(url)
    )

    source_bonus = {
        "web": 8,
        "news": 5,
        "review": 1,
        "forum": -3,
        "social": -5,
    }.get(source_type, 0)

    # Base score now prioritizes AI need detection and buyer signals, not provider text density.
    score = (
        need_score * 0.74
        + min(keyword_count, 5) * 5
        + min(industry_count, 6) * 4
        + min(len(matched_query_tokens), 4) * 4
        + (4 if has_reviews else 0)
        + (10 if contact_found else 0)
        + (6 if parsed_page.get("organization_candidates") else 0)
        + source_bonus
    )
    if has_forum_context and source_type in {"forum", "social"}:
        score -= 8
    score -= min(len(matched_seller_terms), 4) * 6
    if supplier_detected:
        score -= 25
    score = int(max(0, min(100, round(score))))

    threshold = min_need_score if min_need_score is not None else settings.MIN_NEED_SCORE

    if supplier_detected and not include_suppliers:
        if strict_mode:
            return None
        if buyer_probability < 0.72 and need_score < threshold + 18:
            return None

    if strict_mode:
        if need_score < threshold and buyer_probability < 0.58:
            return None
        if classification == "neutral" and buyer_probability < 0.62:
            return None
        if source_type in {"forum", "social"} and buyer_probability < 0.78:
            return None
    else:
        relaxed_threshold = max(18, threshold - 22)
        if need_score < relaxed_threshold and buyer_probability < 0.45:
            return None
        if classification == "neutral" and buyer_probability < 0.50 and need_score < relaxed_threshold + 8:
            return None
        if source_type in {"forum", "social"} and buyer_probability < 0.60:
            return None

    token_overlap = len(matched_query_tokens)
    if strict_mode:
        if token_overlap == 0 and classification != "prospect_need":
            return None
        if token_overlap == 1 and classification != "prospect_need" and need_score < threshold + 12:
            return None
    else:
        if token_overlap == 0 and classification != "prospect_need" and buyer_probability < 0.62 and need_score < threshold + 10:
            return None

    strength = _map_strength(score)
    priority = _map_priority(score)
    lead_type = ai_result.get("need_stage", "unknown")

    reasons = []
    if ai_result.get("need_detected"):
        reasons.append("ai buyer need detected")
    if matched_keywords:
        reasons.append("keyword match")
    if matched_industry:
        reasons.append("industry context")
    if has_forum_context:
        reasons.append("forum discussion context")
    if has_reviews:
        reasons.append("review/opinion context")
    if contact_found:
        reasons.append("contact details found")

    return {
        "matched_keywords": matched_keywords,
        "matched_industry_terms": matched_industry,
        "matched_query_tokens": matched_query_tokens,
        "matched_seller_terms": matched_seller_terms,
        "contact_found": contact_found,
        "emails": parsed_page.get("emails", []),
        "phones": parsed_page.get("phones", []),
        "social": parsed_page.get("social", []),
        "has_reviews": has_reviews,
        "has_forum_context": has_forum_context,
        "score": score,
        "strength": strength,
        "priority": priority,
        "lead_type": lead_type,
        "reasons": reasons,
        "source_type": source_type,
        "source_name": source_info.get("source_name", ""),
        "source_query": source_info.get("query", ""),
        "seller_detected": supplier_detected,
        "ai_used": ai_result.get("ai_used", False),
        "ai_model": ai_result.get("model", "advanced-heuristic-v2"),
        "ai_classification": classification,
        "ai_need_detected": ai_result.get("need_detected", False),
        "ai_need_score": need_score,
        "ai_buyer_probability": round(buyer_probability, 3),
        "ai_supplier_probability": round(supplier_probability, 3),
        "ai_confidence": ai_result.get("confidence", 0.0),
        "ai_need_stage": ai_result.get("need_stage", "unknown"),
        "ai_intent_type": ai_result.get("intent_type", "problema"),
        "ai_summary": ai_result.get("need_summary", ""),
        "ai_evidence": ai_result.get("evidence", []),
        "ai_company_candidate": ai_result.get("company_candidate", ""),
        "ai_error": ai_result.get("ai_error", ""),
        "detection_mode": "strict" if strict_mode else "relaxed",
    }
