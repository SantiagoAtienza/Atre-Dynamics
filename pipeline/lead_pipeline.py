from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from urllib.parse import urlparse

from config import settings
from crawler.crawler import fetch_many_details
from detector.ai_detector import analyze_need
from detector.detector import detect_signal
from discover.discover import discover_candidates
from discover.query_generator import generate_search_queries
from discover.seed_expander import expand_site_to_urls
from parser.parser import parse_page
from pipeline.lead_config import LeadSearchConfig

LARGE_ECOMMERCE_DOMAINS = {
    "amazon.com",
    "amazon.es",
    "mercadolibre.com",
    "alibaba.com",
    "aliexpress.com",
    "ebay.com",
    "walmart.com",
    "etsy.com",
    "temu.com",
}

FORUM_DOMAINS = {
    "reddit.com",
    "www.reddit.com",
    "forocoches.com",
    "www.forocoches.com",
    "quora.com",
    "www.quora.com",
    "stackexchange.com",
    "stackoverflow.com",
}

SOCIAL_DOMAINS = {
    "linkedin.com",
    "www.linkedin.com",
    "x.com",
    "www.x.com",
    "twitter.com",
    "www.twitter.com",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "youtube.com",
    "www.youtube.com",
}

REVIEW_DOMAINS = {
    "trustpilot.com",
    "www.trustpilot.com",
    "g2.com",
    "www.g2.com",
    "capterra.com",
    "www.capterra.com",
    "consumeraffairs.com",
    "www.consumeraffairs.com",
}

NON_HTML_EXTENSIONS = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".zip",
    ".rar",
    ".mp4",
    ".mp3",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
)

CATEGORY_HINTS = [
    "/categoria",
    "/categorias",
    "/category",
    "/categories",
    "/catalog",
    "/catalogo",
    "/tienda",
    "/shop",
    "/store",
    "/productos",
    "/products",
    "/collections",
    "/product-category",
]

LOW_QUALITY_INFO_DOMAINS = {
    "wikipedia.org",
    "www.wikipedia.org",
    "wikidata.org",
    "www.wikidata.org",
    "fandom.com",
    "www.fandom.com",
    "wikia.com",
    "www.wikia.com",
    "wiktionary.org",
    "www.wiktionary.org",
}

COMPANY_PATH_HINTS = [
    "/empresa",
    "/about",
    "/nosotros",
    "/cliente",
    "/clientes",
    "/contact",
    "/contacto",
    "/case",
    "/caso",
    "/casos",
    "/proyecto",
    "/proyectos",
    "/servicio",
    "/servicios",
    "/blog",
    "/insights",
    "/news",
    "/noticias",
]

LOW_INTENT_PATH_HINTS = [
    "/wiki/",
    "/thread/",
    "/threads/",
    "/forum/",
    "/forums/",
    "/community/",
    "/tag/",
    "/tags/",
    "/topic/",
    "/search",
]

SOURCE_RANK_BOOST = {
    "web": 10,
    "news": 7,
    "review": 2,
    "forum": -3,
    "social": -4,
}


def _normalize(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _dedupe_strings(values: list[str]):
    output = []
    seen = set()
    for value in values:
        item = (value or "").strip()
        if not item:
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _keyword_tokens(keywords: list[str]):
    tokens = set()
    for keyword in keywords:
        for token in re.split(r"[^a-z0-9]+", _normalize(keyword)):
            if len(token) >= 4:
                tokens.add(token)
    return sorted(tokens)


def _build_snippet(text: str, max_len=260):
    value = (text or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _count_by(values: list[str]):
    output = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return output


def _domain(url: str):
    return urlparse(url or "").netloc.lower()


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


def _source_type_from_url(url: str):
    domain = _domain(url)
    if any(domain == item or domain.endswith(f".{item}") for item in FORUM_DOMAINS):
        return "forum"
    if any(domain == item or domain.endswith(f".{item}") for item in SOCIAL_DOMAINS):
        return "social"
    if any(domain == item or domain.endswith(f".{item}") for item in REVIEW_DOMAINS):
        return "review"
    return "web"


def _is_large_ecommerce(url: str):
    domain = _domain(url)
    return any(domain == blocked or domain.endswith(f".{blocked}") for blocked in LARGE_ECOMMERCE_DOMAINS)


def _is_low_quality_info_domain(url: str):
    domain = _domain(url)
    return any(domain == blocked or domain.endswith(f".{blocked}") for blocked in LOW_QUALITY_INFO_DOMAINS)


def _looks_like_homepage(url: str):
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip()
    return path in {"", "/"}


def _looks_like_category(url: str):
    lower = (url or "").lower()
    if any(hint in lower for hint in CATEGORY_HINTS):
        return True
    parsed = urlparse(lower)
    path = parsed.path or ""
    if path.count("/") <= 1 and any(token in parsed.query for token in ["page=", "sort=", "filter="]):
        return True
    if re.search(r"/(tag|topic|search)/", path):
        return True
    return False


def _looks_like_non_html(url: str):
    lower = (url or "").lower()
    return any(lower.endswith(ext) for ext in NON_HTML_EXTENSIONS)


def _source_allowed(source_type: str, allowed_sources: set[str]):
    if source_type in allowed_sources:
        return True
    if source_type == "news" and "web" in allowed_sources:
        return True
    if source_type == "seed":
        return True
    return False


def _url_filter_reason(url: str, source_type: str, keep_search_pages: bool = False):
    if not url or not url.startswith("http"):
        return "invalid_url"
    if _is_large_ecommerce(url):
        return "large_ecommerce"
    if _is_low_quality_info_domain(url):
        return "low_quality_info_domain"
    if _looks_like_non_html(url):
        return "non_html"
    if not keep_search_pages and _looks_like_homepage(url):
        return "homepage"
    if not keep_search_pages and _looks_like_category(url):
        return "category_listing"
    if any(hint in (url or "").lower() for hint in LOW_INTENT_PATH_HINTS):
        return "low_intent_path"
    if "?" in (url or "") and any(token in url.lower() for token in ["utm_", "fbclid", "gclid"]):
        return "tracking_url"
    if source_type in {"forum", "review", "social"} and _looks_like_category(url):
        return "forum_or_social_listing"
    return ""


def _build_summary(leads: list[dict], searched_count: int):
    strengths = [lead["signal"].get("strength", "weak") for lead in leads]
    priorities = [lead["signal"].get("priority", "C") for lead in leads]
    source_types = [lead["signal"].get("source_type", "web") for lead in leads]
    need_stages = [lead["signal"].get("ai_need_stage", "unknown") for lead in leads]
    intents = [lead["signal"].get("ai_intent_type", "problema") for lead in leads]
    detection_modes = [lead["signal"].get("detection_mode", "strict") for lead in leads]
    validated_need = sum(1 for lead in leads if lead["signal"].get("ai_need_detected", False))

    return {
        "searched_urls": searched_count,
        "total_leads": len(leads),
        "need_validated": validated_need,
        "by_strength": _count_by(strengths) or {"strong": 0, "medium": 0, "weak": 0},
        "by_priority": _count_by(priorities) or {"A": 0, "B": 0, "C": 0},
        "by_source_type": _count_by(source_types),
        "by_need_stage": _count_by(need_stages),
        "by_intent_type": _count_by(intents),
        "by_detection_mode": _count_by(detection_modes),
        "avg_score": round(sum(lead["signal"].get("score", 0) for lead in leads) / len(leads), 2) if leads else 0,
        "avg_need_score": round(sum(lead["signal"].get("ai_need_score", 0) for lead in leads) / len(leads), 2)
        if leads
        else 0,
    }


def _candidate_sort_key(candidate: dict):
    source_type = candidate.get("source_type", "web")
    url = candidate.get("url", "")
    lower = url.lower()
    quality = 0
    quality += SOURCE_RANK_BOOST.get(source_type, 0)
    quality += 2 if any(hint in lower for hint in COMPANY_PATH_HINTS) else 0
    quality -= 3 if any(hint in lower for hint in LOW_INTENT_PATH_HINTS) else 0
    quality -= 1 if "?" in lower else 0
    return -quality


def _company_signal_score(parsed: dict, source_info: dict):
    url = (parsed.get("url") or source_info.get("url") or "").lower()
    source_type = source_info.get("source_type", "web")
    score = 0

    if source_type == "web":
        score += 2
    elif source_type == "news":
        score += 1
    elif source_type in {"forum", "social"}:
        score -= 2

    if parsed.get("organization_candidates"):
        score += 2
    if parsed.get("emails") or parsed.get("phones"):
        score += 2
    if any(hint in url for hint in COMPANY_PATH_HINTS):
        score += 1
    if parsed.get("has_forum_context", False):
        score -= 2
    if any(hint in url for hint in LOW_INTENT_PATH_HINTS):
        score -= 2

    return score


def _looks_like_content_page(url: str):
    lower = (url or "").lower()
    content_hints = ["/blog/", "/blogs/", "/article/", "/articulo/", "/insights/", "/news/", "/noticias/"]
    return any(hint in lower for hint in content_hints)


def _seed_candidates(seed_sites: list[str], max_results: int, expand_per_site: int = 8):
    candidates = []
    for seed in seed_sites[: max(3, max_results // 2)]:
        seed_source = _source_type_from_url(seed)
        candidates.append(
            {
                "url": seed,
                "source_type": seed_source,
                "source_name": "seed_file",
                "query": "seed",
            }
        )

        if _url_filter_reason(seed, seed_source):
            continue

        for expanded in expand_site_to_urls(seed, max_per_site=expand_per_site):
            expanded_source = _source_type_from_url(expanded)
            if _url_filter_reason(expanded, expanded_source):
                continue
            candidates.append(
                {
                    "url": expanded,
                    "source_type": expanded_source,
                    "source_name": "seed_expander",
                    "query": "seed_expander",
                }
            )
            if len(candidates) >= max_results:
                break
        if len(candidates) >= max_results:
            break

    return list({candidate["url"]: candidate for candidate in candidates}.values())[:max_results]


def _is_search_like(url: str, source_type: str):
    lower = (url or "").lower()
    if ".json" in lower or "/api/" in lower or "rss" in lower:
        return True
    if "search?" in lower or "/search/" in lower:
        return True
    if "hn.algolia.com" in lower:
        return True
    return source_type in {"forum", "review", "news"} and ("?" in lower)


def _is_noise_link(url: str):
    lower = (url or "").lower()
    bad_fragments = [
        "reddit.com/policies",
        "reddithelp.com",
        "wikipedia.org/wiki/",
        "wikidata.org/wiki/",
        "/privacy",
        "/terms",
        "source=trending",
        "/search/?q=",
        "/tag/",
        "/topic/",
        "google.com/search",
        "accounts.google.com",
        ".pdf",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
    ]
    return any(fragment in lower for fragment in bad_fragments)


def _link_relevant(link: str, keyword_tokens: list[str], parent_query: str):
    normalized_link = _normalize(link)
    overlap = sum(1 for token in keyword_tokens if token in normalized_link)
    if overlap >= 1:
        return True

    if any(path_hint in normalized_link for path_hint in COMPANY_PATH_HINTS):
        return True

    query_tokens = [token for token in re.split(r"[^a-z0-9]+", _normalize(parent_query)) if len(token) >= 5]
    return sum(1 for token in query_tokens if token in normalized_link) >= 1


def _should_expand_from_source(url: str, source_type: str, depth: int, max_depth: int, extended_scrape: bool):
    if depth >= max_depth:
        return False
    if _is_search_like(url, source_type):
        return True
    if source_type in {"news", "review", "seed"}:
        return True
    if source_type in {"forum", "social"}:
        return extended_scrape and depth == 0
    if source_type == "web":
        if depth == 0:
            return extended_scrape
        if extended_scrape and depth == 1 and _looks_like_content_page(url):
            return True
    return False


def _collect_expanded_urls(
    parsed: dict,
    parent_source: dict,
    keyword_tokens: list[str],
    max_links: int,
    allow_wider: bool,
):
    urls = []
    parent_url = parent_source.get("url", "")
    parent_url_lower = parent_url.lower()
    force_allow = "news.google.com/rss" in parent_url_lower
    reddit_search = "reddit.com" in parent_url_lower and "/search/" in parent_url_lower
    broad_hints = [
        "/contact",
        "/empresa",
        "/about",
        "/servicio",
        "/proyecto",
        "/cliente",
        "/case",
        "/solution",
        "/blog",
        "/insight",
        "/news",
        "/noticias",
    ]

    for link in parsed.get("links", []):
        if not isinstance(link, str):
            continue
        if not link.startswith("http"):
            continue
        if link == parent_url:
            continue
        if _is_noise_link(link):
            continue
        if any(hint in link.lower() for hint in LOW_INTENT_PATH_HINTS):
            continue

        if not force_allow:
            relevant = False
            if reddit_search and "/comments/" in link.lower():
                relevant = True
            elif _link_relevant(link, keyword_tokens=keyword_tokens, parent_query=parent_source.get("query", "")):
                relevant = True
            elif allow_wider and any(hint in link.lower() for hint in broad_hints):
                relevant = True
            if not relevant:
                continue

        clean_link = link.split("#")[0].strip()
        if not clean_link:
            continue
        urls.append(clean_link)
        if len(urls) >= max_links:
            break

    return list(dict.fromkeys(urls))


def _analyze_parsed_page(
    parsed: dict,
    keywords: list[str],
    source_info: dict,
    include_suppliers: bool,
    min_need_score: int,
    strict_mode: bool,
):
    company_score = _company_signal_score(parsed=parsed, source_info=source_info)
    if strict_mode and company_score <= -1:
        return None

    signal = detect_signal(
        parsed_page=parsed,
        query_keywords=keywords,
        source_info=source_info,
        include_suppliers=include_suppliers,
        min_need_score=min_need_score,
        strict_mode=strict_mode,
    )
    if not signal:
        return None
    signal["company_signal_score"] = company_score
    if company_score >= 3:
        signal.setdefault("reasons", []).append("company profile signal")

    return {
        "url": parsed.get("url", source_info.get("url", "")),
        "page": parsed,
        "signal": signal,
        "snippet": _build_snippet(parsed.get("text", "")),
    }


def _build_emergency_lead(parsed: dict, source_info: dict, keywords: list[str], enforce_minimum: bool = False):
    text = parsed.get("text", "") or ""
    if len(text.strip()) < 60:
        return None
    company_score = _company_signal_score(parsed=parsed, source_info=source_info)

    ai_result = analyze_need(
        text=text,
        keywords=keywords,
        title=parsed.get("title", ""),
        meta_description=parsed.get("meta_description", ""),
        url=parsed.get("url", source_info.get("url", "")),
        source_type=source_info.get("source_type", "web"),
        organizations=parsed.get("organization_candidates", []),
    )
    supplier_probability = float(ai_result.get("supplier_probability", 0.0))
    buyer_probability = float(ai_result.get("buyer_probability", 0.0))
    need_score = int(ai_result.get("need_score", 0))
    classification = str(ai_result.get("classification", "neutral"))

    if not enforce_minimum and classification == "supplier_offer" and supplier_probability >= 0.85 and buyer_probability < 0.55:
        return None

    score = int(
        max(
            0,
            min(
                100,
                round(
                    need_score * 0.66
                    + buyer_probability * 36
                    - supplier_probability * 24
                    + company_score * 6
                    + SOURCE_RANK_BOOST.get(source_info.get("source_type", "web"), 0)
                ),
            ),
        )
    )
    if not enforce_minimum and score < 24:
        return None

    signal = {
        "matched_keywords": [],
        "matched_industry_terms": [],
        "matched_query_tokens": [],
        "matched_seller_terms": [],
        "contact_found": bool(parsed.get("emails") or parsed.get("phones")),
        "emails": parsed.get("emails", []),
        "phones": parsed.get("phones", []),
        "social": parsed.get("social", []),
        "has_reviews": bool(parsed.get("has_reviews", False)),
        "has_forum_context": bool(parsed.get("has_forum_context", False)),
        "score": score,
        "strength": _map_strength(score),
        "priority": _map_priority(score),
        "lead_type": ai_result.get("need_stage", "unknown"),
        "reasons": ["force minimum lead candidate" if enforce_minimum else "emergency fallback candidate"],
        "source_type": source_info.get("source_type", "web"),
        "source_name": source_info.get("source_name", ""),
        "source_query": source_info.get("query", ""),
        "company_signal_score": company_score,
        "seller_detected": classification == "supplier_offer",
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
        "detection_mode": "force_minimum" if enforce_minimum else "emergency",
    }
    return {
        "url": parsed.get("url", source_info.get("url", "")),
        "page": parsed,
        "signal": signal,
        "snippet": _build_snippet(parsed.get("text", "")),
    }


def _lead_rank(item: dict):
    signal = item.get("signal", {})
    source_type = signal.get("source_type", "web")
    company_score = signal.get("company_signal_score", 0)
    weighted_score = signal.get("score", 0) + SOURCE_RANK_BOOST.get(source_type, 0) + company_score * 3
    return (
        weighted_score,
        signal.get("ai_need_score", 0),
        signal.get("ai_buyer_probability", 0),
    )


def phase_configuracion(
    *,
    lead_config: LeadSearchConfig | None,
    keywords: list[str] | None,
    product_need: str | None,
    country: str,
    language: str | None,
    target_leads: int,
    source_types: list[str] | None,
    intent_type: str,
    include_suppliers: bool,
    min_need_score: int,
    extended_scrape: bool,
    link_expansion_depth: int | None,
    links_per_page: int | None,
):
    if lead_config is not None:
        return lead_config

    keyword_values = _dedupe_strings(keywords or [])
    inferred_product = (product_need or "").strip() or ", ".join(keyword_values[:2]) or "necesidad industrial"
    return LeadSearchConfig.from_inputs(
        product_need=inferred_product,
        country=country,
        language=language,
        target_leads=max(1, int(target_leads or 1)),
        source_types=source_types,
        intent_type=intent_type,
        keywords=keyword_values,
        min_need_score=min_need_score,
        include_suppliers=include_suppliers,
        extended_scrape=extended_scrape,
        link_expansion_depth=link_expansion_depth,
        links_per_page=links_per_page,
    )


def phase_generacion_queries(config: LeadSearchConfig, max_results: int, preset_queries: list[str] | None = None):
    if preset_queries:
        return {
            "queries": _dedupe_strings(preset_queries),
            "generator_mode": "manual",
            "ai_error": "",
        }

    query_budget = min(settings.DISCOVERY_MAX_QUERIES, max(8, max_results * 2))
    generated = generate_search_queries(config=config, max_queries=query_budget)
    queries = _dedupe_strings(generated.get("queries", []))
    if not queries:
        queries = _dedupe_strings(config.keywords)

    return {
        "queries": queries[:query_budget],
        "generator_mode": generated.get("generator_mode", "heuristic"),
        "ai_error": generated.get("ai_error", ""),
    }


def phase_descubrimiento_urls(
    config: LeadSearchConfig,
    queries: list[str],
    max_results: int,
    per_query: int,
    seed_sites: list[str] | None = None,
):
    discovery_target = min(
        settings.DISCOVERY_MAX_SEARCH_BUDGET,
        max(max_results * (8 if config.extended_scrape else 4), 60),
    )
    if seed_sites:
        candidates = _seed_candidates(
            seed_sites=seed_sites,
            max_results=discovery_target,
            expand_per_site=18 if config.extended_scrape else 8,
        )
    else:
        candidates = discover_candidates(
            keywords=config.keywords,
            max_results=discovery_target,
            per_query=per_query,
            country=config.country,
            queries=queries,
            source_types=config.source_types,
        )
    return list({candidate["url"]: candidate for candidate in candidates}.values())


def phase_filtrado_urls(config: LeadSearchConfig, candidates: list[dict], max_results: int):
    filtered = []
    removed = defaultdict(int)
    allowed_sources = set(config.source_types)
    domain_counts = defaultdict(int)
    domain_caps = {
        "web": 10,
        "news": 8,
        "review": 4,
        "forum": 2,
        "social": 2,
    }

    sorted_candidates = sorted(candidates, key=_candidate_sort_key)
    for candidate in sorted_candidates:
        url = candidate.get("url", "")
        source_type = candidate.get("source_type", "web")
        if source_type == "seed":
            source_type = _source_type_from_url(url)

        if not _source_allowed(source_type, allowed_sources):
            removed["source_type_filtered"] += 1
            continue

        reason = _url_filter_reason(url, source_type)
        if reason:
            removed[reason] += 1
            continue

        domain = _domain(url)
        cap = domain_caps.get(source_type, 4)
        if domain and domain_counts[domain] >= cap:
            removed["domain_cap_reached"] += 1
            continue

        filtered.append(
            {
                "url": url,
                "source_type": source_type,
                "source_name": candidate.get("source_name", ""),
                "query": candidate.get("query", ""),
            }
        )
        if domain:
            domain_counts[domain] += 1
        if len(filtered) >= max(max_results * 4, max_results + 12):
            break

    return filtered, {
        "input_candidates": len(candidates),
        "kept_for_scrape": len(filtered),
        "removed_reasons": dict(removed),
    }


def phase_scraping(config: LeadSearchConfig, candidates: list[dict], concurrency: int, max_results: int):
    source_map = {candidate["url"]: candidate for candidate in candidates}
    keyword_tokens = _keyword_tokens(config.keywords)

    max_depth = config.link_expansion_depth
    if max_depth is None:
        max_depth = settings.DISCOVERY_LINK_EXPANSION_DEPTH if config.extended_scrape else 1
    max_links_per_page = config.links_per_page
    if max_links_per_page is None:
        max_links_per_page = settings.DISCOVERY_LINKS_PER_PAGE if config.extended_scrape else 10

    search_budget = min(
        settings.DISCOVERY_MAX_SEARCH_BUDGET,
        max(max_results * (10 if config.extended_scrape else 4), len(candidates) * 3),
    )

    pending = [(candidate["url"], 0) for candidate in candidates]
    queued_urls = {candidate["url"] for candidate in candidates}
    fetched_urls = set()
    searched_urls = []
    parsed_cache = {}
    scrape_errors = defaultdict(int)
    blocked_codes = defaultdict(int)

    while pending and len(fetched_urls) < search_budget:
        batch = []
        depth_by_url = {}
        batch_size = max(concurrency * 3, 12)
        while pending and len(batch) < batch_size and len(fetched_urls) + len(batch) < search_budget:
            url, depth = pending.pop(0)
            if url in fetched_urls:
                continue
            fetched_urls.add(url)
            batch.append(url)
            depth_by_url[url] = depth
        if not batch:
            break

        details = fetch_many_details(
            urls=batch,
            concurrency=concurrency,
            timeout=settings.REQUEST_TIMEOUT,
            delay_range=(settings.DELAY_MIN, settings.DELAY_MAX),
        )

        for url, payload in details.items():
            searched_urls.append(url)
            status_code = int(payload.get("status_code") or 0)
            html = payload.get("html")
            if status_code in {403, 429}:
                blocked_codes[str(status_code)] += 1
            if not html:
                if payload.get("error"):
                    error_key = str(payload.get("error"))
                elif status_code:
                    error_key = f"http_{status_code}"
                else:
                    error_key = "fetch_error"
                scrape_errors[str(error_key)] += 1
                continue

            source_info = source_map.get(url, {"url": url, "source_type": "web", "source_name": "unknown", "query": ""})
            try:
                parsed = parse_page(html, url=url)
            except Exception as error:
                scrape_errors[f"parse_error:{type(error).__name__}"] += 1
                continue
            parsed_cache[url] = {"parsed": parsed, "source_info": source_info}

            depth = depth_by_url.get(url, 0)
            source_type = source_info.get("source_type", "web")
            if not _should_expand_from_source(
                url,
                source_type,
                depth,
                max_depth=max_depth,
                extended_scrape=config.extended_scrape,
            ):
                continue

            expanded = _collect_expanded_urls(
                parsed=parsed,
                parent_source=source_info,
                keyword_tokens=keyword_tokens,
                max_links=max_links_per_page,
                allow_wider=config.extended_scrape,
            )
            for expanded_url in expanded:
                if expanded_url in queued_urls:
                    continue
                if len(queued_urls) >= search_budget:
                    break

                expanded_source_type = _source_type_from_url(expanded_url)
                reason = _url_filter_reason(expanded_url, expanded_source_type)
                if reason:
                    scrape_errors[f"expanded_filtered:{reason}"] += 1
                    continue

                queued_urls.add(expanded_url)
                source_map[expanded_url] = {
                    "url": expanded_url,
                    "source_type": expanded_source_type,
                    "source_name": "expanded_link",
                    "query": source_info.get("query", ""),
                }
                pending.append((expanded_url, depth + 1))

    return {
        "parsed_cache": parsed_cache,
        "searched_urls": searched_urls,
        "scrape_stats": {
            "searched_urls": len(searched_urls),
            "parsed_pages": len(parsed_cache),
            "queue_size": len(queued_urls),
            "blocked_http_codes": dict(blocked_codes),
            "errors": dict(scrape_errors),
        },
    }


def phase_analisis_ia(
    config: LeadSearchConfig,
    parsed_cache: dict[str, dict],
    min_need_score: int,
    target_min_leads: int,
):
    leads = []
    seen_lead_urls = set()

    for item in parsed_cache.values():
        strict_lead = _analyze_parsed_page(
            parsed=item["parsed"],
            keywords=config.keywords,
            source_info=item["source_info"],
            include_suppliers=config.include_suppliers,
            min_need_score=min_need_score,
            strict_mode=True,
        )
        if strict_lead and strict_lead["url"] not in seen_lead_urls:
            seen_lead_urls.add(strict_lead["url"])
            leads.append(strict_lead)

    strict_leads_count = len(leads)
    fallback_scan_limit = max(12, target_min_leads * 12)
    fallback_candidates = list(parsed_cache.items())[:fallback_scan_limit]

    if len(leads) < max(0, target_min_leads):
        relaxed_pool = []
        for url, item in fallback_candidates:
            if url in seen_lead_urls:
                continue
            relaxed_lead = _analyze_parsed_page(
                parsed=item["parsed"],
                keywords=config.keywords,
                source_info=item["source_info"],
                include_suppliers=config.include_suppliers,
                min_need_score=min_need_score,
                strict_mode=False,
            )
            if relaxed_lead:
                relaxed_pool.append(relaxed_lead)

        relaxed_pool.sort(key=_lead_rank, reverse=True)
        needed = max(target_min_leads - len(leads), 0)
        for lead in relaxed_pool[:needed]:
            if lead["url"] in seen_lead_urls:
                continue
            seen_lead_urls.add(lead["url"])
            leads.append(lead)

    if len(leads) < max(0, target_min_leads):
        emergency_pool = []
        for url, item in fallback_candidates:
            if url in seen_lead_urls:
                continue
            emergency_lead = _build_emergency_lead(
                parsed=item["parsed"],
                source_info=item["source_info"],
                keywords=config.keywords,
            )
            if emergency_lead:
                emergency_pool.append(emergency_lead)

        emergency_pool.sort(key=_lead_rank, reverse=True)
        needed = max(target_min_leads - len(leads), 0)
        for lead in emergency_pool[:needed]:
            if lead["url"] in seen_lead_urls:
                continue
            seen_lead_urls.add(lead["url"])
            leads.append(lead)

    if len(leads) < max(0, target_min_leads):
        forced_pool = []
        for url, item in fallback_candidates:
            if url in seen_lead_urls:
                continue
            forced_lead = _build_emergency_lead(
                parsed=item["parsed"],
                source_info=item["source_info"],
                keywords=config.keywords,
                enforce_minimum=True,
            )
            if forced_lead:
                forced_pool.append(forced_lead)

        forced_pool.sort(key=_lead_rank, reverse=True)
        needed = max(target_min_leads - len(leads), 0)
        for lead in forced_pool[:needed]:
            if lead["url"] in seen_lead_urls:
                continue
            seen_lead_urls.add(lead["url"])
            leads.append(lead)

    leads.sort(key=_lead_rank, reverse=True)
    return {
        "leads": leads,
        "analysis_stats": {
            "strict_leads": strict_leads_count,
            "relaxed_leads_added": sum(1 for lead in leads if lead["signal"].get("detection_mode") == "relaxed"),
            "emergency_leads_added": sum(1 for lead in leads if lead["signal"].get("detection_mode") == "emergency"),
            "force_minimum_added": sum(1 for lead in leads if lead["signal"].get("detection_mode") == "force_minimum"),
        },
    }


def phase_scoring_leads(
    leads: list[dict],
    max_results: int,
    searched_urls: list[str],
    query_info: dict,
    discovery_candidates: list[dict],
    filter_stats: dict,
    scrape_stats: dict,
    analysis_stats: dict,
    config: LeadSearchConfig,
):
    leads.sort(key=_lead_rank, reverse=True)
    final_leads = leads[:max_results]

    summary = _build_summary(final_leads, searched_count=len(searched_urls))
    summary["strict_leads"] = sum(1 for lead in final_leads if lead["signal"].get("detection_mode") == "strict")
    summary["relaxed_leads_added"] = sum(1 for lead in final_leads if lead["signal"].get("detection_mode") == "relaxed")
    summary["blocked_403_429"] = scrape_stats.get("blocked_http_codes", {})

    phases = {
        "configuracion": {
            "product_need": config.product_need,
            "country": config.country,
            "language": config.language,
            "intent_type": config.intent_type,
            "target_leads": config.target_leads,
            "source_types": config.source_types,
        },
        "generacion_queries": query_info,
        "descubrimiento_urls": {"found_candidates": len(discovery_candidates)},
        "filtrado_urls": filter_stats,
        "scraping": scrape_stats,
        "analisis_ia": analysis_stats,
        "scoring_leads": {"final_leads": len(final_leads)},
    }

    return {
        "leads": final_leads,
        "summary": summary,
        "phase_metrics": phases,
    }


def run_lead_search(
    keywords: list[str] | None = None,
    max_results=40,
    desired_leads: int | None = None,
    country="es",
    language: str | None = None,
    concurrency=8,
    seed_sites=None,
    include_suppliers=False,
    min_need_score=45,
    extended_scrape=True,
    target_min_leads=settings.TARGET_MIN_LEADS,
    link_expansion_depth=None,
    links_per_page=None,
    product_need: str | None = None,
    source_types: list[str] | None = None,
    intent_type: str = "problema",
    lead_config: LeadSearchConfig | None = None,
):
    discovery_budget = max(1, int(max_results or 1))
    target_leads = max(1, int(target_min_leads or 1))
    lead_goal = max(1, int(desired_leads or target_leads))

    config = phase_configuracion(
        lead_config=lead_config,
        keywords=keywords,
        product_need=product_need,
        country=country,
        language=language,
        target_leads=lead_goal,
        source_types=source_types,
        intent_type=intent_type,
        include_suppliers=include_suppliers,
        min_need_score=min_need_score,
        extended_scrape=extended_scrape,
        link_expansion_depth=link_expansion_depth,
        links_per_page=links_per_page,
    )

    final_limit = max(1, int(config.target_leads or 1))
    per_query = min(24, max(8, discovery_budget // 2)) if config.extended_scrape else min(12, discovery_budget)
    query_info = phase_generacion_queries(config=config, max_results=discovery_budget)
    queries = query_info.get("queries", [])

    candidates = phase_descubrimiento_urls(
        config=config,
        queries=queries,
        max_results=discovery_budget,
        per_query=per_query,
        seed_sites=seed_sites,
    )

    filtered_candidates, filter_stats = phase_filtrado_urls(
        config=config,
        candidates=candidates,
        max_results=discovery_budget,
    )

    scraping_phase = phase_scraping(
        config=config,
        candidates=filtered_candidates,
        concurrency=concurrency,
        max_results=discovery_budget,
    )

    analysis_phase = phase_analisis_ia(
        config=config,
        parsed_cache=scraping_phase["parsed_cache"],
        min_need_score=config.min_need_score,
        target_min_leads=target_leads,
    )

    scoring_phase = phase_scoring_leads(
        leads=analysis_phase["leads"],
        max_results=final_limit,
        searched_urls=scraping_phase["searched_urls"],
        query_info=query_info,
        discovery_candidates=candidates,
        filter_stats=filter_stats,
        scrape_stats=scraping_phase["scrape_stats"],
        analysis_stats=analysis_phase["analysis_stats"],
        config=config,
    )

    return {
        "keywords": config.keywords,
        "country": config.country,
        "language": config.language,
        "intent_type": config.intent_type,
        "source_types": config.source_types,
        "product_need": config.product_need,
        "queries": queries,
        "searched_urls": scraping_phase["searched_urls"],
        "candidates": filtered_candidates,
        "leads": scoring_phase["leads"],
        "summary": scoring_phase["summary"],
        "phase_metrics": scoring_phase["phase_metrics"],
    }
