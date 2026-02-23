from __future__ import annotations

from dataclasses import dataclass, field

from config import settings
from config.keywords import KEYWORDS

SUPPORTED_SOURCE_TYPES = ("web", "forum", "review", "social")
SUPPORTED_INTENT_TYPES = ("problema", "mantenimiento", "parada_produccion", "compra")

INTENT_ALIASES = {
    "problema": "problema",
    "issue": "problema",
    "problem": "problema",
    "pain": "problema",
    "mantenimiento": "mantenimiento",
    "maintenance": "mantenimiento",
    "service": "mantenimiento",
    "parada_produccion": "parada_produccion",
    "parada de produccion": "parada_produccion",
    "downtime": "parada_produccion",
    "production_stop": "parada_produccion",
    "compra": "compra",
    "buy": "compra",
    "purchase": "compra",
    "procurement": "compra",
}

COUNTRY_LANGUAGE_DEFAULT = {
    "es": "es",
    "mx": "es",
    "ar": "es",
    "co": "es",
    "cl": "es",
    "pe": "es",
    "us": "en",
    "uk": "en",
    "all": "es",
}


def _dedupe(values: list[str]):
    cleaned = []
    seen = set()
    for value in values:
        item = (value or "").strip()
        if not item:
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
    return cleaned


def _normalize_sources(values: list[str] | None):
    if not values:
        return ["web"]
    normalized = []
    for value in values:
        source = (value or "").strip().lower()
        if source == "webs":
            source = "web"
        if source == "foros":
            source = "forum"
        if source == "reviews":
            source = "review"
        if source in {"social_media", "redes", "redes_sociales"}:
            source = "social"
        if source in SUPPORTED_SOURCE_TYPES:
            normalized.append(source)
    normalized = _dedupe(normalized)
    return normalized or ["web"]


def _normalize_intent(value: str | None):
    raw = (value or "").strip().lower()
    if not raw:
        return "problema"
    return INTENT_ALIASES.get(raw, "problema")


def _infer_keywords(product_need: str, fallback_keywords: list[str] | None = None):
    base = [part.strip() for part in (product_need or "").split(",") if part.strip()]
    merged = _dedupe(base + (fallback_keywords or []))
    return merged or KEYWORDS[:3]


@dataclass(slots=True)
class LeadSearchConfig:
    product_need: str
    country: str = "es"
    language: str = "es"
    target_leads: int = 5
    source_types: list[str] = field(default_factory=lambda: ["web"])
    intent_type: str = "problema"
    keywords: list[str] = field(default_factory=list)
    min_need_score: int = settings.MIN_NEED_SCORE
    include_suppliers: bool = False
    extended_scrape: bool = True
    link_expansion_depth: int | None = None
    links_per_page: int | None = None

    @classmethod
    def from_inputs(
        cls,
        *,
        product_need: str,
        country: str = "es",
        language: str | None = None,
        target_leads: int = 5,
        source_types: list[str] | None = None,
        intent_type: str = "problema",
        keywords: list[str] | None = None,
        min_need_score: int | None = None,
        include_suppliers: bool = False,
        extended_scrape: bool = True,
        link_expansion_depth: int | None = None,
        links_per_page: int | None = None,
    ):
        country_code = (country or "es").strip().lower()
        if not country_code:
            country_code = "es"
        selected_language = (language or COUNTRY_LANGUAGE_DEFAULT.get(country_code, "es")).strip().lower() or "es"

        return cls(
            product_need=(product_need or "").strip(),
            country=country_code,
            language=selected_language,
            target_leads=max(1, int(target_leads or 1)),
            source_types=_normalize_sources(source_types),
            intent_type=_normalize_intent(intent_type),
            keywords=_infer_keywords(product_need or "", keywords or []),
            min_need_score=int(min_need_score if min_need_score is not None else settings.MIN_NEED_SCORE),
            include_suppliers=bool(include_suppliers),
            extended_scrape=bool(extended_scrape),
            link_expansion_depth=link_expansion_depth,
            links_per_page=links_per_page,
        )
