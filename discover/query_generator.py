from __future__ import annotations

import json
import os
from typing import Any

from config import settings
from pipeline.lead_config import LeadSearchConfig
#c
INTENT_TERMS = {
    "es": {
        "problema": ["problema", "fallo", "averia", "no funciona", "incidencia"],
        "mantenimiento": ["mantenimiento", "servicio tecnico", "correctivo", "preventivo"],
        "parada_produccion": ["parada de produccion", "linea parada", "downtime", "paro de planta"],
        "compra": ["cotizacion", "presupuesto", "solicitud de oferta", "rfq", "comprar"],
    },
    "en": {
        "problema": ["problem", "failure", "issue", "fault", "not working"],
        "mantenimiento": ["maintenance", "service", "preventive maintenance", "corrective maintenance"],
        "parada_produccion": ["production downtime", "line stopped", "plant shutdown", "outage"],
        "compra": ["quote request", "rfq", "procurement", "purchase request", "buy"],
    },
}

SOURCE_QUERY_HINTS = {
    "web": [
        "empresa industrial",
        "caso de exito",
        "blog industrial",
        "noticias empresa",
        "mantenimiento planta",
        "linea de produccion",
        "proyecto electrico",
        "cliente industrial",
    ],
    "forum": ["foro", "thread", "discussion", "community"],
    "review": ["opiniones", "reviews", "quejas", "trustpilot", "g2"],
    "social": ["site:linkedin.com/posts", "site:x.com", "post", "comentarios"],
}


def _dedupe(values: list[str]):
    output = []
    seen = set()
    for value in values:
        item = " ".join((value or "").strip().split())
        if not item:
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _intent_terms(config: LeadSearchConfig):
    lang_map = INTENT_TERMS.get(config.language, INTENT_TERMS["es"])
    return lang_map.get(config.intent_type, lang_map["problema"])


def _fallback_queries(config: LeadSearchConfig, max_queries: int):
    intent_terms = _intent_terms(config)
    queries = []
    for keyword in config.keywords:
        kw = keyword.strip()
        if not kw:
            continue

        for term in intent_terms:
            queries.append(f"{kw} {term}")

        if config.intent_type == "compra":
            queries.extend(
                [
                    f"empresa necesita {kw}",
                    f"planta busca {kw}",
                    f"{kw} solicitud de oferta industrial",
                ]
            )
        elif config.intent_type == "parada_produccion":
            queries.extend(
                [
                    f"{kw} parada de produccion",
                    f"{kw} linea de produccion detenida",
                    f"{kw} downtime issue factory",
                ]
            )
        elif config.intent_type == "mantenimiento":
            queries.extend(
                [
                    f"{kw} mantenimiento urgente",
                    f"{kw} mantenimiento correctivo planta",
                    f"{kw} maintenance issue industrial",
                ]
            )
        else:
            queries.extend(
                [
                    f"{kw} no funciona industria",
                    f"{kw} averia planta",
                    f"{kw} electrical issue factory",
                ]
            )

        for source_type in config.source_types:
            for hint in SOURCE_QUERY_HINTS.get(source_type, []):
                queries.append(f"{hint} {kw} {intent_terms[0]}")

        if config.country and config.country != "all":
            queries.append(f"site:.{config.country} {kw} {intent_terms[0]}")
            queries.append(f"{kw} {intent_terms[0]} {config.country}")

    return _dedupe(queries)[:max_queries]


def _extract_json_payload(content: str):
    text = (content or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return {}
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return {}


def _openai_queries(config: LeadSearchConfig, max_queries: int):
    if not settings.USE_OPENAI or not os.environ.get("OPENAI_API_KEY"):
        return None, "openai_disabled"

    try:
        from openai import OpenAI
    except Exception as error:
        return None, f"openai_import_error: {error}"

    source_types = ", ".join(config.source_types)
    keyword_block = ", ".join(config.keywords)
    intent_terms = ", ".join(_intent_terms(config))

    prompt = (
        "Generate high-precision B2B search queries to find companies with active need.\n"
        f"Product/need: {config.product_need}\n"
        f"Country: {config.country}\n"
        f"Language: {config.language}\n"
        f"Lead intent: {config.intent_type}\n"
        f"Sources to target: {source_types}\n"
        f"Keyword hints: {keyword_block}\n"
        f"Intent terms: {intent_terms}\n"
        "Avoid ecommerce marketplaces and product category pages.\n"
        "Avoid reddit, wikipedia, quora, generic dictionaries, and low-intent community pages.\n"
        "Prioritize company websites, engineering blogs, case studies, and industrial news mentioning named companies.\n"
        "Focus on problems, maintenance, downtime, purchase requests, procurement signals.\n"
        f"Return strict JSON: {{\"queries\": [..]}} with at most {max_queries} queries."
    )

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    try:
        completion = client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You design B2B demand-intent search strategies."},
                {"role": "user", "content": prompt},
            ],
        )
        payload = _extract_json_payload(completion.choices[0].message.content or "")
    except Exception as error:
        return None, f"openai_request_error: {error}"

    queries = payload.get("queries", [])
    if not isinstance(queries, list):
        return None, "openai_invalid_payload"
    cleaned = _dedupe([str(query) for query in queries if isinstance(query, str)])
    return cleaned[:max_queries], None


def generate_search_queries(config: LeadSearchConfig, max_queries: int | None = None):
    limit = max(6, int(max_queries or settings.DISCOVERY_MAX_QUERIES))

    ai_queries, ai_error = _openai_queries(config=config, max_queries=limit)
    fallback_queries = _fallback_queries(config=config, max_queries=limit)

    combined = _dedupe((ai_queries or []) + fallback_queries)
    if not combined:
        combined = fallback_queries

    return {
        "queries": combined[:limit],
        "generator_mode": "ai_plus_fallback" if ai_queries else "heuristic",
        "ai_error": ai_error or "",
    }
