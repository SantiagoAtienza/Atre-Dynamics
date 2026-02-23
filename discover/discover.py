import random
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import settings
from config.keywords import NEED_INTENT_TERMS
from discover.link_crawler import crawl_links

try:
    from integrations.serpapi_search import serpapi_search
except Exception:
    serpapi_search = None


BLOCKED_DOMAINS = {
    "duckduckgo.com",
    "www.duckduckgo.com",
    "bing.com",
    "www.bing.com",
    "google.com",
    "www.google.com",
    "search.brave.com",
}

NOISE_DOMAINS = {
    "merriam-webster.com",
    "www.merriam-webster.com",
    "buscapalabra.com",
    "www.buscapalabra.com",
    "answers.microsoft.com",
    "learn.microsoft.com",
    "dictionary.cambridge.org",
    "www.dictionary.com",
    "wiktionary.org",
    "www.wiktionary.org",
    "wikipedia.org",
    "www.wikipedia.org",
    "wikidata.org",
    "www.wikidata.org",
    "fandom.com",
    "www.fandom.com",
    "wikia.com",
    "www.wikia.com",
    "medium.com",
    "www.medium.com",
}

LARGE_ECOMMERCE_DOMAINS = {
    "amazon.com",
    "amazon.es",
    "mercadolibre.com",
    "alibaba.com",
    "aliexpress.com",
    "ebay.com",
    "walmart.com",
    "etsy.com",
    "shopify.com",
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

QUERY_STOPWORDS = {
    "site",
    "www",
    "http",
    "https",
    "com",
    "es",
    "reddit",
    "linkedin",
    "posts",
    "forocoches",
    "industria",
    "industrial",
    "empresa",
}

SOURCE_PRIORITY = {
    "web": 0,
    "news": 1,
    "review": 2,
    "forum": 3,
    "social": 4,
}

CORE_TOPIC_TOKENS = {"electrico", "electrical", "cuadro", "cabinet", "panel", "armario"}
NEED_CONTEXT_TOKENS = {"necesita", "need", "problema", "averia", "mantenimiento", "industrial", "planta"}
HIGH_INTENT_PATH_HINTS = {
    "/caso",
    "/casos",
    "/case",
    "/projects",
    "/proyecto",
    "/proyectos",
    "/clientes",
    "/solutions",
    "/servicios",
    "/servicio",
    "/contact",
    "/contacto",
    "/blog",
    "/insights",
    "/noticias",
    "/news",
}
LOW_INTENT_PATH_HINTS = {
    "/wiki/",
    "/thread/",
    "/threads/",
    "/forum/",
    "/forums/",
    "/community/",
    "/tag/",
    "/tags/",
    "/category/",
    "/categoria/",
    "/search",
}


def _normalize(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _make_session():
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "Mozilla/5.0 (LeadRadar/1.0)"})
    proxy = getattr(settings, "LR_PROXY", None) or None
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def _request_timeout():
    return (settings.DISCOVERY_CONNECT_TIMEOUT, settings.DISCOVERY_READ_TIMEOUT)


def _domain(url: str):
    return urlparse(url).netloc.lower()


def _clean_href(href: str):
    if not href:
        return None
    href = href.strip()
    if href.startswith("javascript:") or href.startswith("mailto:"):
        return None

    parsed = urlparse(href)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        params = parse_qs(parsed.query)
        if "uddg" in params and params["uddg"]:
            return unquote(params["uddg"][0])
    return href


def _is_valid_result_url(url: str):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    domain = parsed.netloc.lower()
    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in BLOCKED_DOMAINS):
        return False
    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in NOISE_DOMAINS):
        return False
    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in LARGE_ECOMMERCE_DOMAINS):
        return False
    return True


def _source_type_from_url(url: str, query: str, source_name: str):
    domain = _domain(url)
    normalized_url = _normalize(url)

    if source_name in {"google_news_rss"}:
        return "news"
    if any(domain == value or domain.endswith(f".{value}") for value in FORUM_DOMAINS):
        return "forum"
    if any(domain == value or domain.endswith(f".{value}") for value in SOCIAL_DOMAINS):
        return "social"
    if any(domain == value or domain.endswith(f".{value}") for value in REVIEW_DOMAINS):
        return "review"
    if "linkedin.com" in normalized_url or "twitter.com" in normalized_url or "x.com" in normalized_url:
        return "social"
    return "web"


def _country_rank(url: str, country: str):
    if country != "es":
        return 0
    domain = _domain(url)
    lower = url.lower()
    if domain.endswith(".es"):
        return 0
    if "/es/" in lower or "espana" in lower or "spain" in lower:
        return 1
    return 2


def _query_overlap_rank(url: str, keywords: list[str]):
    normalized = _normalize(url)
    score = 0
    for keyword in keywords:
        tokens = [tok for tok in _normalize(keyword).split() if len(tok) >= 4]
        score = max(score, sum(1 for token in tokens if token in normalized))
    # lower is better for sorting
    return -score


def _url_quality_rank(url: str, source_type: str):
    lower = (url or "").lower()
    parsed = urlparse(lower)
    path = parsed.path or ""
    quality = 0

    if source_type == "web":
        quality += 3
    elif source_type == "news":
        quality += 2
    elif source_type == "review":
        quality += 0
    else:
        quality -= 2

    if path in {"", "/"}:
        quality -= 2
    if any(hint in path for hint in HIGH_INTENT_PATH_HINTS):
        quality += 3
    if any(hint in path for hint in LOW_INTENT_PATH_HINTS):
        quality -= 3
    if "?" in lower:
        quality -= 1
    if any(host_hint in parsed.netloc for host_hint in ("wikipedia", "reddit", "quora")):
        quality -= 5

    # lower is better for sorting
    return -quality


def _query_tokens(query: str):
    tokens = []
    for token in re.split(r"[^a-z0-9]+", _normalize(query)):
        token = token.strip()
        if len(token) < 4:
            continue
        if token in QUERY_STOPWORDS:
            continue
        tokens.append(token)
    # preserve order, remove duplicates
    return list(dict.fromkeys(tokens))


def _relevance_from_text(text: str, query: str):
    normalized = _normalize(text)
    tokens = _query_tokens(query)
    if not tokens:
        return True
    overlap = sum(1 for token in tokens if token in normalized)
    minimum = 2 if len(tokens) >= 2 else 1
    return overlap >= minimum


def _build_queries(keywords: list[str], country: str):
    templates = [
        "{kw} industria problema",
        "{kw} averia",
        "{kw} mantenimiento urgente",
        "{kw} no funciona",
        "empresa necesita {kw}",
        "planta necesita {kw}",
        "buscamos {kw} industrial",
        "{kw} parada de linea",
        "{kw} retrofit industrial",
        "{kw} reemplazo urgente",
        "{kw} solicitud de oferta",
        "{kw} presupuesto industrial",
        "{kw} maintenance issue",
        "{kw} production line failure",
        "{kw} needs replacement",
        "{kw} downtime issue",
        "{kw} caso de exito industrial",
        "{kw} blog industrial mantenimiento",
        "{kw} noticias empresa industrial",
        "{kw} cliente industrial proyecto",
        "{kw} ingenieria electrica blog",
        "site:news.google.com {kw} industria",
    ]
    if country == "es":
        templates.extend(
            [
                "site:.es {kw} problema electrico",
                "site:.es {kw} mantenimiento industrial",
                "site:.es {kw} parada de produccion",
                "site:.es {kw} caso de exito industrial",
                "site:.es {kw} blog mantenimiento planta",
                "{kw} espana industria",
                "{kw} empresa espanola mantenimiento",
            ]
        )

    queries = []
    for keyword in keywords:
        for template in templates:
            queries.append(template.format(kw=keyword))
        for need_term in NEED_INTENT_TERMS[:6]:
            queries.append(f"{keyword} {need_term}")

    deduped = list(dict.fromkeys(queries))
    random.shuffle(deduped)
    return deduped[: settings.DISCOVERY_MAX_QUERIES]


def _search_bing_rss(session: requests.Session, query: str, per_query: int):
    url = f"https://www.bing.com/search?q={quote_plus(query)}&format=rss&count={per_query}"
    response = session.get(url, timeout=_request_timeout())
    response.raise_for_status()
    links = []
    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        return links
    for item in root.findall(".//item"):
        link = item.findtext("link")
        title = item.findtext("title") or ""
        description = item.findtext("description") or ""
        if link and _relevance_from_text(f"{title} {description} {link}", query):
            links.append(link.strip())
    return links


def _search_google_news_rss(session: requests.Session, query: str, per_query: int):
    url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(query)}&hl=es&gl=ES&ceid=ES:es"
    )
    response = session.get(url, timeout=_request_timeout())
    response.raise_for_status()
    links = []
    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        return links
    for item in root.findall(".//item"):
        link = item.findtext("link")
        title = item.findtext("title") or ""
        description = item.findtext("description") or ""
        if link and _relevance_from_text(f"{title} {description} {link}", query):
            links.append(link.strip())
        if len(links) >= per_query:
            break
    return links


def _search_duckduckgo_html(session: requests.Session, query: str):
    url = f"https://html.duckduckgo.com/html?q={quote_plus(query)}&s=0"
    response = session.get(url, timeout=_request_timeout())
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for anchor in soup.find_all("a", href=True):
        href = _clean_href(anchor.get("href"))
        text = anchor.get_text(" ", strip=True)
        if href and _relevance_from_text(f"{text} {href}", query):
            results.append(href)
    return results


def _search_duckduckgo_lite(session: requests.Session, query: str):
    url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    response = session.get(url, timeout=_request_timeout())
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    results = []
    for anchor in soup.find_all("a", href=True):
        href = _clean_href(anchor.get("href"))
        text = anchor.get_text(" ", strip=True)
        if href and _relevance_from_text(f"{text} {href}", query):
            results.append(href)
    return results


def _search_reddit_json(session: requests.Session, query: str, per_query: int):
    url = (
        "https://www.reddit.com/search.json?"
        f"q={quote_plus(query)}&limit={per_query}&sort=new&t=year"
    )
    response = session.get(url, timeout=_request_timeout(), headers={"User-Agent": "LeadRadar/1.0"})
    response.raise_for_status()
    payload = response.json()
    links = []
    for child in payload.get("data", {}).get("children", []):
        data = child.get("data", {})
        permalink = data.get("permalink")
        external = data.get("url")
        title = data.get("title", "") or ""
        selftext = data.get("selftext", "") or ""
        text_blob = f"{title} {selftext} {external or ''}"
        if not _relevance_from_text(text_blob, query):
            continue
        normalized_blob = _normalize(text_blob)
        normalized_slug = _normalize(f"{permalink or ''} {title}")
        if not any(token in normalized_blob for token in CORE_TOPIC_TOKENS):
            continue
        if not any(token in normalized_slug for token in CORE_TOPIC_TOKENS):
            continue
        if not any(token in normalized_blob for token in NEED_CONTEXT_TOKENS):
            continue
        if permalink:
            links.append("https://www.reddit.com" + permalink)
        if external and external.startswith("http"):
            links.append(external)
        if len(links) >= per_query:
            break
    return links


def _search_hn_algolia(session: requests.Session, query: str, per_query: int):
    url = (
        "https://hn.algolia.com/api/v1/search_by_date?"
        f"query={quote_plus(query)}&tags=story&hitsPerPage={per_query}"
    )
    response = session.get(url, timeout=_request_timeout())
    response.raise_for_status()
    payload = response.json()
    links = []
    for hit in payload.get("hits", []):
        target = hit.get("url")
        if not target and hit.get("objectID"):
            target = f"https://news.ycombinator.com/item?id={hit['objectID']}"
        title = hit.get("title", "") or ""
        story = hit.get("story_text", "") or ""
        if not _relevance_from_text(f"{title} {story} {target or ''}", query):
            continue
        if target and target.startswith("http"):
            links.append(target)
        if len(links) >= per_query:
            break
    return links


def _generate_fallback_candidates(keywords: list[str], country: str):
    del keywords, country
    return []


def _add_candidate(
    href: str,
    source_name: str,
    query: str,
    keywords: list[str],
    found: list[dict],
    seen: set[str],
    max_results: int,
    allowed_source_types: set[str] | None = None,
):
    if not href:
        return
    if not _is_valid_result_url(href):
        return
    href = href.split("#")[0].strip()
    if not href or href in seen:
        return

    source_type = _source_type_from_url(href, query=query, source_name=source_name)
    if allowed_source_types is not None and source_type not in allowed_source_types:
        return

    seen.add(href)
    found.append(
        {
            "url": href,
            "source_type": source_type,
            "source_name": source_name,
            "query": query,
            "overlap_rank": _query_overlap_rank(href, keywords),
        }
    )
    if len(found) >= max_results:
        return


def _extend_with_crawl(candidates: list[dict], seen: set[str], max_results: int):
    if len(candidates) >= max_results:
        return candidates
    extra = []
    crawl_seed_count = max(4, min(10, max_results // 4))
    for candidate in candidates[:crawl_seed_count]:
        base_url = candidate["url"].lower()
        if candidate.get("source_name") == "fallback_api":
            continue
        if ".json" in base_url or "/api/" in base_url or "news.google.com/rss" in base_url:
            continue
        if "search?" in base_url or "/search/?" in base_url:
            continue
        try:
            crawled = crawl_links(
                candidate["url"],
                max_pages=settings.DISCOVERY_DEEP_CRAWL_PAGES,
                delay=(0.2, 0.5),
            )
        except Exception:
            continue
        for url in crawled.get("candidates", []):
            if url in seen or not _is_valid_result_url(url):
                continue
            lower_url = url.lower()
            if any(hint in lower_url for hint in LOW_INTENT_PATH_HINTS):
                continue
            if not _relevance_from_text(url, candidate.get("query", "")):
                path_hints = [
                    "contact",
                    "contacto",
                    "servicio",
                    "proyecto",
                    "cliente",
                    "about",
                    "case",
                    "solution",
                    "industr",
                    "blog",
                    "insight",
                    "noticia",
                    "news",
                ]
                if not any(hint in lower_url for hint in path_hints):
                    continue
            if "search/?q=" in lower_url or "source=trending" in lower_url:
                continue
            seen.add(url)
            extra.append(
                {
                    "url": url,
                    "source_type": candidate.get("source_type", "web"),
                    "source_name": "deep_crawl",
                    "query": candidate.get("query", ""),
                    "overlap_rank": candidate.get("overlap_rank", 0),
                }
            )
            if len(candidates) + len(extra) >= max_results:
                break
        if len(candidates) + len(extra) >= max_results:
            break
    return candidates + extra


def discover_candidates(
    keywords: list,
    max_results=40,
    per_query=8,
    country="es",
    queries: list[str] | None = None,
    source_types: list[str] | None = None,
):
    session = _make_session()
    queries = queries or _build_queries(keywords, country=country)
    found = []
    seen = set()
    disabled = set()
    failures = defaultdict(int)
    allowed_source_types = set(source_types or ["web", "news"])
    if "web" in allowed_source_types:
        allowed_source_types.add("news")

    engines = [
        ("bing_rss", lambda q: _search_bing_rss(session, q, per_query)),
        ("google_news_rss", lambda q: _search_google_news_rss(session, q, per_query)),
        ("ddg_html", lambda q: _search_duckduckgo_html(session, q)),
        ("ddg_lite", lambda q: _search_duckduckgo_lite(session, q)),
    ]
    if "forum" in allowed_source_types:
        engines.append(("reddit_json", lambda q: _search_reddit_json(session, q, per_query)))
        engines.append(("hn_algolia", lambda q: _search_hn_algolia(session, q, per_query)))

    for query in queries:
        if len(found) >= max_results:
            break

        if settings.USE_SERPAPI and serpapi_search:
            try:
                for href in serpapi_search(query, num=per_query):
                    _add_candidate(
                        href,
                        "serpapi",
                        query,
                        keywords,
                        found,
                        seen,
                        max_results,
                        allowed_source_types=allowed_source_types,
                    )
                    if len(found) >= max_results:
                        break
            except Exception as error:
                print(f"[DISCOVER WARN] serpapi failed: {error}")

        for name, fn in engines:
            if name in disabled:
                continue
            try:
                for href in fn(query):
                    _add_candidate(
                        href,
                        name,
                        query,
                        keywords,
                        found,
                        seen,
                        max_results,
                        allowed_source_types=allowed_source_types,
                    )
                    if len(found) >= max_results:
                        break
            except Exception as error:
                failures[name] += 1
                if failures[name] <= 2:
                    print(f"[DISCOVER WARN] {name} failed on '{query}': {error}")
                if failures[name] >= 3:
                    disabled.add(name)
                    print(f"[DISCOVER INFO] Disabled engine: {name}")
            if len(found) >= max_results:
                break

        if engines and len(disabled) == len(engines):
            print("[DISCOVER INFO] All engines disabled, switching to fallback candidates.")
            break
        time.sleep(random.uniform(0.15, 0.45))

    min_expected = max(6, max_results // 5)
    if len(found) < min_expected:
        fallback_candidates = _generate_fallback_candidates(keywords=keywords, country=country)
        for candidate in fallback_candidates:
            _add_candidate(
                href=candidate["url"],
                source_name=candidate["source_name"],
                query=candidate["query"],
                keywords=keywords,
                found=found,
                seen=seen,
                max_results=max_results,
                allowed_source_types=allowed_source_types,
            )
        # force source types from predefined fallback for newly added items
        manual_map = {candidate["url"]: candidate for candidate in fallback_candidates}
        for idx, candidate in enumerate(found):
            if candidate["url"] in manual_map:
                found[idx]["source_type"] = manual_map[candidate["url"]]["source_type"]

    found = _extend_with_crawl(found, seen=seen, max_results=max_results)

    found = list({candidate["url"]: candidate for candidate in found}.values())
    found.sort(
        key=lambda candidate: (
            SOURCE_PRIORITY.get(candidate.get("source_type", "web"), 10),
            _url_quality_rank(candidate["url"], candidate.get("source_type", "web")),
            _country_rank(candidate["url"], country=country),
            candidate.get("overlap_rank", 0),
        )
    )
    return found[:max_results]


def discover_urls(keywords: list, max_results=20, per_query=10, country="es"):
    candidates = discover_candidates(
        keywords=keywords,
        max_results=max_results,
        per_query=per_query,
        country=country,
    )
    return [candidate["url"] for candidate in candidates]
