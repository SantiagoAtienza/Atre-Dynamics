import json
import re
import unicodedata
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(r"\+?\d[\d\s\-()]{6,}\d")
URL_RE = re.compile(r"https?://[^\s\"'>]+")
ORG_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&,\- ]{2,60}\s(?:S\.?L\.?U?\.?|S\.?A\.?|SL|SA|LLC|LTD|INC))\b"
)


def _normalize(value: str):
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(c for c in normalized if not unicodedata.combining(c)).lower()


def _extract_jsonld(soup):
    data = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(tag.string or "")
            data.append(payload)
        except Exception:
            continue
    return data


def _flatten_json(payload):
    if isinstance(payload, dict):
        yield payload
        for value in payload.values():
            yield from _flatten_json(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _flatten_json(item)


def _extract_org_candidates(title: str, text: str, json_data: list):
    candidates = []
    if title:
        first_chunk = re.split(r"[|\-:]", title)[0].strip()
        if len(first_chunk) > 3:
            candidates.append(first_chunk)

    for match in ORG_RE.findall(text[:9000]):
        candidates.append(match.strip())

    for item in json_data:
        for node in _flatten_json(item):
            if not isinstance(node, dict):
                continue
            type_value = str(node.get("@type", "")).lower()
            if "organization" in type_value or "localbusiness" in type_value:
                name = node.get("name")
                if isinstance(name, str) and name.strip():
                    candidates.append(name.strip())
            # Support API payloads like Reddit/HN where name-like fields appear.
            for key in ("author", "subreddit", "company", "organization", "site_name"):
                value = node.get(key)
                if isinstance(value, str) and len(value.strip()) >= 3:
                    candidates.append(value.strip())

    clean = []
    seen = set()
    for candidate in candidates:
        normalized = _normalize(candidate)
        if len(normalized) < 3:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        clean.append(candidate)
        if len(clean) >= 10:
            break
    return clean


def _extract_sentences(text: str, max_sentences=100):
    raw = re.split(r"(?<=[\.\!\?\n])\s+", text or "")
    sentences = []
    for sentence in raw:
        s = sentence.strip()
        if len(s) < 20:
            continue
        sentences.append(s[:360])
        if len(sentences) >= max_sentences:
            break
    return sentences


def _extract_from_json_payload(payload):
    text_chunks = []
    links = []
    title = ""
    for node in _flatten_json(payload):
        if isinstance(node, dict):
            for key, value in node.items():
                if not isinstance(value, str):
                    continue
                value = value.strip()
                if not value:
                    continue
                lower_key = key.lower()
                if lower_key in {"title", "headline", "story_title"} and not title:
                    title = value[:220]
                if lower_key in {
                    "title",
                    "headline",
                    "story_title",
                    "story_text",
                    "selftext",
                    "selftext_html",
                    "body",
                    "body_html",
                    "text",
                    "description",
                    "content",
                    "summary",
                    "comment_text",
                }:
                    text_chunks.append(value)
                if value.startswith("http"):
                    links.append(value)
                for match in URL_RE.findall(value):
                    links.append(match)
    return {
        "title": title,
        "text": " ".join(text_chunks),
        "links": sorted(set(links)),
    }


def _parse_json_like(content: str):
    stripped = (content or "").strip()
    if not stripped:
        return None
    if not (stripped.startswith("{") or stripped.startswith("[")):
        return None
    try:
        return json.loads(stripped)
    except Exception:
        return None


def _parse_xml_like(content: str):
    stripped = (content or "").strip()
    if not stripped:
        return None
    if not (stripped.startswith("<?xml") or "<rss" in stripped[:200].lower() or "<feed" in stripped[:200].lower()):
        return None
    try:
        root = ET.fromstring(stripped)
    except Exception:
        return None

    text_chunks = []
    links = []
    title = ""

    for node in root.findall(".//item") + root.findall(".//entry"):
        node_title = node.findtext("title") or ""
        node_desc = node.findtext("description") or node.findtext("summary") or node.findtext("content") or ""
        if node_title:
            text_chunks.append(node_title)
            if not title:
                title = node_title[:220]
        if node_desc:
            text_chunks.append(node_desc)

        link_text = node.findtext("link")
        if link_text and link_text.startswith("http"):
            links.append(link_text)

        for child in node.findall("link"):
            href = child.attrib.get("href")
            if href and href.startswith("http"):
                links.append(href)

    if not text_chunks and not links:
        return None
    return {
        "title": title,
        "text": " ".join(text_chunks),
        "links": sorted(set(links)),
    }


def parse_page(html: str, url: str | None = None):
    domain = urlparse(url or "").netloc.lower()
    json_payload = _parse_json_like(html)
    xml_payload = _parse_xml_like(html)

    if json_payload is not None:
        parsed = _extract_from_json_payload(json_payload)
        full_text = parsed["text"]
        normalized_text = _normalize(full_text)
        json_data = [json_payload]
        org_candidates = _extract_org_candidates(
            title=parsed.get("title", ""),
            text=full_text,
            json_data=json_data,
        )
        emails = sorted(set(EMAIL_RE.findall(full_text)))
        phones = sorted(set(PHONE_RE.findall(full_text)))
        review_terms = ["opiniones", "resenas", "reviews", "valoraciones", "resena", "quejas", "complaint"]
        forum_terms = ["foro", "thread", "hilo", "discusion", "subreddit", "community", "reddit"]
        return {
            "url": url or "",
            "domain": domain,
            "title": parsed.get("title", ""),
            "meta_description": "",
            "og_site_name": "",
            "text": full_text,
            "sentences": _extract_sentences(full_text),
            "images": [],
            "emails": emails,
            "phones": phones,
            "social": [],
            "links": parsed.get("links", []),
            "jsonld": json_data,
            "organization_candidates": org_candidates,
            "has_reviews": any(term in normalized_text for term in review_terms),
            "has_forum_context": any(term in normalized_text for term in forum_terms),
        }

    if xml_payload is not None:
        full_text = xml_payload["text"]
        normalized_text = _normalize(full_text)
        json_data = []
        org_candidates = _extract_org_candidates(
            title=xml_payload.get("title", ""),
            text=full_text,
            json_data=json_data,
        )
        emails = sorted(set(EMAIL_RE.findall(full_text)))
        phones = sorted(set(PHONE_RE.findall(full_text)))
        review_terms = ["opiniones", "resenas", "reviews", "valoraciones", "resena", "quejas", "complaint"]
        forum_terms = ["foro", "thread", "hilo", "discusion", "community", "reddit"]
        return {
            "url": url or "",
            "domain": domain,
            "title": xml_payload.get("title", ""),
            "meta_description": "",
            "og_site_name": "",
            "text": full_text,
            "sentences": _extract_sentences(full_text),
            "images": [],
            "emails": emails,
            "phones": phones,
            "social": [],
            "links": xml_payload.get("links", []),
            "jsonld": [],
            "organization_candidates": org_candidates,
            "has_reviews": any(term in normalized_text for term in review_terms),
            "has_forum_context": any(term in normalized_text for term in forum_terms),
        }

    soup = BeautifulSoup(html, "lxml")
    text_tags = ["p", "h1", "h2", "h3", "li", "blockquote", "td"]
    texts = [
        tag.get_text(" ", strip=True)
        for tag in soup.find_all(text_tags)
        if tag.get_text(strip=True)
    ]
    full_text = " ".join(texts)
    normalized_text = _normalize(full_text)

    title = (soup.title.string or "").strip() if soup.title else ""
    meta_description = ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if meta_tag and meta_tag.get("content"):
        meta_description = meta_tag.get("content").strip()

    og_site_name = ""
    og_tag = soup.find("meta", attrs={"property": "og:site_name"})
    if og_tag and og_tag.get("content"):
        og_site_name = og_tag.get("content").strip()

    images = [
        {"src": image.get("src"), "alt": image.get("alt", "")}
        for image in soup.find_all("img")
        if image.get("src")
    ]

    emails = sorted(set(EMAIL_RE.findall(full_text)))
    phones = sorted(set(PHONE_RE.findall(full_text)))

    social = []
    social_domains = [
        "facebook.com",
        "linkedin.com",
        "instagram.com",
        "twitter.com",
        "x.com",
        "youtube.com",
        "tiktok.com",
    ]
    for anchor in soup.find_all("a", href=True):
        href = urljoin(url or "", anchor["href"])
        if any(domain_name in href for domain_name in social_domains):
            social.append(href)
    social = sorted(set(social))

    links = []
    for anchor in soup.find_all("a", href=True):
        href = urljoin(url or "", anchor["href"])
        if href and not href.startswith("javascript:"):
            links.append(href)

    review_terms = ["opiniones", "resenas", "reviews", "valoraciones", "resena", "quejas"]
    has_reviews = any(term in normalized_text for term in review_terms)

    forum_terms = ["foro", "thread", "hilo", "discusion", "comunidad", "community"]
    has_forum_context = any(term in normalized_text for term in forum_terms)

    jsonld_data = _extract_jsonld(soup)
    org_candidates = _extract_org_candidates(title=title, text=full_text, json_data=jsonld_data)

    return {
        "url": url or "",
        "domain": domain,
        "title": title,
        "meta_description": meta_description,
        "og_site_name": og_site_name,
        "text": full_text,
        "sentences": _extract_sentences(full_text),
        "images": images,
        "emails": emails,
        "phones": phones,
        "social": social,
        "links": links,
        "jsonld": jsonld_data,
        "organization_candidates": org_candidates,
        "has_reviews": has_reviews,
        "has_forum_context": has_forum_context,
    }
